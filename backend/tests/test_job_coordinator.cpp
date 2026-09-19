/**
 * 任务协调器 (Job Coordinator) 测试用例
 *
 * 通过杀死 worker、重启协调器、重复提交等故障注入，验证：
 * - 没有静默丢单（至少一次投递 + 租约重投 + 日志恢复）
 * - 没有重复完成（迟到确认/重复结果被识别且不计入）
 * - 死信、重放、单实例、损坏日志尾部恢复、优雅关停
 */

#include "test_framework.h"
#include "job_coordinator.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <string>

using namespace test;
using namespace ipc::coord;

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

namespace {

std::string make_temp_dir() {
    char tmpl[] = "/tmp/ipc_coord_test_XXXXXX";
    char* d = mkdtemp(tmpl);
    return d ? std::string(d) : "";
}

void rm_rf(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
}

void write_file(const std::string& path, const std::string& content) {
    std::ofstream f(path);
    f << content;
}

void append_file(const std::string& path, const std::string& content) {
    int fd = open(path.c_str(), O_WRONLY | O_APPEND);
    if (fd >= 0) {
        ssize_t r = write(fd, content.data(), content.size());
        (void)r;
        close(fd);
    }
}

std::string read_file(const std::string& path) {
    std::ifstream f(path);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

bool file_exists(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

bool wait_for(const std::function<bool()>& pred, int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        usleep(20000);
    }
    return pred();
}

size_t count_occurrences(const std::string& haystack, const std::string& needle) {
    size_t count = 0, pos = 0;
    while ((pos = haystack.find(needle, pos)) != std::string::npos) {
        count++;
        pos += needle.size();
    }
    return count;
}

// 子进程先关闭继承的 fd（特别是父进程协调器的锁 fd），再进入运行模式
void child_close_fds() {
    for (int fd = 3; fd < 1024; fd++) close(fd);
}

pid_t fork_coordinator(const CoordinatorConfig& cfg) {
    std::cout.flush();
    pid_t pid = fork();
    if (pid == 0) {
        child_close_fds();
        _exit(ipc::coord::run_coordinator(cfg));
    }
    return pid;
}

pid_t fork_worker(const WorkerConfig& cfg) {
    std::cout.flush();
    pid_t pid = fork();
    if (pid == 0) {
        child_close_fds();
        _exit(ipc::coord::run_worker(cfg));
    }
    return pid;
}

// 等待子进程退出；超时则 SIGKILL。返回退出状态。
int reap_or_kill(pid_t pid, int timeout_ms) {
    int status = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) return status;
        usleep(20000);
    }
    kill(pid, SIGKILL);
    waitpid(pid, &status, 0);
    return status;
}

// 子进程守卫：测试断言失败抛异常时，确保 fork 出的协调器/worker 被清理，
// 避免泄漏的孙进程持有 stdout 管道导致整个测试运行悬挂
struct ChildGuard {
    std::vector<pid_t> pids;
    ~ChildGuard() {
        for (pid_t p : pids) kill(p, SIGKILL);
        for (pid_t p : pids) waitpid(p, nullptr, 0);
    }
    void add(pid_t p) { pids.push_back(p); }
    void remove(pid_t p) { pids.erase(std::remove(pids.begin(), pids.end(), p), pids.end()); }
};

// 直接向协调器队列注入一条确认消息（模拟 worker，含迟到/重复确认）
void inject_ack(int msqid, uint64_t job_id, uint32_t attempt, const std::string& result) {
    ipc::coord::proto::AckMsg ack;
    memset(&ack, 0, sizeof(ack));
    ack.mtype = ipc::coord::proto::MTYPE_ACK;
    ack.job_id = job_id;
    ack.attempt = attempt;
    ack.ok = 1;
    snprintf(ack.result, sizeof(ack.result), "%s", result.c_str());
    msgsnd(msqid, &ack, sizeof(ack) - sizeof(long), 0);
}

CoordinatorConfig base_coord_cfg(const std::string& dir) {
    CoordinatorConfig cfg;
    cfg.state_dir = dir;
    cfg.quiet = true;
    return cfg;
}

}  // namespace

// ---------------------------------------------------------------------------
// 1. 日志恢复：损坏尾部被忽略，在途作业重新排队
// ---------------------------------------------------------------------------
void test_coord_log_recovery() {
    std::string dir = make_temp_dir();
    ASSERT_FALSE(dir.empty());
    write_file(dir + "/jobs.txt", "1 alpha\n2 beta\n");

    CoordinatorConfig cfg = base_coord_cfg(dir);
    cfg.input_file = dir + "/jobs.txt";

    {
        Coordinator c1(cfg);
        ASSERT_TRUE(c1.ok());
        c1.poll_once();  // 派发两个作业（无 worker，停留在在途）
        ASSERT_EQ(c1.status().in_flight, (size_t)2);
        // 模拟崩溃前最后一写被撕裂：日志尾部只有半条记录
        append_file(dir + "/coordinator.log", "#deadbeef 173736 SUBMIT 99 brok");
        // c1 析构且不 shutdown() = 模拟崩溃（队列遗留、锁释放）
    }

    {
        Coordinator c2(cfg);
        ASSERT_TRUE(c2.ok());  // 损坏尾部被忽略，恢复成功
        // 崩溃时在途的作业已重新排队（不丢单）
        ASSERT_EQ(c2.status().pending, (size_t)2);
        ASSERT_EQ(c2.status().in_flight, (size_t)0);
        // 损坏尾部已被截断，日志不再包含垃圾内容
        std::string log = read_file(dir + "/coordinator.log");
        ASSERT_TRUE(log.find("brok") == std::string::npos);
        ASSERT_TRUE(log.back() == '\n');
        // 恢复时的租约作废有审计记录
        std::string h = job_history(dir, 1);
        ASSERT_TRUE(h.find("EXPIRE") != std::string::npos);
        ASSERT_TRUE(h.find("coordinator-restart") != std::string::npos);
        c2.shutdown();
        ASSERT_TRUE(c2.shutdown_done());
    }

    rm_rf(dir);
}

// ---------------------------------------------------------------------------
// 2. 基本生命周期：提交→派发→确认→成功；优雅关停删除队列并保存状态
// ---------------------------------------------------------------------------
void test_coord_basic_lifecycle() {
    std::string dir = make_temp_dir();
    write_file(dir + "/jobs.txt", "1 alpha\n2 beta\n3 gamma\n");
    ChildGuard guard;

    CoordinatorConfig ccfg = base_coord_cfg(dir);
    ccfg.input_file = dir + "/jobs.txt";
    ccfg.lease_ms = 2000;
    pid_t cpid = fork_coordinator(ccfg);
    ASSERT_TRUE(cpid > 0);
    guard.add(cpid);

    WorkerConfig wcfg;
    wcfg.state_dir = dir;
    wcfg.work_ms = 50;
    wcfg.reconnect_ms = 800;
    wcfg.quiet = true;
    pid_t wpid = fork_worker(wcfg);
    ASSERT_TRUE(wpid > 0);
    guard.add(wpid);

    // 等待全部成功
    ASSERT_TRUE(wait_for(
        [&] {
            StatusInfo st;
            std::string err;
            return read_status(dir, &st, &err) && st.succeeded == 3;
        },
        10000));

    // 结果正确且每个作业只成功一次
    StatusInfo st;
    std::string err;
    ASSERT_TRUE(read_status(dir, &st, &err));
    ASSERT_EQ(st.pending + st.in_flight + st.dead, (size_t)0);
    ASSERT_EQ(st.jobs.size(), (size_t)3);
    for (const auto& j : st.jobs) ASSERT_EQ(j.result, compute_result(j.payload));

    // 优雅关停
    kill(cpid, SIGTERM);
    int cstatus = reap_or_kill(cpid, 8000);
    guard.remove(cpid);
    ASSERT_TRUE(WIFEXITED(cstatus));
    ASSERT_EQ(WEXITSTATUS(cstatus), 0);

    // 协调器自己创建的消息队列已删除
    key_t key = ftok((dir + "/queue.key").c_str(), 'J');
    ASSERT_NE(key, -1);
    ASSERT_EQ(msgget(key, 0666), -1);
    ASSERT_EQ(errno, ENOENT);

    // 一致状态已保存：SHUTDOWN 记录 + 快照
    ASSERT_TRUE(file_exists(dir + "/snapshot.txt"));
    std::string snap = read_file(dir + "/snapshot.txt");
    ASSERT_TRUE(snap.find("succeeded 3") != std::string::npos);
    std::string log = read_file(dir + "/coordinator.log");
    ASSERT_TRUE(log.find("SHUTDOWN") != std::string::npos);

    // worker 在队列删除后自行退出
    int wstatus = reap_or_kill(wpid, 4000);
    guard.remove(wpid);
    ASSERT_TRUE(WIFEXITED(wstatus));
    ASSERT_EQ(WEXITSTATUS(wstatus), 0);

    rm_rf(dir);
}

// ---------------------------------------------------------------------------
// 3. 杀死 worker：租约过期后作业重新投递，不丢单、不重复完成
// ---------------------------------------------------------------------------
void test_coord_worker_killed_redelivery() {
    std::string dir = make_temp_dir();
    write_file(dir + "/jobs.txt", "1 alpha\n2 beta\n3 gamma\n");
    ChildGuard guard;

    CoordinatorConfig ccfg = base_coord_cfg(dir);
    ccfg.input_file = dir + "/jobs.txt";
    ccfg.lease_ms = 1000;
    pid_t cpid = fork_coordinator(ccfg);
    ASSERT_TRUE(cpid > 0);
    guard.add(cpid);

    // worker-1：收到 job 2 即"被杀"（模拟崩溃，不发送确认）
    WorkerConfig w1cfg;
    w1cfg.state_dir = dir;
    w1cfg.work_ms = 50;
    w1cfg.crash_on_job = 2;
    w1cfg.reconnect_ms = 800;
    w1cfg.quiet = true;
    pid_t w1 = fork_worker(w1cfg);
    guard.add(w1);
    int st1 = reap_or_kill(w1, 8000);
    guard.remove(w1);
    ASSERT_TRUE(WIFEXITED(st1));
    ASSERT_EQ(WEXITSTATUS(st1), 2);  // worker-1 已崩溃

    // worker-2 接替工作
    WorkerConfig w2cfg;
    w2cfg.state_dir = dir;
    w2cfg.work_ms = 50;
    w2cfg.reconnect_ms = 800;
    w2cfg.quiet = true;
    pid_t w2 = fork_worker(w2cfg);
    ASSERT_TRUE(w2 > 0);
    guard.add(w2);

    // 全部作业最终成功（job 2 经历崩溃重投）
    ASSERT_TRUE(wait_for(
        [&] {
            StatusInfo st;
            std::string err;
            return read_status(dir, &st, &err) && st.succeeded == 3;
        },
        10000));

    // job 2 的尝试历史：2 次派发、1 次租约过期、恰好 1 次成功确认
    // （记录行格式为 "时间  TYPE ..."，用双空格前缀与 final state 行区分）
    std::string h = job_history(dir, 2);
    ASSERT_EQ(count_occurrences(h, "  DISPATCH "), (size_t)2);
    ASSERT_EQ(count_occurrences(h, "  EXPIRE "), (size_t)1);
    ASSERT_EQ(count_occurrences(h, "  ACK "), (size_t)1);
    ASSERT_TRUE(h.find("SUCCEEDED") != std::string::npos);

    StatusInfo st;
    std::string err;
    ASSERT_TRUE(read_status(dir, &st, &err));
    ASSERT_EQ(st.succeeded, (size_t)3);
    ASSERT_EQ(st.dead, (size_t)0);

    kill(cpid, SIGTERM);
    reap_or_kill(cpid, 8000);
    guard.remove(cpid);
    reap_or_kill(w2, 4000);
    guard.remove(w2);
    rm_rf(dir);
}

// ---------------------------------------------------------------------------
// 4. 重启协调器：崩溃后从日志恢复，在途作业重新投递，worker 自动重连
// ---------------------------------------------------------------------------
void test_coord_coordinator_restart() {
    std::string dir = make_temp_dir();
    write_file(dir + "/jobs.txt", "1 a\n2 bb\n3 ccc\n4 dddd\n");
    ChildGuard guard;

    CoordinatorConfig cfg = base_coord_cfg(dir);
    cfg.input_file = dir + "/jobs.txt";
    cfg.lease_ms = 1500;

    auto c1 = std::make_unique<Coordinator>(cfg);
    ASSERT_TRUE(c1->ok());

    WorkerConfig wcfg;
    wcfg.state_dir = dir;
    wcfg.work_ms = 120;
    wcfg.reconnect_ms = 8000;  // 足够跨越协调器重启
    wcfg.quiet = true;
    pid_t wpid = fork_worker(wcfg);
    ASSERT_TRUE(wpid > 0);
    guard.add(wpid);

    // 至少完成一个作业后，模拟协调器崩溃
    ASSERT_TRUE(wait_for([&] {
        c1->poll_once();
        return c1->status().succeeded >= 1;
    }, 10000));
    c1.reset();  // 不 shutdown：模拟崩溃（队列遗留，锁随 fd 关闭释放）

    // 重启协调器：从日志恢复，在途作业重新排队
    auto c2 = std::make_unique<Coordinator>(cfg);
    ASSERT_TRUE(c2->ok());

    // worker 自动重连到新队列；全部作业最终成功且只计入一次
    ASSERT_TRUE(wait_for([&] {
        c2->poll_once();
        return c2->status().succeeded == 4;
    }, 15000));

    StatusInfo st = c2->status();
    ASSERT_EQ(st.succeeded, (size_t)4);
    ASSERT_EQ(st.dead, (size_t)0);
    for (const auto& j : st.jobs) ASSERT_EQ(j.result, compute_result(j.payload));

    c2->shutdown();
    reap_or_kill(wpid, 1000);
    guard.remove(wpid);
    rm_rf(dir);
}

// ---------------------------------------------------------------------------
// 5. 重复提交：已成功的作业直接返回原结果，不重新执行
// ---------------------------------------------------------------------------
void test_coord_duplicate_submit() {
    std::string dir = make_temp_dir();
    write_file(dir + "/jobs.txt", "1 alpha\n");
    ChildGuard guard;

    CoordinatorConfig ccfg = base_coord_cfg(dir);
    ccfg.input_file = dir + "/jobs.txt";
    ccfg.lease_ms = 2000;
    pid_t cpid = fork_coordinator(ccfg);
    ASSERT_TRUE(cpid > 0);
    guard.add(cpid);

    WorkerConfig wcfg;
    wcfg.state_dir = dir;
    wcfg.work_ms = 50;
    wcfg.reconnect_ms = 800;
    wcfg.quiet = true;
    pid_t wpid = fork_worker(wcfg);
    ASSERT_TRUE(wpid > 0);
    guard.add(wpid);

    ASSERT_TRUE(wait_for(
        [&] {
            StatusInfo st;
            std::string err;
            return read_status(dir, &st, &err) && st.succeeded == 1;
        },
        10000));

    // 运行时重复提交同一 job_id：直接返回原结果
    std::string out;
    int rc = cmd_submit(dir, 1, "alpha", &out);
    ASSERT_EQ(rc, 1);
    ASSERT_TRUE(out.find(compute_result("alpha")) != std::string::npos);

    // 成功计数不变，没有重新执行
    StatusInfo st;
    std::string err;
    ASSERT_TRUE(read_status(dir, &st, &err));
    ASSERT_EQ(st.succeeded, (size_t)1);
    ASSERT_EQ(st.jobs.size(), (size_t)1);

    // 停掉 worker 与协调器
    reap_or_kill(wpid, 1000);
    guard.remove(wpid);
    kill(cpid, SIGTERM);
    reap_or_kill(cpid, 8000);
    guard.remove(cpid);

    // 重启协调器并用同一输入文件再次提交：识别为重复，不重新执行
    pid_t cpid2 = fork_coordinator(ccfg);
    ASSERT_TRUE(cpid2 > 0);
    guard.add(cpid2);
    ASSERT_TRUE(wait_for(
        [&] {
            std::string log = read_file(dir + "/coordinator.log");
            return log.find("DUP_SUBMIT") != std::string::npos;
        },
        8000));

    ASSERT_TRUE(read_status(dir, &st, &err));
    ASSERT_EQ(st.succeeded, (size_t)1);
    ASSERT_EQ(st.pending + st.in_flight, (size_t)0);
    std::string h = job_history(dir, 1);
    ASSERT_EQ(count_occurrences(h, "  ACK "), (size_t)1);  // 仍然只有一次成功确认

    kill(cpid2, SIGTERM);
    reap_or_kill(cpid2, 8000);
    guard.remove(cpid2);
    rm_rf(dir);
}

// ---------------------------------------------------------------------------
// 6. 死信与重放：重试上限后进入死信，可查询历史，显式重放后成功
// ---------------------------------------------------------------------------
void test_coord_dead_letter_replay() {
    std::string dir = make_temp_dir();
    write_file(dir + "/jobs.txt", "1 flaky\n");
    ChildGuard guard;

    CoordinatorConfig ccfg = base_coord_cfg(dir);
    ccfg.input_file = dir + "/jobs.txt";
    ccfg.lease_ms = 1000;
    ccfg.max_attempts = 2;
    pid_t cpid = fork_coordinator(ccfg);
    ASSERT_TRUE(cpid > 0);
    guard.add(cpid);

    // worker 对 job 1 总是返回失败
    WorkerConfig w1cfg;
    w1cfg.state_dir = dir;
    w1cfg.work_ms = 30;
    w1cfg.fail_on_job = 1;
    w1cfg.reconnect_ms = 800;
    w1cfg.quiet = true;
    pid_t w1 = fork_worker(w1cfg);
    ASSERT_TRUE(w1 > 0);
    guard.add(w1);

    // 达到重试上限 → 死信
    ASSERT_TRUE(wait_for(
        [&] {
            StatusInfo st;
            std::string err;
            return read_status(dir, &st, &err) && st.dead == 1;
        },
        10000));
    reap_or_kill(w1, 1000);
    guard.remove(w1);

    // 尝试历史可查询：2 次派发、2 次失败确认、1 次死信
    std::string h = job_history(dir, 1);
    ASSERT_EQ(count_occurrences(h, "  DISPATCH "), (size_t)2);
    ASSERT_EQ(count_occurrences(h, "  NACK "), (size_t)2);
    ASSERT_EQ(count_occurrences(h, "  DEAD "), (size_t)1);
    ASSERT_TRUE(h.find("DEAD") != std::string::npos);

    // 换一个正常的 worker，显式重放死信
    WorkerConfig w2cfg;
    w2cfg.state_dir = dir;
    w2cfg.work_ms = 30;
    w2cfg.reconnect_ms = 800;
    w2cfg.quiet = true;
    pid_t w2 = fork_worker(w2cfg);
    ASSERT_TRUE(w2 > 0);
    guard.add(w2);

    std::string out;
    int rc = cmd_replay(dir, 1, &out);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(out.find("replayed 1") != std::string::npos);

    ASSERT_TRUE(wait_for(
        [&] {
            StatusInfo st;
            std::string err;
            return read_status(dir, &st, &err) && st.succeeded == 1 && st.dead == 0;
        },
        10000));

    kill(cpid, SIGTERM);
    reap_or_kill(cpid, 8000);
    guard.remove(cpid);
    reap_or_kill(w2, 4000);
    guard.remove(w2);
    rm_rf(dir);
}

// ---------------------------------------------------------------------------
// 7. 单实例：同一状态目录的第二个协调器必须失败退出
// ---------------------------------------------------------------------------
void test_coord_second_instance_fails() {
    std::string dir = make_temp_dir();

    CoordinatorConfig cfg = base_coord_cfg(dir);
    Coordinator a(cfg);
    ASSERT_TRUE(a.ok());

    // 同一状态目录的第二个协调器：构造即失败
    Coordinator b(cfg);
    ASSERT_FALSE(b.ok());
    ASSERT_TRUE(b.error().find("another coordinator") != std::string::npos);

    // 进程级运行模式同样以非零码失败退出
    ASSERT_EQ(run_coordinator(cfg), 2);

    a.shutdown();
    rm_rf(dir);
}

// ---------------------------------------------------------------------------
// 8. 迟到确认与重复结果：被识别且不计入成功
// ---------------------------------------------------------------------------
void test_coord_late_and_duplicate_ack() {
    std::string dir = make_temp_dir();

    CoordinatorConfig cfg = base_coord_cfg(dir);
    cfg.lease_ms = 300;
    cfg.max_attempts = 5;
    Coordinator c(cfg);
    ASSERT_TRUE(c.ok());

    ASSERT_EQ(c.submit_job(7, "payload-x", nullptr), 0);
    c.poll_once();  // 派发 attempt 1
    ASSERT_EQ(c.status().in_flight, (size_t)1);

    usleep(400000);  // 租约过期
    c.poll_once();   // EXPIRE + 重投 attempt 2
    ASSERT_EQ(c.status().in_flight, (size_t)1);

    // 迟到确认（attempt 1，已被取代）：识别，不计入
    inject_ack(c.queue_id(), 7, 1, "STALE:5");
    c.poll_once();
    ASSERT_EQ(c.status().succeeded, (size_t)0);
    ASSERT_EQ(c.status().in_flight, (size_t)1);
    std::string h = job_history(dir, 7);
    ASSERT_TRUE(h.find("LATE_ACK") != std::string::npos);

    // 正确确认（attempt 2）：成功
    inject_ack(c.queue_id(), 7, 2, compute_result("payload-x"));
    c.poll_once();
    ASSERT_EQ(c.status().succeeded, (size_t)1);

    // 重复结果（同一确认的重投）：识别，不重复计入
    inject_ack(c.queue_id(), 7, 2, compute_result("payload-x"));
    c.poll_once();
    ASSERT_EQ(c.status().succeeded, (size_t)1);
    h = job_history(dir, 7);
    ASSERT_TRUE(h.find("DUP_ACK") != std::string::npos);
    ASSERT_EQ(count_occurrences(h, " ACK "), (size_t)1);  // 成功确认只入账一次

    c.shutdown();
    rm_rf(dir);
}

void test_job_coordinator_suite() {
    run_test("coord_log_recovery", test_coord_log_recovery);
    run_test("coord_basic_lifecycle", test_coord_basic_lifecycle);
    run_test("coord_worker_killed_redelivery", test_coord_worker_killed_redelivery);
    run_test("coord_coordinator_restart", test_coord_coordinator_restart);
    run_test("coord_duplicate_submit", test_coord_duplicate_submit);
    run_test("coord_dead_letter_replay", test_coord_dead_letter_replay);
    run_test("coord_second_instance_fails", test_coord_second_instance_fails);
    run_test("coord_late_and_duplicate_ack", test_coord_late_and_duplicate_ack);
}
