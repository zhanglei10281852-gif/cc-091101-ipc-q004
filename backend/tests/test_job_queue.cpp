/**
 * 离线任务队列（协调器 / Worker）测试用例
 *
 * 单元测试：JobStore 状态机、日志恢复、损坏尾部处理
 * 集成测试：真实拉起协调器与 worker 进程，覆盖
 *   - worker 崩溃后租约过期重投递（不丢单）
 *   - 协调器被杀后重启恢复（不丢单、不重复完成）
 *   - 重复提交 / 迟到确认 / 重复结果被识别且不重复计入
 *   - 死信与重放、单实例锁、关停时租约结算与队列清理
 */

#include "test_framework.h"
#include "job_queue.h"

#include <sys/file.h>
#include <sys/stat.h>

using namespace test;
namespace jq = ipc::jobq;

#ifndef IPC_DEMO_BIN
#define IPC_DEMO_BIN "./ipc_demo"
#endif

namespace {

// ================= 测试工具 =================

std::string make_tmpdir(const char* tag) {
    std::string t = std::string("/tmp/jobq_") + tag + "_XXXXXX";
    std::vector<char> buf(t.begin(), t.end());
    buf.push_back('\0');
    char* d = mkdtemp(buf.data());
    if (!d) throw std::runtime_error("mkdtemp failed");
    return d;
}

void remove_dir(const std::string& dir) {
    std::string cmd = "rm -rf " + dir;
    if (system(cmd.c_str()) != 0) { /* 忽略清理失败 */ }
}

void write_file(const std::string& path, const std::string& content) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_TRUE(fd >= 0);
    ssize_t n = write(fd, content.data(), content.size());
    ASSERT_EQ(n, (ssize_t)content.size());
    close(fd);
}

void append_file(const std::string& path, const std::string& content) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    ASSERT_TRUE(fd >= 0);
    ssize_t n = write(fd, content.data(), content.size());
    ASSERT_EQ(n, (ssize_t)content.size());
    close(fd);
}

std::string read_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return "";
    std::string out;
    char buf[4096];
    ssize_t n;
    while ((n = read(fd, buf, sizeof(buf))) > 0) out.append(buf, (size_t)n);
    close(fd);
    return out;
}

// 统计以 prefix 开头的行数
int count_lines_with(const std::string& path, const std::string& prefix) {
    std::string content = read_file(path);
    int n = 0;
    size_t pos = 0;
    while (pos < content.size()) {
        size_t eol = content.find('\n', pos);
        std::string line = content.substr(pos, eol == std::string::npos
                                                   ? std::string::npos : eol - pos);
        if (!line.empty() && line.rfind(prefix, 0) == 0) n++;
        if (eol == std::string::npos) break;
        pos = eol + 1;
    }
    return n;
}

bool wait_for(std::function<bool()> cond, int timeout_ms) {
    for (int t = 0; t < timeout_ms; t += 25) {
        if (cond()) return true;
        usleep(25000);
    }
    return cond();
}

// 进程守护：测试异常退出时也能清理子进程与消息队列
struct ProcGuard {
    std::vector<pid_t> pids;
    std::string state_dir;

    ~ProcGuard() {
        for (pid_t p : pids) kill(p, SIGKILL);
        for (pid_t p : pids) {
            int st;
            waitpid(p, &st, 0);
        }
        if (!state_dir.empty()) {
            key_t k = jq::queue_key_for(state_dir);
            if (k != (key_t)-1) {
                int q = msgget(k, 0);
                if (q >= 0) msgctl(q, IPC_RMID, nullptr);
            }
        }
    }

    pid_t spawn(const std::vector<std::string>& args, const std::string& log_file) {
        pid_t pid = fork();
        if (pid == 0) {
            int fd = open(log_file.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (fd >= 0) {
                dup2(fd, STDOUT_FILENO);
                dup2(fd, STDERR_FILENO);
            }
            std::vector<char*> argv;
            for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
            argv.push_back(nullptr);
            execv(argv[0], argv.data());
            _exit(127);
        }
        if (pid > 0) pids.push_back(pid);
        return pid;
    }
};

struct CliOut {
    int exit_code = -1;
    std::string text;
};

// 运行 ipc_demo 子命令并捕获输出与退出码
CliOut run_cli(const std::vector<std::string>& args) {
    int pipefd[2];
    if (pipe(pipefd) != 0) throw std::runtime_error("pipe failed");
    pid_t pid = fork();
    if (pid == 0) {
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[0]);
        close(pipefd[1]);
        std::vector<char*> argv;
        for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
        argv.push_back(nullptr);
        execv(argv[0], argv.data());
        _exit(127);
    }
    close(pipefd[1]);
    CliOut out;
    char buf[4096];
    ssize_t n;
    while ((n = read(pipefd[0], buf, sizeof(buf))) > 0) out.text.append(buf, (size_t)n);
    close(pipefd[0]);
    int st = 0;
    waitpid(pid, &st, 0);
    out.exit_code = WIFEXITED(st) ? WEXITSTATUS(st) : 128;
    return out;
}

bool queue_exists(const std::string& dir) {
    key_t k = jq::queue_key_for(dir);
    if (k == (key_t)-1) return false;
    return msgget(k, 0) >= 0;
}

int count_log_records(const std::string& dir, const std::string& type) {
    int n = 0;
    jq::scan_job_log(dir + "/jobs.log", [&](const jq::LogRecord& r) {
        if (r.type == type) n++;
    });
    return n;
}

bool contains(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

// ================= 单元测试：JobStore =================

// 基本流程：提交 -> 派发 -> 确认成功
void test_jobq_store_basic() {
    std::string dir = make_tmpdir("basic");
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(dir + "/jobs.log", true));
        ASSERT_EQ(store.submit("job-1", "alpha"), jq::SubmitVerdict::NEW);
        ASSERT_TRUE(store.dispatch("job-1", 1, 1));
        ASSERT_EQ(store.on_result("job-1", 1, 0, "result:alpha"),
                  jq::ResultVerdict::ACCEPTED_SUCCESS);
        jq::Counts c = store.counts();
        ASSERT_EQ(c.succeeded, 1u);
        ASSERT_EQ(c.pending, 0u);
        ASSERT_EQ(c.inflight, 0u);
        const jq::Job* j = store.get("job-1");
        ASSERT_TRUE(j != nullptr);
        ASSERT_EQ(j->result, std::string("result:alpha"));
        store.close();
    }
    remove_dir(dir);
}

// 重复提交：同一 job_id 不重复受理；已成功作业返回原结果
void test_jobq_store_duplicate_submit() {
    std::string dir = make_tmpdir("dupsub");
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(dir + "/jobs.log", true));
        ASSERT_EQ(store.submit("job-1", "alpha"), jq::SubmitVerdict::NEW);
        ASSERT_EQ(store.submit("job-1", "alpha"), jq::SubmitVerdict::ALREADY_PENDING);
        ASSERT_TRUE(store.dispatch("job-1", 1, 1));
        ASSERT_EQ(store.submit("job-1", "alpha"), jq::SubmitVerdict::ALREADY_INFLIGHT);
        ASSERT_EQ(store.on_result("job-1", 1, 0, "result:alpha"),
                  jq::ResultVerdict::ACCEPTED_SUCCESS);
        // 已成功作业再次提交：不重复计入，原结果仍在
        ASSERT_EQ(store.submit("job-1", "alpha"), jq::SubmitVerdict::ALREADY_SUCCEEDED);
        jq::Counts c = store.counts();
        ASSERT_EQ(c.succeeded, 1u);
        ASSERT_EQ(c.dup_submits, 3u);
        const jq::Job* j = store.get("job-1");
        ASSERT_EQ(j->result, std::string("result:alpha"));
        store.close();
    }
    remove_dir(dir);
}

// 迟到确认与重复结果：必须被识别且不重复计入
void test_jobq_store_late_and_dup_ack() {
    std::string dir = make_tmpdir("lateack");
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(dir + "/jobs.log", true));
        store.submit("job-1", "alpha");
        store.dispatch("job-1", 1, 1);
        ASSERT_TRUE(store.expire("job-1", "lease-timeout"));   // 租约过期，重新入队
        ASSERT_TRUE(store.dispatch("job-1", 2, 2));            // 新租约 token=2
        // 旧租约的迟到确认：忽略
        ASSERT_EQ(store.on_result("job-1", 1, 0, "result:alpha"),
                  jq::ResultVerdict::LATE_ACK);
        ASSERT_EQ(store.counts().succeeded, 0u);
        // 当前租约的确认：接受
        ASSERT_EQ(store.on_result("job-1", 2, 0, "result:alpha"),
                  jq::ResultVerdict::ACCEPTED_SUCCESS);
        // 重复结果：忽略，不重复计入
        ASSERT_EQ(store.on_result("job-1", 2, 0, "result:alpha"),
                  jq::ResultVerdict::DUP_ACK);
        // 未知作业
        ASSERT_EQ(store.on_result("job-x", 9, 0, "zzz"),
                  jq::ResultVerdict::UNKNOWN_JOB);
        jq::Counts c = store.counts();
        ASSERT_EQ(c.succeeded, 1u);
        ASSERT_EQ(c.late_acks, 1u);
        ASSERT_EQ(c.dup_acks, 1u);
        store.close();
    }
    remove_dir(dir);
}

// 达到重试上限进入死信；死信后的确认被忽略
void test_jobq_store_dead_letter() {
    std::string dir = make_tmpdir("dead");
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(dir + "/jobs.log", true));
        const int max_attempts = 2;
        store.submit("job-1", "alpha");
        store.dispatch("job-1", 1, 1);
        store.expire("job-1", "lease-timeout");
        store.dispatch("job-1", 2, 2);
        store.expire("job-1", "lease-timeout");
        const jq::Job* j = store.get("job-1");
        ASSERT_EQ((int)j->attempts, max_attempts);
        ASSERT_TRUE(store.mark_dead("job-1", "max-attempts"));
        ASSERT_EQ(store.counts().dead, 1u);
        ASSERT_EQ(store.counts().pending, 0u);
        // 死信后的迟到确认：忽略
        ASSERT_EQ(store.on_result("job-1", 2, 0, "result:alpha"),
                  jq::ResultVerdict::LATE_ACK);
        ASSERT_EQ(store.counts().succeeded, 0u);
        store.close();
    }
    remove_dir(dir);
}

// 死信重放：重新入队且尝试计数清零，之后可以成功
void test_jobq_store_replay() {
    std::string dir = make_tmpdir("replay");
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(dir + "/jobs.log", true));
        store.submit("job-1", "alpha");
        store.dispatch("job-1", 1, 1);
        store.expire("job-1", "lease-timeout");
        store.mark_dead("job-1", "max-attempts");
        // 非死信不能重放
        ASSERT_FALSE(store.replay("job-x"));
        ASSERT_TRUE(store.replay("job-1"));
        const jq::Job* j = store.get("job-1");
        ASSERT_TRUE(j->state == jq::JobState::PENDING);
        ASSERT_EQ(j->attempts, 0u);
        // 重放后重新走完整流程
        ASSERT_TRUE(store.dispatch("job-1", 2, 1));
        ASSERT_EQ(store.on_result("job-1", 2, 0, "result:alpha"),
                  jq::ResultVerdict::ACCEPTED_SUCCESS);
        ASSERT_EQ(store.counts().succeeded, 1u);
        ASSERT_EQ(store.counts().dead, 0u);
        store.close();
    }
    remove_dir(dir);
}

// 重启恢复：重建待处理 / 处理中 / 成功 / 死信四个集合
void test_jobq_store_recovery() {
    std::string dir = make_tmpdir("recover");
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(dir + "/jobs.log", true));
        store.submit("job-a", "1");
        store.submit("job-b", "2");
        store.submit("job-c", "3");
        store.submit("job-d", "4");
        store.dispatch("job-a", 1, 1);
        store.on_result("job-a", 1, 0, "result:1");   // 成功
        store.dispatch("job-b", 2, 1);                // 处理中（崩溃时）
        store.dispatch("job-c", 3, 1);
        store.expire("job-c", "lease-timeout");
        store.mark_dead("job-c", "max-attempts");     // 死信
        store.close();                                // job-d 留在待处理
    }
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(dir + "/jobs.log", true));
        jq::Counts c = store.counts();
        ASSERT_EQ(c.pending, 1u);     // job-d
        ASSERT_EQ(c.inflight, 1u);    // job-b
        ASSERT_EQ(c.succeeded, 1u);   // job-a
        ASSERT_EQ(c.dead, 1u);        // job-c
        ASSERT_EQ(store.max_token(), 3u);  // token 不回退，避免新旧租约混淆
        // 重启后处理中租约立即过期，重新入队
        ASSERT_EQ(store.expire_all_inflight("coordinator-restart", 3), 1u);
        ASSERT_EQ(store.counts().pending, 2u);
        ASSERT_EQ(store.counts().inflight, 0u);
        // 恢复后的状态可以继续推进（新租约 token 必须大于历史最大值）
        ASSERT_TRUE(store.dispatch("job-d", 4, 1));
        ASSERT_EQ(store.on_result("job-d", 4, 0, "result:4"),
                  jq::ResultVerdict::ACCEPTED_SUCCESS);
        store.close();
    }
    remove_dir(dir);
}

// 损坏尾部：半条记录被忽略，良好前缀正常恢复，可继续追加
void test_jobq_store_corrupt_tail() {
    std::string dir = make_tmpdir("corrupt");
    std::string log = dir + "/jobs.log";
    off_t good_size = 0;
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(log, true));
        store.submit("job-1", "alpha");
        store.submit("job-2", "beta");
        store.dispatch("job-1", 1, 1);
        store.on_result("job-1", 1, 0, "result:alpha");
        store.close();
        struct stat st;
        ASSERT_EQ(stat(log.c_str(), &st), 0);
        good_size = st.st_size;
    }
    // 模拟崩溃导致的半条记录
    append_file(log, "SUBMIT|1700|job-");
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(log, true));
        ASSERT_TRUE(store.had_corrupt_tail());
        jq::Counts c = store.counts();
        ASSERT_EQ(c.succeeded, 1u);
        ASSERT_EQ(c.pending, 1u);
        // 损坏尾部已被截断
        struct stat st;
        ASSERT_EQ(stat(log.c_str(), &st), 0);
        ASSERT_EQ((off_t)st.st_size, good_size);
        // 可以继续追加新记录
        ASSERT_EQ(store.submit("job-3", "gamma"), jq::SubmitVerdict::NEW);
        store.close();
    }
    // 再恢复一次：状态完整且无损坏
    {
        jq::JobStore store;
        ASSERT_TRUE(store.open(log, true));
        ASSERT_FALSE(store.had_corrupt_tail());
        jq::Counts c = store.counts();
        ASSERT_EQ(c.succeeded, 1u);
        ASSERT_EQ(c.pending, 2u);
        store.close();
    }
    remove_dir(dir);
}

// ================= 集成测试 =================

// 两个协调器指向同一状态目录：后启动者必须失败退出
void test_jobq_single_coordinator() {
    std::string dir = make_tmpdir("lock");
    ProcGuard guard;
    guard.state_dir = dir;

    pid_t first = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                               "--lease-ms", "500", "--poll-ms", "20"},
                              dir + "/coord1.log");
    ASSERT_TRUE(wait_for([&] { return queue_exists(dir); }, 5000));

    CliOut second = run_cli({IPC_DEMO_BIN, "coordinator", "--state-dir", dir});
    ASSERT_NE(second.exit_code, 0);

    kill(first, SIGTERM);
    int st = 0;
    waitpid(first, &st, 0);
    ASSERT_TRUE(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    remove_dir(dir);
}

// 端到端：多 worker 并行，全部作业恰好完成一次；关停后队列被删除
void test_jobq_end_to_end() {
    std::string dir = make_tmpdir("e2e");
    write_file(dir + "/jobs.txt",
               "job-1 alpha\njob-2 beta\njob-3 gamma\n");
    ProcGuard guard;
    guard.state_dir = dir;

    pid_t coord = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                               "--input", dir + "/jobs.txt",
                               "--lease-ms", "600", "--poll-ms", "20"},
                              dir + "/coord.log");
    ASSERT_TRUE(wait_for([&] { return queue_exists(dir); }, 5000));
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir, "--name", "w1"},
                dir + "/w1.log");
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir, "--name", "w2"},
                dir + "/w2.log");

    ASSERT_TRUE(wait_for([&] {
        return count_lines_with(dir + "/results.log", "job-") == 3;
    }, 15000));

    std::string results = read_file(dir + "/results.log");
    ASSERT_TRUE(contains(results, "job-1\tresult:alpha"));
    ASSERT_TRUE(contains(results, "job-2\tresult:beta"));
    ASSERT_TRUE(contains(results, "job-3\tresult:gamma"));

    CliOut st = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
    ASSERT_EQ(st.exit_code, 0);
    ASSERT_TRUE(contains(st.text, "succeeded=3"));
    ASSERT_TRUE(contains(st.text, "pending=0"));
    ASSERT_TRUE(contains(st.text, "inflight=0"));

    // 关停：保存状态并删除自己创建的消息队列
    kill(coord, SIGTERM);
    int wst = 0;
    waitpid(coord, &wst, 0);
    ASSERT_TRUE(WIFEXITED(wst) && WEXITSTATUS(wst) == 0);
    ASSERT_FALSE(queue_exists(dir));
    remove_dir(dir);
}

// worker 崩溃：租约过期后重投递，作业不丢失且只完成一次
void test_jobq_worker_crash() {
    std::string dir = make_tmpdir("wcrash");
    write_file(dir + "/jobs.txt", "job-c1 payload-c1\n");
    ProcGuard guard;
    guard.state_dir = dir;

    pid_t coord = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                               "--input", dir + "/jobs.txt",
                               "--lease-ms", "400", "--poll-ms", "20",
                               "--max-attempts", "3"},
                              dir + "/coord.log");
    ASSERT_TRUE(wait_for([&] { return queue_exists(dir); }, 5000));

    // 崩溃 worker：收到第一个任务后不确认直接退出
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir,
                 "--name", "crasher", "--crash-after", "0"},
                dir + "/crasher.log");

    // 等待租约过期并重新派发（DISPATCH 出现两次）
    ASSERT_TRUE(wait_for([&] {
        return count_log_records(dir, "DISPATCH") >= 2;
    }, 10000));

    // 正常 worker 接手
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir, "--name", "good"},
                dir + "/good.log");
    ASSERT_TRUE(wait_for([&] {
        return count_lines_with(dir + "/results.log", "job-c1\t") == 1;
    }, 10000));

    // 尝试历史：一次过期、两次派发、一次成功
    CliOut h = run_cli({IPC_DEMO_BIN, "history", "--state-dir", dir,
                        "--job-id", "job-c1"});
    ASSERT_EQ(h.exit_code, 0);
    ASSERT_TRUE(contains(h.text, "EXPIRE"));
    ASSERT_TRUE(contains(h.text, "SUCCESS"));
    ASSERT_EQ(count_log_records(dir, "DISPATCH"), 2);
    ASSERT_EQ(count_log_records(dir, "SUCCESS"), 1);
    ASSERT_EQ(count_lines_with(dir + "/results.log", "job-c1\t"), 1);

    kill(coord, SIGTERM);
    int wst = 0;
    waitpid(coord, &wst, 0);
    remove_dir(dir);
}

// 协调器被杀（kill -9）后重启：从日志恢复，不丢单也不重复完成
void test_jobq_coordinator_restart() {
    std::string dir = make_tmpdir("restart");
    write_file(dir + "/jobs.txt",
               "job-r1 p1\njob-r2 p2\njob-r3 p3\njob-r4 p4\n");
    ProcGuard guard;
    guard.state_dir = dir;

    pid_t coord1 = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                                "--input", dir + "/jobs.txt",
                                "--lease-ms", "500", "--poll-ms", "20"},
                               dir + "/coord1.log");
    ASSERT_TRUE(wait_for([&] { return queue_exists(dir); }, 5000));
    // 慢 worker，保证崩溃时仍有作业未完成
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir,
                 "--name", "w1", "--delay-ms", "300"},
                dir + "/w1.log");

    ASSERT_TRUE(wait_for([&] {
        return count_lines_with(dir + "/results.log", "job-") >= 1;
    }, 15000));

    // 模拟协调器崩溃：SIGKILL，无机会清理队列
    kill(coord1, SIGKILL);
    int st = 0;
    waitpid(coord1, &st, 0);
    ASSERT_TRUE(WIFSIGNALED(st));

    // 重启协调器（同一状态目录）：清理残留队列并从日志恢复
    pid_t coord2 = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                                "--input", dir + "/jobs.txt",
                                "--lease-ms", "500", "--poll-ms", "20"},
                               dir + "/coord2.log");
    // 等待第二个协调器完成恢复（日志中出现第二条 BOOT）
    ASSERT_TRUE(wait_for([&] { return count_log_records(dir, "BOOT") >= 2; }, 5000));

    // 新 worker 接手（旧 worker 会随旧队列删除而退出）
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir, "--name", "w2"},
                dir + "/w2.log");

    // 全部 4 个作业恰好完成一次：不丢单、不重复
    ASSERT_TRUE(wait_for([&] {
        return count_lines_with(dir + "/results.log", "job-") == 4;
    }, 20000));
    for (const char* j : {"job-r1", "job-r2", "job-r3", "job-r4"}) {
        ASSERT_EQ(count_lines_with(dir + "/results.log", std::string(j) + "\t"), 1);
    }
    CliOut st2 = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
    ASSERT_TRUE(contains(st2.text, "succeeded=4"));

    kill(coord2, SIGTERM);
    waitpid(coord2, &st, 0);
    remove_dir(dir);
}

// 重复提交：CLI 与输入文件两条路径都不重复计入；已成功作业直接返回原结果
void test_jobq_duplicate_submission() {
    std::string dir = make_tmpdir("dupsubm");
    write_file(dir + "/jobs.txt", "job-d1 payload-d1\n");
    ProcGuard guard;
    guard.state_dir = dir;

    pid_t coord = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                               "--input", dir + "/jobs.txt",
                               "--lease-ms", "500", "--poll-ms", "20"},
                              dir + "/coord.log");
    ASSERT_TRUE(wait_for([&] { return queue_exists(dir); }, 5000));
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir, "--name", "w1"},
                dir + "/w1.log");
    ASSERT_TRUE(wait_for([&] {
        return count_lines_with(dir + "/results.log", "job-d1\t") == 1;
    }, 10000));

    // CLI 重复提交已成功作业：直接返回原结果
    CliOut s1 = run_cli({IPC_DEMO_BIN, "submit", "--state-dir", dir,
                         "--job-id", "job-d1", "--payload", "payload-d1"});
    ASSERT_EQ(s1.exit_code, 0);
    ASSERT_TRUE(contains(s1.text, "ALREADY_DONE"));
    ASSERT_TRUE(contains(s1.text, "result:payload-d1"));

    // CLI 提交新作业：正常受理
    CliOut s2 = run_cli({IPC_DEMO_BIN, "submit", "--state-dir", dir,
                         "--job-id", "job-d2", "--payload", "payload-d2"});
    ASSERT_EQ(s2.exit_code, 0);
    ASSERT_TRUE(contains(s2.text, "ACCEPTED"));

    // 输入文件重复提交同一 job_id
    append_file(dir + "/jobs.txt", "job-d1 payload-d1-again\n");

    // 等待两次重复提交都被识别（CLI 一次 + 文件一次）
    ASSERT_TRUE(wait_for([&] {
        CliOut st = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
        return contains(st.text, "dup_submits=2");
    }, 10000));

    // job-d2 正常完成；job-d1 没有被重复执行
    ASSERT_TRUE(wait_for([&] {
        return count_lines_with(dir + "/results.log", "job-d2\t") == 1;
    }, 10000));
    ASSERT_EQ(count_lines_with(dir + "/results.log", "job-d1\t"), 1);
    CliOut st = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
    ASSERT_TRUE(contains(st.text, "succeeded=2"));

    kill(coord, SIGTERM);
    int wst = 0;
    waitpid(coord, &wst, 0);
    remove_dir(dir);
}

// 迟到确认：慢 worker 超出租约，重投递由快 worker 完成；迟到确认被识别且不计入
void test_jobq_late_ack() {
    std::string dir = make_tmpdir("late");
    write_file(dir + "/jobs.txt", "job-s1 payload-s1\n");
    ProcGuard guard;
    guard.state_dir = dir;

    pid_t coord = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                               "--input", dir + "/jobs.txt",
                               "--lease-ms", "400", "--poll-ms", "20",
                               "--max-attempts", "5"},
                              dir + "/coord.log");
    ASSERT_TRUE(wait_for([&] { return queue_exists(dir); }, 5000));

    // 慢 worker：处理耗时超出租约，且只处理一个任务就退出
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir,
                 "--name", "slow", "--delay-ms", "900", "--exit-after", "1"},
                dir + "/slow.log");

    // 等待迟到确认被识别（旧租约的确认到达时租约已过期）
    ASSERT_TRUE(wait_for([&] {
        CliOut st = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
        return contains(st.text, "late_acks=") && !contains(st.text, "late_acks=0");
    }, 15000));

    // 快 worker 完成重投递的任务
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir, "--name", "fast"},
                dir + "/fast.log");
    ASSERT_TRUE(wait_for([&] {
        return count_lines_with(dir + "/results.log", "job-s1\t") == 1;
    }, 10000));

    // 恰好成功一次
    ASSERT_EQ(count_lines_with(dir + "/results.log", "job-s1\t"), 1);
    ASSERT_EQ(count_log_records(dir, "SUCCESS"), 1);
    CliOut st = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
    ASSERT_TRUE(contains(st.text, "succeeded=1"));

    kill(coord, SIGTERM);
    int wst = 0;
    waitpid(coord, &wst, 0);
    remove_dir(dir);
}

// 重复结果：worker 重复发送确认，协调器识别且不重复计入
void test_jobq_duplicate_result() {
    std::string dir = make_tmpdir("dupres");
    write_file(dir + "/jobs.txt", "job-q1 payload-q1\n");
    ProcGuard guard;
    guard.state_dir = dir;

    pid_t coord = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                               "--input", dir + "/jobs.txt",
                               "--lease-ms", "500", "--poll-ms", "20"},
                              dir + "/coord.log");
    ASSERT_TRUE(wait_for([&] { return queue_exists(dir); }, 5000));
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir,
                 "--name", "dup", "--send-duplicates", "--exit-after", "1"},
                dir + "/dup.log");

    ASSERT_TRUE(wait_for([&] {
        return count_lines_with(dir + "/results.log", "job-q1\t") == 1;
    }, 10000));
    // 等待重复确认被识别
    ASSERT_TRUE(wait_for([&] {
        CliOut st = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
        return contains(st.text, "dup_acks=1");
    }, 10000));

    ASSERT_EQ(count_lines_with(dir + "/results.log", "job-q1\t"), 1);
    ASSERT_EQ(count_log_records(dir, "SUCCESS"), 1);

    kill(coord, SIGTERM);
    int wst = 0;
    waitpid(coord, &wst, 0);
    remove_dir(dir);
}

// 死信与在线重放：无 worker 时达到重试上限进入死信，重放后由 worker 完成
void test_jobq_dead_letter_replay() {
    std::string dir = make_tmpdir("dlq");
    write_file(dir + "/jobs.txt", "job-x1 payload-x1\n");
    ProcGuard guard;
    guard.state_dir = dir;

    pid_t coord = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                               "--input", dir + "/jobs.txt",
                               "--lease-ms", "600", "--poll-ms", "20",
                               "--max-attempts", "2"},
                              dir + "/coord.log");
    ASSERT_TRUE(wait_for([&] { return queue_exists(dir); }, 5000));

    // 没有 worker：两次尝试都过期后进入死信
    ASSERT_TRUE(wait_for([&] {
        CliOut st = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
        return contains(st.text, "dead=1");
    }, 15000));

    // 在线重放死信
    CliOut rp = run_cli({IPC_DEMO_BIN, "replay", "--state-dir", dir,
                         "--job-id", "job-x1"});
    ASSERT_EQ(rp.exit_code, 0);
    ASSERT_TRUE(contains(rp.text, "REPLAYED"));

    // 启动 worker，重放后的作业在租约内完成
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir, "--name", "w1"},
                dir + "/w1.log");
    ASSERT_TRUE(wait_for([&] {
        return count_lines_with(dir + "/results.log", "job-x1\t") == 1;
    }, 10000));

    // 尝试历史完整：过期 -> 死信 -> 重放 -> 成功
    CliOut h = run_cli({IPC_DEMO_BIN, "history", "--state-dir", dir,
                        "--job-id", "job-x1"});
    ASSERT_EQ(h.exit_code, 0);
    ASSERT_TRUE(contains(h.text, "EXPIRE"));
    ASSERT_TRUE(contains(h.text, "DEAD"));
    ASSERT_TRUE(contains(h.text, "REPLAY"));
    ASSERT_TRUE(contains(h.text, "SUCCESS"));
    ASSERT_TRUE(contains(h.text, "final_state=SUCCEEDED"));

    CliOut st = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
    ASSERT_TRUE(contains(st.text, "dead=0"));
    ASSERT_TRUE(contains(st.text, "succeeded=1"));

    kill(coord, SIGTERM);
    int wst = 0;
    waitpid(coord, &wst, 0);
    remove_dir(dir);
}

// 离线重放：协调器未运行时 replay 命令直接修改日志
void test_jobq_offline_replay() {
    std::string dir = make_tmpdir("offreplay");
    {
        // 直接构造一份包含死信的状态日志
        jq::JobStore store;
        ASSERT_TRUE(store.open(dir + "/jobs.log", true));
        store.submit("job-off", "payload-off");
        store.dispatch("job-off", 1, 1);
        store.expire("job-off", "lease-timeout");
        store.mark_dead("job-off", "max-attempts");
        store.close();
    }
    CliOut st1 = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
    ASSERT_TRUE(contains(st1.text, "dead=1"));

    CliOut rp = run_cli({IPC_DEMO_BIN, "replay", "--state-dir", dir,
                         "--job-id", "job-off"});
    ASSERT_EQ(rp.exit_code, 0);
    ASSERT_TRUE(contains(rp.text, "REPLAYED"));

    CliOut st2 = run_cli({IPC_DEMO_BIN, "status", "--state-dir", dir});
    ASSERT_TRUE(contains(st2.text, "pending=1"));
    ASSERT_TRUE(contains(st2.text, "dead=0"));

    // 已不在死信中：重复重放失败
    CliOut rp2 = run_cli({IPC_DEMO_BIN, "replay", "--state-dir", dir,
                          "--job-id", "job-off"});
    ASSERT_NE(rp2.exit_code, 0);
    ASSERT_TRUE(contains(rp2.text, "NOT_DEAD"));
    remove_dir(dir);
}

// 关停结算：不再接收新作业，等待当前租约完成，保存状态并删除队列
void test_jobq_shutdown_drain() {
    std::string dir = make_tmpdir("drain");
    write_file(dir + "/jobs.txt", "job-g1 payload-g1\n");
    ProcGuard guard;
    guard.state_dir = dir;

    pid_t coord = guard.spawn({IPC_DEMO_BIN, "coordinator", "--state-dir", dir,
                               "--input", dir + "/jobs.txt",
                               "--lease-ms", "2000", "--poll-ms", "20"},
                              dir + "/coord.log");
    ASSERT_TRUE(wait_for([&] { return queue_exists(dir); }, 5000));
    guard.spawn({IPC_DEMO_BIN, "worker", "--state-dir", dir,
                 "--name", "w1", "--delay-ms", "800"},
                dir + "/w1.log");

    // 等任务下发（租约开始计时）
    ASSERT_TRUE(wait_for([&] { return count_log_records(dir, "DISPATCH") >= 1; },
                       5000));

    // 关停：worker 需要 800ms 才确认，协调器应等待租约结算
    kill(coord, SIGTERM);

    // 关停期间提交新作业：被拒绝
    CliOut s = run_cli({IPC_DEMO_BIN, "submit", "--state-dir", dir,
                        "--job-id", "job-late", "--payload", "x"});
    ASSERT_EQ(s.exit_code, 3);
    ASSERT_TRUE(contains(s.text, "SHUTTING_DOWN"));

    // 协调器等待处理中的作业完成后退出
    int wst = 0;
    ASSERT_TRUE(wait_for([&] {
        return waitpid(coord, &wst, WNOHANG) == coord;
    }, 10000));
    ASSERT_TRUE(WIFEXITED(wst) && WEXITSTATUS(wst) == 0);

    // 处理中的作业已在关停期间结算完成，队列已删除
    ASSERT_EQ(count_lines_with(dir + "/results.log", "job-g1\t"), 1);
    ASSERT_FALSE(queue_exists(dir));
    remove_dir(dir);
}

} // namespace

void test_job_queue_suite() {
    // 单元测试：状态机与日志
    run_test("jobq_store_basic", test_jobq_store_basic);
    run_test("jobq_store_duplicate_submit", test_jobq_store_duplicate_submit);
    run_test("jobq_store_late_and_dup_ack", test_jobq_store_late_and_dup_ack);
    run_test("jobq_store_dead_letter", test_jobq_store_dead_letter);
    run_test("jobq_store_replay", test_jobq_store_replay);
    run_test("jobq_store_recovery", test_jobq_store_recovery);
    run_test("jobq_store_corrupt_tail", test_jobq_store_corrupt_tail);
    // 集成测试：真实进程
    run_test("jobq_single_coordinator", test_jobq_single_coordinator);
    run_test("jobq_end_to_end", test_jobq_end_to_end);
    run_test("jobq_worker_crash", test_jobq_worker_crash);
    run_test("jobq_coordinator_restart", test_jobq_coordinator_restart);
    run_test("jobq_duplicate_submission", test_jobq_duplicate_submission);
    run_test("jobq_late_ack", test_jobq_late_ack);
    run_test("jobq_duplicate_result", test_jobq_duplicate_result);
    run_test("jobq_dead_letter_replay", test_jobq_dead_letter_replay);
    run_test("jobq_offline_replay", test_jobq_offline_replay);
    run_test("jobq_shutdown_drain", test_jobq_shutdown_drain);
}
