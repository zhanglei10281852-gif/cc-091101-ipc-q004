/**
 * 离线任务队列：协调器 / Worker 运行模式与管理命令
 *
 * 运行模式：
 *   ipc_demo coordinator --state-dir DIR [--input FILE] [--lease-ms N]
 *                        [--max-attempts N] [--poll-ms N]
 *   ipc_demo worker      --state-dir DIR [--name N] [--delay-ms N]
 *                        [--crash-after N] [--exit-after N] [--send-duplicates]
 *
 * 管理命令：
 *   ipc_demo submit  --state-dir DIR --job-id ID [--payload TEXT]
 *   ipc_demo status  --state-dir DIR
 *   ipc_demo history --state-dir DIR --job-id ID
 *   ipc_demo replay  --state-dir DIR (--job-id ID | --all)
 *
 * 语义：
 * - 作业只有收到对应租约 token 的完成确认才算成功
 * - 租约超时未确认的作业重新投递；迟到确认与重复结果被识别且不计入
 * - 状态全部落在 <state-dir>/jobs.log（追加 + fsync），重启后重建
 * - 同一状态目录只允许一个协调器（coordinator.lock 上的 flock）
 * - 关停（SIGTERM/SIGINT）时停止受理新作业，等待当前租约结算，
 *   保存一致状态后删除自己创建的消息队列
 */

#include "ipc_demo.h"
#include "job_queue.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <chrono>
#include <map>
#include <set>

namespace ipc {
namespace jobq {

namespace {

// ================= 命令行参数 =================

struct Args {
    std::map<std::string, std::string> kv;
    std::set<std::string> flags;

    static Args parse(int argc, char** argv) {
        Args a;
        for (int i = 0; i < argc; i++) {
            std::string s = argv[i];
            if (s.rfind("--", 0) == 0) {
                if (i + 1 < argc && std::string(argv[i + 1]).rfind("--", 0) != 0)
                    a.kv[s] = argv[++i];
                else
                    a.flags.insert(s);
            }
        }
        return a;
    }
    bool has(const std::string& k) const { return flags.count(k) || kv.count(k); }
    std::string get(const std::string& k, const std::string& def = "") const {
        auto it = kv.find(k);
        return it == kv.end() ? def : it->second;
    }
    int get_int(const std::string& k, int def) const {
        auto it = kv.find(k);
        if (it == kv.end()) return def;
        try { return std::stoi(it->second); } catch (...) { return def; }
    }
};

// ================= 信号 =================

volatile sig_atomic_t g_stop = 0;

void on_stop_signal(int) { g_stop = 1; }

void install_signal_handlers() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_stop_signal;   // 不带 SA_RESTART：让阻塞的 msgrcv 被中断
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGQUIT, &sa, nullptr);
    sigaction(SIGHUP, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);
}

// ================= 小工具 =================

bool mkdir_p(const std::string& path) {
    if (path.empty()) return false;
    std::string cur;
    for (size_t i = 0; i < path.size(); i++) {
        cur += path[i];
        if (path[i] == '/' || i == path.size() - 1) {
            if (cur.size() > 1 && mkdir(cur.c_str(), 0755) != 0 && errno != EEXIST)
                return false;
        }
    }
    return true;
}

std::string cstr(const char* buf, size_t max_len) {
    size_t n = 0;
    while (n < max_len && buf[n] != '\0') n++;
    return std::string(buf, n);
}

void write_all(int fd, const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
        ssize_t n = write(fd, s.data() + off, s.size() - off);
        if (n <= 0) break;
        off += (size_t)n;
    }
}

// 等待控制命令回复（带超时）；0 = 收到，1 = 超时，<0 = 出错
int wait_reply(int reply_qid, ReplyMsg& rep, int timeout_ms) {
    for (int t = 0; t < timeout_ms; t += 10) {
        ssize_t n = msgrcv(reply_qid, &rep, sizeof(rep) - sizeof(long), 0, IPC_NOWAIT);
        if (n > 0) return 0;
        if (n < 0 && errno != ENOMSG && errno != EINTR) return -1;
        usleep(10000);
    }
    return 1;
}

// ================= 协调器 =================

struct Coordinator {
    std::string state_dir;
    std::string input_path;
    int lease_ms = 2000;
    int max_attempts = 3;
    int poll_ms = 50;

    int lock_fd = -1;
    int qid = -1;
    int results_fd = -1;
    int input_fd = -1;
    off_t input_off = 0;
    std::string input_buf;
    JobStore store;
    uint64_t last_token = 0;
    bool draining = false;

    int run();
    void drain_messages();
    void handle_result(const ResultMsg& rm);
    void handle_control(const ControlMsg& cm);
    void send_reply(const ControlMsg& cm, int32_t status, int32_t extra,
                    const std::string& result);
    void check_leases();
    void dispatch_pending();
    void poll_input();
    void handle_input_line(const std::string& line);
};

void Coordinator::send_reply(const ControlMsg& cm, int32_t status, int32_t extra,
                             const std::string& result) {
    if (cm.reply_qid < 0) return;
    ReplyMsg rep;
    memset(&rep, 0, sizeof(rep));
    rep.mtype = 1;
    rep.status = status;
    rep.extra = extra;
    strncpy(rep.result, result.c_str(), RESULT_LEN - 1);
    msgsnd(cm.reply_qid, &rep, sizeof(rep) - sizeof(long), IPC_NOWAIT);
}

void Coordinator::handle_result(const ResultMsg& rm) {
    std::string id = cstr(rm.job_id, JOB_ID_LEN);
    std::string result = cstr(rm.result, RESULT_LEN);
    ResultVerdict v = store.on_result(id, rm.token, rm.status, result);
    switch (v) {
        case ResultVerdict::ACCEPTED_SUCCESS: {
            std::string line = id + "\t" + result + "\n";
            write_all(results_fd, line);
            fsync(results_fd);
            Logger::info("COORD", "作业完成 job=" + id + " result=" + result);
            break;
        }
        case ResultVerdict::ACCEPTED_FAILURE: {
            Logger::warn("COORD", "worker 报告失败，已重新入队 job=" + id +
                                  " error=" + result);
            const Job* j = store.get(id);
            if (j && (int)j->attempts >= max_attempts) {
                store.mark_dead(id, "max-attempts");
                Logger::error("COORD", "达到重试上限，进入死信 job=" + id);
            }
            break;
        }
        case ResultVerdict::LATE_ACK:
            Logger::warn("COORD", "迟到确认已识别并忽略 job=" + id +
                                  " token=" + std::to_string(rm.token));
            break;
        case ResultVerdict::DUP_ACK:
            Logger::warn("COORD", "重复结果已识别并忽略 job=" + id +
                                  " token=" + std::to_string(rm.token));
            break;
        case ResultVerdict::UNKNOWN_JOB:
            Logger::warn("COORD", "未知作业的确认，忽略 job=" + id);
            break;
    }
}

void Coordinator::handle_control(const ControlMsg& cm) {
    std::string id = cstr(cm.job_id, JOB_ID_LEN);
    if (cm.cmd == CTRL_SUBMIT) {
        if (draining) {
            Logger::warn("COORD", "关停中，拒绝新作业 job=" + id);
            send_reply(cm, ST_SHUTTING_DOWN, 0, "");
            return;
        }
        std::string payload = cstr(cm.payload, PAYLOAD_LEN);
        SubmitVerdict v = store.submit(id, payload);
        switch (v) {
            case SubmitVerdict::NEW:
                Logger::info("COORD", "受理作业(控制通道) job=" + id);
                send_reply(cm, ST_ACCEPTED, 0, "");
                break;
            case SubmitVerdict::ALREADY_PENDING:
            case SubmitVerdict::ALREADY_INFLIGHT:
                Logger::info("COORD", "重复提交(处理中)，不重复计入 job=" + id);
                send_reply(cm, ST_IN_PROGRESS, 0, "");
                break;
            case SubmitVerdict::ALREADY_SUCCEEDED: {
                const Job* j = store.get(id);
                std::string result = j ? j->result : "";
                Logger::info("COORD", "作业已成功，直接返回原结果 job=" + id +
                                      " result=" + result);
                send_reply(cm, ST_ALREADY_DONE, 0, result);
                break;
            }
            case SubmitVerdict::ALREADY_DEAD:
                Logger::warn("COORD", "作业在死信中，需先重放 job=" + id);
                send_reply(cm, ST_DEAD, 0, "");
                break;
        }
    } else if (cm.cmd == CTRL_REPLAY) {
        const Job* j = store.get(id);
        if (!j) {
            send_reply(cm, ST_UNKNOWN_JOB, 0, "");
        } else if (store.replay(id)) {
            Logger::info("COORD", "死信重放 job=" + id);
            send_reply(cm, ST_REPLAYED, 1, "");
        } else {
            send_reply(cm, ST_NOT_DEAD, 0, "");
        }
    } else if (cm.cmd == CTRL_REPLAY_ALL) {
        int n = 0;
        for (const auto& d : store.dead_ids())
            if (store.replay(d)) n++;
        Logger::info("COORD", "重放全部死信，共 " + std::to_string(n) + " 个");
        send_reply(cm, ST_REPLAYED, n, "");
    }
}

void Coordinator::drain_messages() {
    ResultMsg rm;
    while (msgrcv(qid, &rm, sizeof(rm) - sizeof(long), MT_RESULT, IPC_NOWAIT) > 0)
        handle_result(rm);
    ControlMsg cm;
    while (msgrcv(qid, &cm, sizeof(cm) - sizeof(long), MT_CONTROL, IPC_NOWAIT) > 0)
        handle_control(cm);
}

void Coordinator::check_leases() {
    auto now = std::chrono::steady_clock::now();
    for (const auto& id : store.inflight_ids()) {
        Job* j = store.get_mut(id);
        if (!j || now < j->deadline) continue;
        uint64_t tok = j->token;
        if (store.expire(id, "lease-timeout")) {
            Logger::warn("COORD", "租约过期，重新入队 job=" + id +
                                  " token=" + std::to_string(tok));
            const Job* jj = store.get(id);
            if (jj && (int)jj->attempts >= max_attempts) {
                store.mark_dead(id, "max-attempts");
                Logger::error("COORD", "达到重试上限，进入死信 job=" + id);
            }
        }
    }
}

void Coordinator::dispatch_pending() {
    while (true) {
        std::vector<std::string> ids = store.pending_ids();
        if (ids.empty()) break;
        const std::string& id = ids.front();
        const Job* j = store.get(id);
        if (!j) break;
        uint64_t token = ++last_token;
        uint32_t attempt = j->attempts + 1;
        std::string payload = j->payload;
        if (!store.dispatch(id, token, attempt)) break;

        TaskMsg tm;
        memset(&tm, 0, sizeof(tm));
        tm.mtype = MT_TASK;
        tm.token = token;
        tm.attempt = attempt;
        strncpy(tm.job_id, id.c_str(), JOB_ID_LEN - 1);
        strncpy(tm.payload, payload.c_str(), PAYLOAD_LEN - 1);
        if (msgsnd(qid, &tm, sizeof(tm) - sizeof(long), IPC_NOWAIT) != 0) {
            // 队列满或已删除：回到待处理，下一轮再试
            store.expire(id, "dispatch-failed");
            Logger::error("COORD", "任务下发失败 job=" + id + ": " +
                                   std::string(strerror(errno)));
            break;
        }
        Job* jm = store.get_mut(id);
        if (jm)
            jm->deadline = std::chrono::steady_clock::now() +
                           std::chrono::milliseconds(lease_ms);
        Logger::info("COORD", "任务已下发 job=" + id +
                              " attempt=" + std::to_string(attempt) +
                              " token=" + std::to_string(token));
    }
}

void Coordinator::poll_input() {
    if (input_fd < 0) return;
    struct stat st;
    if (fstat(input_fd, &st) != 0) return;
    if (st.st_size < input_off) {   // 文件被截断：从头重读
        input_off = 0;
        input_buf.clear();
    }
    if (st.st_size == input_off) return;
    char chunk[8192];
    ssize_t n = pread(input_fd, chunk, sizeof(chunk), input_off);
    if (n <= 0) return;
    input_off += n;
    input_buf.append(chunk, (size_t)n);
    size_t pos;
    while ((pos = input_buf.find('\n')) != std::string::npos) {
        std::string line = input_buf.substr(0, pos);
        input_buf.erase(0, pos + 1);
        handle_input_line(line);
    }
}

void Coordinator::handle_input_line(const std::string& raw) {
    std::string line = raw;
    while (!line.empty() &&
           (line.back() == '\r' || line.back() == ' ' || line.back() == '\t'))
        line.pop_back();
    if (line.empty() || line[0] == '#') return;

    std::string id, payload;
    size_t sp = line.find_first_of(" \t");
    if (sp == std::string::npos) {
        id = line;
    } else {
        id = line.substr(0, sp);
        payload = line.substr(sp + 1);
        size_t p0 = payload.find_first_not_of(" \t");
        payload = p0 == std::string::npos ? "" : payload.substr(p0);
    }
    if (!valid_job_id(id)) {
        Logger::warn("COORD", "非法 job_id，已忽略: " + id);
        return;
    }
    SubmitVerdict v = store.submit(id, payload);
    switch (v) {
        case SubmitVerdict::NEW:
            Logger::info("COORD", "受理作业 job=" + id + " payload=" + payload);
            break;
        case SubmitVerdict::ALREADY_SUCCEEDED: {
            const Job* j = store.get(id);
            Logger::info("COORD", "作业已成功，直接返回原结果 job=" + id +
                                  " result=" + (j ? j->result : ""));
            break;
        }
        default: {
            const Job* j = store.get(id);
            Logger::warn("COORD", "重复提交已忽略 job=" + id + " 当前状态=" +
                                  (j ? state_name(j->state) : "?"));
            break;
        }
    }
}

int Coordinator::run() {
    if (!mkdir_p(state_dir)) {
        Logger::error("COORD", "无法创建状态目录: " + state_dir);
        return 1;
    }

    // 单实例：同一状态目录只允许一个协调器
    lock_fd = open((state_dir + "/coordinator.lock").c_str(), O_RDWR | O_CREAT, 0644);
    if (lock_fd < 0 || flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
        Logger::error("COORD", "另一个协调器正在使用状态目录，退出: " + state_dir);
        if (lock_fd >= 0) close(lock_fd);
        return 1;
    }

    // 消息队列：持有锁，因此残留队列一定来自异常退出的前任，直接清理
    std::string key_file = state_dir + "/queue.key";
    {
        int fd = open(key_file.c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd >= 0) close(fd);
    }
    key_t key = ftok(key_file.c_str(), 'J');
    qid = msgget(key, IPC_CREAT | IPC_EXCL | 0600);
    if (qid < 0 && errno == EEXIST) {
        int old = msgget(key, 0);
        if (old >= 0) {
            msgctl(old, IPC_RMID, nullptr);
            Logger::warn("COORD", "已清理异常退出残留的消息队列");
        }
        qid = msgget(key, IPC_CREAT | IPC_EXCL | 0600);
    }
    if (qid < 0) {
        Logger::error("COORD", "创建消息队列失败: " + std::string(strerror(errno)));
        close(lock_fd);
        return 1;
    }

    // 状态恢复
    if (!store.open(state_dir + "/jobs.log", true)) {
        Logger::error("COORD", "无法打开状态日志");
        msgctl(qid, IPC_RMID, nullptr);
        close(lock_fd);
        return 1;
    }
    if (store.had_corrupt_tail())
        Logger::warn("COORD", "检测到损坏的日志尾部，已忽略并截断");
    Counts rc = store.counts();
    Logger::info("COORD", "状态恢复完成: pending=" + std::to_string(rc.pending) +
                          " inflight=" + std::to_string(rc.inflight) +
                          " succeeded=" + std::to_string(rc.succeeded) +
                          " dead=" + std::to_string(rc.dead));
    size_t expired = store.expire_all_inflight("coordinator-restart", max_attempts);
    if (expired > 0)
        Logger::warn("COORD", std::to_string(expired) +
                              " 个处理中租约随重启过期，已重新入队");
    store.log_boot();
    last_token = store.max_token();

    results_fd = open((state_dir + "/results.log").c_str(),
                      O_WRONLY | O_CREAT | O_APPEND, 0644);

    if (!input_path.empty()) {
        input_fd = open(input_path.c_str(), O_RDONLY | O_CREAT, 0644);
        if (input_fd < 0)
            Logger::warn("COORD", "无法打开输入文件: " + input_path);
    }

    install_signal_handlers();
    Logger::info("COORD", "协调器已启动 dir=" + state_dir +
                          " qid=" + std::to_string(qid) +
                          " lease=" + std::to_string(lease_ms) + "ms" +
                          " max-attempts=" + std::to_string(max_attempts));

    while (true) {
        if (g_stop && !draining) {
            draining = true;
            Logger::info("COORD", "收到关停信号：停止接收新作业，等待当前租约结算...");
            if (input_fd >= 0) {
                close(input_fd);
                input_fd = -1;
            }
        }
        drain_messages();
        check_leases();
        if (!draining) {
            dispatch_pending();
            poll_input();
        } else if (store.counts().inflight == 0) {
            break;   // 所有租约已结算（确认或过期）
        }
        usleep((useconds_t)poll_ms * 1000);
    }

    // 保存一致状态并清理
    store.log_stop();
    Counts fc = store.counts();
    store.close();
    msgctl(qid, IPC_RMID, nullptr);
    if (results_fd >= 0) close(results_fd);
    close(lock_fd);   // 释放 flock
    Logger::info("COORD", "已保存状态并删除消息队列，退出: pending=" +
                          std::to_string(fc.pending) +
                          " succeeded=" + std::to_string(fc.succeeded) +
                          " dead=" + std::to_string(fc.dead));
    return 0;
}

} // namespace

int run_coordinator(int argc, char** argv) {
    Args a = Args::parse(argc, argv);
    Coordinator c;
    c.state_dir = a.get("--state-dir");
    c.input_path = a.get("--input");
    c.lease_ms = a.get_int("--lease-ms", 2000);
    c.max_attempts = a.get_int("--max-attempts", 3);
    c.poll_ms = a.get_int("--poll-ms", 50);
    if (c.state_dir.empty()) {
        Logger::error("COORD", "用法: ipc_demo coordinator --state-dir DIR "
                               "[--input FILE] [--lease-ms N] [--max-attempts N] "
                               "[--poll-ms N]");
        return 1;
    }
    return c.run();
}

// ================= Worker =================

int run_worker(int argc, char** argv) {
    Args a = Args::parse(argc, argv);
    std::string dir = a.get("--state-dir");
    std::string name = a.get("--name", "worker");
    int delay_ms = a.get_int("--delay-ms", 0);
    int crash_after = a.get_int("--crash-after", -1);
    int exit_after = a.get_int("--exit-after", -1);
    bool send_duplicates = a.has("--send-duplicates");
    int wait_ms = a.get_int("--wait-queue-ms", 15000);
    if (dir.empty()) {
        Logger::error(name, "用法: ipc_demo worker --state-dir DIR [--name N] "
                            "[--delay-ms N] [--crash-after N] [--exit-after N] "
                            "[--send-duplicates]");
        return 1;
    }

    // 等待协调器创建队列
    std::string key_file = dir + "/queue.key";
    int qid = -1;
    for (int t = 0; t < wait_ms && qid < 0; t += 100) {
        if (access(key_file.c_str(), F_OK) == 0) {
            key_t key = ftok(key_file.c_str(), 'J');
            if (key != (key_t)-1) qid = msgget(key, 0);
        }
        if (qid < 0) usleep(100000);
    }
    if (qid < 0) {
        Logger::error(name, "等待消息队列超时，协调器未运行？");
        return 1;
    }

    install_signal_handlers();
    Logger::info(name, "worker 已启动 qid=" + std::to_string(qid));

    int processed = 0;
    while (!g_stop) {
        TaskMsg tm;
        ssize_t n = msgrcv(qid, &tm, sizeof(tm) - sizeof(long), MT_TASK, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EIDRM || errno == EINVAL) {
                Logger::info(name, "消息队列已删除，退出");
                break;
            }
            Logger::error(name, "msgrcv 失败: " + std::string(strerror(errno)));
            break;
        }
        std::string id = cstr(tm.job_id, JOB_ID_LEN);
        std::string payload = cstr(tm.payload, PAYLOAD_LEN);

        if (crash_after >= 0 && processed >= crash_after) {
            // 测试钩子：收到任务后不确认直接崩溃，模拟 worker 宕机
            Logger::error(name, "模拟崩溃：收到任务但未确认 job=" + id);
            _exit(2);
        }
        Logger::info(name, "收到任务 job=" + id +
                           " attempt=" + std::to_string(tm.attempt) +
                           " token=" + std::to_string(tm.token));
        if (delay_ms > 0) usleep((useconds_t)delay_ms * 1000);

        std::string result = "result:" + payload;
        ResultMsg rm;
        memset(&rm, 0, sizeof(rm));
        rm.mtype = MT_RESULT;
        rm.token = tm.token;
        rm.status = 0;
        strncpy(rm.job_id, id.c_str(), JOB_ID_LEN - 1);
        strncpy(rm.result, result.c_str(), RESULT_LEN - 1);
        if (msgsnd(qid, &rm, sizeof(rm) - sizeof(long), 0) != 0) {
            Logger::error(name, "结果发送失败 job=" + id + ": " +
                                std::string(strerror(errno)));
            break;
        }
        if (send_duplicates) {
            // 测试钩子：重复发送确认，协调器必须识别且不重复计入
            msgsnd(qid, &rm, sizeof(rm) - sizeof(long), 0);
            Logger::warn(name, "(测试钩子) 重复发送确认 job=" + id);
        }
        processed++;
        Logger::info(name, "任务完成 job=" + id);
        if (exit_after >= 0 && processed >= exit_after) break;
    }
    Logger::info(name, "worker 退出，共处理 " + std::to_string(processed) + " 个任务");
    return 0;
}

// ================= 管理命令 =================

// 连接运行中的协调器队列；失败返回 -1
int connect_queue(const std::string& dir) {
    key_t key = queue_key_for(dir);
    if (key == (key_t)-1) return -1;
    return msgget(key, 0);
}

int run_submit_cmd(int argc, char** argv) {
    Args a = Args::parse(argc, argv);
    std::string dir = a.get("--state-dir");
    std::string id = a.get("--job-id");
    std::string payload = a.get("--payload");
    if (dir.empty() || !valid_job_id(id)) {
        std::cerr << "用法: ipc_demo submit --state-dir DIR --job-id ID "
                     "[--payload TEXT]" << std::endl;
        return 1;
    }
    int qid = connect_queue(dir);
    if (qid < 0) {
        std::cerr << "协调器未运行（无法连接消息队列）" << std::endl;
        return 2;
    }
    int rq = msgget(IPC_PRIVATE, 0600);
    if (rq < 0) {
        std::cerr << "创建回复队列失败: " << strerror(errno) << std::endl;
        return 1;
    }
    ControlMsg cm;
    memset(&cm, 0, sizeof(cm));
    cm.mtype = MT_CONTROL;
    cm.cmd = CTRL_SUBMIT;
    cm.reply_qid = rq;
    strncpy(cm.job_id, id.c_str(), JOB_ID_LEN - 1);
    strncpy(cm.payload, payload.c_str(), PAYLOAD_LEN - 1);
    if (msgsnd(qid, &cm, sizeof(cm) - sizeof(long), 0) != 0) {
        std::cerr << "提交失败: " << strerror(errno) << std::endl;
        msgctl(rq, IPC_RMID, nullptr);
        return 1;
    }
    ReplyMsg rep;
    int rc = wait_reply(rq, rep, 5000);
    msgctl(rq, IPC_RMID, nullptr);
    if (rc != 0) {
        std::cerr << "等待协调器回复超时" << std::endl;
        return 1;
    }
    switch (rep.status) {
        case ST_ACCEPTED:
            std::cout << "ACCEPTED job=" << id << std::endl;
            return 0;
        case ST_IN_PROGRESS:
            std::cout << "IN_PROGRESS job=" << id << "（已受理过，排队或处理中）"
                      << std::endl;
            return 0;
        case ST_ALREADY_DONE:
            std::cout << "ALREADY_DONE job=" << id << " result=" << rep.result
                      << std::endl;
            return 0;
        case ST_SHUTTING_DOWN:
            std::cout << "SHUTTING_DOWN job=" << id << "（协调器关停中，未受理）"
                      << std::endl;
            return 3;
        case ST_DEAD:
            std::cout << "DEAD job=" << id << "（在死信中，请先 replay）" << std::endl;
            return 4;
        default:
            std::cout << "ERROR status=" << rep.status << std::endl;
            return 1;
    }
}

int run_status_cmd(int argc, char** argv) {
    Args a = Args::parse(argc, argv);
    std::string dir = a.get("--state-dir");
    if (dir.empty()) {
        std::cerr << "用法: ipc_demo status --state-dir DIR" << std::endl;
        return 1;
    }
    if (access(dir.c_str(), F_OK) != 0) {
        std::cerr << "状态目录不存在: " << dir << std::endl;
        return 1;
    }
    JobStore store;
    if (!store.open(dir + "/jobs.log", false)) {
        std::cerr << "无法读取状态日志" << std::endl;
        return 1;
    }
    Counts c = store.counts();
    std::cout << "pending=" << c.pending
              << " inflight=" << c.inflight
              << " succeeded=" << c.succeeded
              << " dead=" << c.dead << std::endl;
    std::cout << "dispatches=" << c.dispatches
              << " dup_submits=" << c.dup_submits
              << " late_acks=" << c.late_acks
              << " dup_acks=" << c.dup_acks << std::endl;
    return 0;
}

int run_history_cmd(int argc, char** argv) {
    Args a = Args::parse(argc, argv);
    std::string dir = a.get("--state-dir");
    std::string id = a.get("--job-id");
    if (dir.empty() || id.empty()) {
        std::cerr << "用法: ipc_demo history --state-dir DIR --job-id ID" << std::endl;
        return 1;
    }
    std::string log_path = dir + "/jobs.log";
    scan_job_log(log_path, [&](const LogRecord& r) {
        if (r.job_id != id) return;
        std::cout << format_ts(r.ts_ms) << " " << r.type;
        for (const auto& f : r.fields) std::cout << " " << f;
        std::cout << std::endl;
    });
    JobStore store;
    store.open(log_path, false);
    const Job* j = store.get(id);
    if (!j) {
        std::cout << "job " << id << " 不存在" << std::endl;
        return 1;
    }
    std::cout << "final_state=" << state_name(j->state)
              << " attempts=" << j->attempts;
    if (j->state == JobState::SUCCEEDED) std::cout << " result=" << j->result;
    std::cout << std::endl;
    return 0;
}

int run_replay_cmd(int argc, char** argv) {
    Args a = Args::parse(argc, argv);
    std::string dir = a.get("--state-dir");
    std::string id = a.get("--job-id");
    bool all = a.has("--all");
    if (dir.empty() || (id.empty() && !all)) {
        std::cerr << "用法: ipc_demo replay --state-dir DIR (--job-id ID | --all)"
                  << std::endl;
        return 1;
    }

    // 协调器未运行时直接改日志（离线重放）；运行中则走控制消息（在线重放）
    int lock_fd = open((dir + "/coordinator.lock").c_str(), O_RDWR | O_CREAT, 0644);
    if (lock_fd >= 0 && flock(lock_fd, LOCK_EX | LOCK_NB) == 0) {
        JobStore store;
        if (!store.open(dir + "/jobs.log", true)) {
            std::cerr << "无法打开状态日志" << std::endl;
            close(lock_fd);
            return 1;
        }
        int rc = 0;
        if (all) {
            int n = 0;
            for (const auto& d : store.dead_ids()) {
                if (store.replay(d)) {
                    std::cout << "REPLAYED job=" << d << "（离线）" << std::endl;
                    n++;
                }
            }
            std::cout << "共重放 " << n << " 个死信" << std::endl;
        } else {
            const Job* j = store.get(id);
            if (!j) {
                std::cout << "UNKNOWN_JOB job=" << id << std::endl;
                rc = 1;
            } else if (store.replay(id)) {
                std::cout << "REPLAYED job=" << id << "（离线）" << std::endl;
            } else {
                std::cout << "NOT_DEAD job=" << id
                          << " state=" << state_name(j->state) << std::endl;
                rc = 1;
            }
        }
        store.close();
        close(lock_fd);
        return rc;
    }
    if (lock_fd >= 0) close(lock_fd);

    // 在线重放
    int qid = connect_queue(dir);
    if (qid < 0) {
        std::cerr << "状态目录被锁定但无法连接消息队列" << std::endl;
        return 2;
    }
    int rq = msgget(IPC_PRIVATE, 0600);
    if (rq < 0) {
        std::cerr << "创建回复队列失败: " << strerror(errno) << std::endl;
        return 1;
    }
    ControlMsg cm;
    memset(&cm, 0, sizeof(cm));
    cm.mtype = MT_CONTROL;
    cm.cmd = all ? CTRL_REPLAY_ALL : CTRL_REPLAY;
    cm.reply_qid = rq;
    if (!id.empty()) strncpy(cm.job_id, id.c_str(), JOB_ID_LEN - 1);
    if (msgsnd(qid, &cm, sizeof(cm) - sizeof(long), 0) != 0) {
        std::cerr << "发送重放命令失败: " << strerror(errno) << std::endl;
        msgctl(rq, IPC_RMID, nullptr);
        return 1;
    }
    ReplyMsg rep;
    int rc = wait_reply(rq, rep, 5000);
    msgctl(rq, IPC_RMID, nullptr);
    if (rc != 0) {
        std::cerr << "等待协调器回复超时" << std::endl;
        return 1;
    }
    switch (rep.status) {
        case ST_REPLAYED:
            std::cout << "REPLAYED"
                      << (id.empty() ? "" : " job=" + id)
                      << " count=" << rep.extra << std::endl;
            return 0;
        case ST_NOT_DEAD:
            std::cout << "NOT_DEAD job=" << id << std::endl;
            return 1;
        case ST_UNKNOWN_JOB:
            std::cout << "UNKNOWN_JOB job=" << id << std::endl;
            return 1;
        default:
            std::cout << "ERROR status=" << rep.status << std::endl;
            return 1;
    }
}

} // namespace jobq
} // namespace ipc
