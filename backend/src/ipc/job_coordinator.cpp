/**
 * 离线分析任务协调器实现
 *
 * 可靠性设计：
 * - 预写日志（WAL）：任何状态变更先落盘（write + fdatasync）再生效/再发消息
 * - 日志记录格式："#<crc32> <ts_ms> <TYPE> <字段...>\n"，crc 覆盖 "ts TYPE 字段"
 * - 恢复时逐条校验，遇到半条/损坏记录即视为损坏尾部，忽略并截断后继续
 * - 至少一次投递 + 协调器按 (job_id, attempt) 去重确认，保证不丢单、不重复计入
 */

#include "job_coordinator.h"
#include "ipc_demo.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <sstream>

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/file.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace ipc {
namespace coord {

// ---------------------------------------------------------------------------
// 基础工具
// ---------------------------------------------------------------------------

static uint64_t wall_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static uint64_t mono_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static uint32_t crc32_of(const std::string& s) {
    static uint32_t table[256];
    static bool init = false;
    if (!init) {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            table[i] = c;
        }
        init = true;
    }
    uint32_t c = 0xFFFFFFFFu;
    for (unsigned char ch : s) c = table[(c ^ ch) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

static std::string trim(const std::string& s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return "";
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

// 日志行："#<crc32(hex)> <ts_ms> <body>\n"，crc 覆盖 "<ts_ms> <body>"
static std::string format_record(const std::string& body) {
    std::string content = std::to_string(wall_ms()) + " " + body;
    char crc[16];
    snprintf(crc, sizeof(crc), "#%08x ", crc32_of(content));
    return std::string(crc) + content + "\n";
}

// 解析并校验一条日志记录；成功返回 true 并给出 ts 与 body
static bool parse_record(const std::string& line, uint64_t* ts, std::string* body) {
    if (line.size() < 12 || line[0] != '#') return false;
    size_t sp = line.find(' ');
    if (sp != 9) return false;  // "#%08x" 固定 9 字符
    char* end = nullptr;
    unsigned long crc = strtoul(line.c_str() + 1, &end, 16);
    if (end != line.c_str() + 9) return false;
    std::string content = line.substr(10);
    if (crc32_of(content) != (uint32_t)crc) return false;
    size_t sp2 = content.find(' ');
    if (sp2 == std::string::npos) return false;
    *ts = strtoull(content.c_str(), &end, 10);
    if (end != content.c_str() + sp2) return false;
    *body = content.substr(sp2 + 1);
    return true;
}

static std::string fmt_time(uint64_t ms) {
    time_t sec = (time_t)(ms / 1000);
    struct tm tmv;
    localtime_r(&sec, &tmv);
    char buf[40];
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", tmv.tm_hour, tmv.tm_min, tmv.tm_sec,
             (int)(ms % 1000));
    return buf;
}

const char* state_name(JobState s) {
    switch (s) {
        case JobState::PENDING: return "PENDING";
        case JobState::IN_FLIGHT: return "IN_FLIGHT";
        case JobState::SUCCEEDED: return "SUCCEEDED";
        case JobState::DEAD: return "DEAD";
    }
    return "?";
}

std::string compute_result(const std::string& payload) {
    std::string up = payload;
    for (auto& ch : up) ch = (char)toupper((unsigned char)ch);
    return up + ":" + std::to_string(payload.size());
}

// ---------------------------------------------------------------------------
// 日志恢复（协调器启动与离线查询共用）
// ---------------------------------------------------------------------------

namespace {

struct RecoveredState {
    StatusInfo status;  // jobs 按提交顺序
    std::map<uint64_t, std::vector<std::string>> history;  // job_id → "ts\tbody" 列表
    bool truncated_tail = false;  // 是否忽略并截断了损坏尾部
    long valid_end = 0;           // 有效日志的结束偏移
};

// 将一条已校验的记录应用到恢复状态
void apply_record(const std::string& body, uint64_t ts, RecoveredState* rs) {
    std::istringstream iss(body);
    std::string type;
    iss >> type;

    auto find_job = [&](uint64_t id) -> JobInfo* {
        for (auto& j : rs->status.jobs)
            if (j.job_id == id) return &j;
        return nullptr;
    };

    if (type == "SHUTDOWN") return;  // 仅标记干净关停，无状态语义

    uint64_t id = 0;
    if (!(iss >> id)) return;  // 其余记录均带 job_id

    // 记录历史（供 history 命令展示）
    rs->history[id].push_back(std::to_string(ts) + "\t" + body);

    if (type == "SUBMIT") {
        std::string payload;
        std::getline(iss, payload);
        payload = trim(payload);
        if (find_job(id)) return;  // 重复 SUBMIT 不应出现在合法日志中，防御性忽略
        JobInfo j;
        j.job_id = id;
        j.state = JobState::PENDING;
        j.payload = payload;
        rs->status.jobs.push_back(j);
    } else if (type == "DISPATCH") {
        uint32_t attempt = 0;
        iss >> attempt;
        JobInfo* j = find_job(id);
        if (j && j->state != JobState::SUCCEEDED && j->state != JobState::DEAD) {
            j->state = JobState::IN_FLIGHT;
            j->attempts = attempt;
        }
    } else if (type == "ACK") {
        uint32_t attempt = 0;
        iss >> attempt;
        std::string result;
        std::getline(iss, result);
        result = trim(result);
        JobInfo* j = find_job(id);
        if (j && j->state == JobState::IN_FLIGHT) {
            j->state = JobState::SUCCEEDED;
            j->result = result;
        }
    } else if (type == "NACK" || type == "EXPIRE") {
        JobInfo* j = find_job(id);
        if (j && j->state == JobState::IN_FLIGHT) j->state = JobState::PENDING;
    } else if (type == "DEAD") {
        JobInfo* j = find_job(id);
        if (j && j->state != JobState::SUCCEEDED) j->state = JobState::DEAD;
    } else if (type == "REPLAY") {
        JobInfo* j = find_job(id);
        if (j && j->state == JobState::DEAD) {
            j->state = JobState::PENDING;
            j->attempts = 0;
        }
    }
    // DUP_SUBMIT / DUP_ACK / LATE_ACK / STRAY_ACK：审计记录，不改变状态机
}

// 已知记录类型（用于识别损坏数据）
bool known_type(const std::string& body) {
    static const char* types[] = {"SUBMIT", "DISPATCH", "ACK",      "NACK",   "EXPIRE",
                                  "DEAD",   "REPLAY",   "SHUTDOWN", "DUP_SUBMIT", "DUP_ACK",
                                  "LATE_ACK", "STRAY_ACK"};
    std::istringstream iss(body);
    std::string t;
    iss >> t;
    for (const char* k : types)
        if (t == k) return true;
    return false;
}

bool recover_log(const std::string& log_path, RecoveredState* out, std::string* err) {
    *out = RecoveredState();
    int fd = open(log_path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) return true;  // 无日志 = 空状态
        *err = "open log failed: " + std::string(strerror(errno));
        return false;
    }
    std::string data;
    char buf[8192];
    ssize_t n;
    while ((n = read(fd, buf, sizeof(buf))) > 0) data.append(buf, (size_t)n);
    close(fd);

    size_t pos = 0;
    while (pos < data.size()) {
        size_t nl = data.find('\n', pos);
        if (nl == std::string::npos) {
            out->truncated_tail = true;  // 半条记录（崩溃撕裂的写）
            break;
        }
        std::string line = data.substr(pos, nl - pos);
        uint64_t ts = 0;
        std::string body;
        if (!parse_record(line, &ts, &body) || !known_type(body)) {
            out->truncated_tail = true;  // 校验失败：损坏尾部
            break;
        }
        apply_record(body, ts, out);
        pos = nl + 1;
        out->valid_end = (long)pos;
    }

    // 汇总计数
    for (const auto& j : out->status.jobs) {
        switch (j.state) {
            case JobState::PENDING: out->status.pending++; break;
            case JobState::IN_FLIGHT: out->status.in_flight++; break;
            case JobState::SUCCEEDED: out->status.succeeded++; break;
            case JobState::DEAD: out->status.dead++; break;
        }
    }
    return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

Coordinator::Coordinator(const CoordinatorConfig& cfg) : cfg_(cfg) {
    if (cfg_.state_dir.empty()) {
        err_ = "state dir is empty";
        return;
    }
    if (cfg_.max_attempts < 1) cfg_.max_attempts = 1;
    if (cfg_.max_in_flight < 1) cfg_.max_in_flight = 1;
    if (cfg_.lease_ms < 50) cfg_.lease_ms = 50;

    mkdir(cfg_.state_dir.c_str(), 0755);  // 已存在则忽略

    // 1) 单实例锁：同一状态目录只允许一个协调器
    std::string lock_path = cfg_.state_dir + "/coordinator.lock";
    lock_fd_ = open(lock_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (lock_fd_ < 0) {
        err_ = "cannot open lock file: " + std::string(strerror(errno));
        return;
    }
    if (flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
        err_ = "another coordinator is already running for state dir " + cfg_.state_dir;
        return;
    }

    // 2) 打开日志并恢复状态
    log_path_ = cfg_.state_dir + "/coordinator.log";
    log_fd_ = open(log_path_.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
    if (log_fd_ < 0) {
        err_ = "cannot open log: " + std::string(strerror(errno));
        return;
    }
    RecoveredState rs;
    std::string rerr;
    if (!recover_log(log_path_, &rs, &rerr)) {
        err_ = rerr;
        return;
    }
    if (rs.truncated_tail) {
        // 忽略损坏尾部：截断到最后的有效记录，后续追加从干净位置继续
        ftruncate(log_fd_, rs.valid_end);
        log_line("日志尾部存在半条/损坏记录，已忽略并截断，继续恢复");
    }
    for (const auto& j : rs.status.jobs) {
        JobRec rec;
        rec.id = j.job_id;
        rec.payload = j.payload;
        rec.state = j.state;
        rec.attempts = j.attempts;
        rec.result = j.result;
        jobs_[rec.id] = rec;
        submit_order_.push_back(rec.id);
        if (rec.state == JobState::PENDING) pending_.push_back(rec.id);
    }
    // 恢复时仍处于在途的作业：租约作废，重新排队（不丢单）
    for (auto& kv : jobs_) {
        JobRec& j = kv.second;
        if (j.state == JobState::IN_FLIGHT) {
            log_append("EXPIRE " + std::to_string(j.id) + " " + std::to_string(j.attempts) +
                       " coordinator-restart");
            j.state = JobState::PENDING;
            pending_.push_back(j.id);
            log_line("作业 " + std::to_string(j.id) + " 恢复时处于在途状态，已重新排队");
        }
    }

    // 3) 消息队列：若上次崩溃遗留旧队列则清理重建
    std::string key_path = cfg_.state_dir + "/queue.key";
    int kf = open(key_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (kf >= 0) close(kf);
    key_t key = ftok(key_path.c_str(), 'J');
    if (key == -1) {
        err_ = "ftok failed: " + std::string(strerror(errno));
        return;
    }
    msqid_ = msgget(key, IPC_CREAT | IPC_EXCL | 0666);
    if (msqid_ < 0 && errno == EEXIST) {
        int old = msgget(key, 0666);
        if (old >= 0) msgctl(old, IPC_RMID, nullptr);  // 清理上次崩溃遗留的队列
        msqid_ = msgget(key, IPC_CREAT | IPC_EXCL | 0666);
    }
    if (msqid_ < 0) {
        err_ = "msgget failed: " + std::string(strerror(errno));
        return;
    }

    // 4) 受理输入文件中的作业（重复 job_id 自动去重）
    if (!cfg_.input_file.empty()) load_input(cfg_.input_file);

    log_line("协调器已启动: state_dir=" + cfg_.state_dir + " msqid=" + std::to_string(msqid_) +
             " 待处理=" + std::to_string(pending_.size()));
    ok_ = true;
}

Coordinator::~Coordinator() {
    // 注意：析构不删除消息队列 —— 正常退出路径是 shutdown()；
    // 直接析构相当于"崩溃"，遗留队列由下一次启动清理重建。
    if (log_fd_ >= 0) close(log_fd_);
    if (lock_fd_ >= 0) close(lock_fd_);  // 关闭即释放 flock
}

void Coordinator::log_line(const std::string& msg) const {
    if (!cfg_.quiet) Logger::info("COORD", msg);
}

bool Coordinator::log_append(const std::string& body) {
    std::string line = format_record(body);
    ssize_t w = write(log_fd_, line.data(), line.size());
    if (w != (ssize_t)line.size()) {
        Logger::error("COORD", "日志写入失败: " + std::string(strerror(errno)));
        return false;
    }
    fdatasync(log_fd_);
    return true;
}

void Coordinator::load_input(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        Logger::warn("COORD", "输入文件不存在或不可读: " + path);
        return;
    }
    std::string line;
    int accepted = 0, dup = 0;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#') continue;
        std::istringstream iss(line);
        uint64_t id = 0;
        if (!(iss >> id)) {
            Logger::warn("COORD", "跳过非法行: " + line);
            continue;
        }
        std::string payload;
        std::getline(iss, payload);
        payload = trim(payload);
        int rc = submit_job(id, payload, nullptr);
        if (rc == 0)
            accepted++;
        else
            dup++;
    }
    log_line("输入文件受理完成: 新作业=" + std::to_string(accepted) +
             " 重复/拒绝=" + std::to_string(dup));
}

int Coordinator::submit_job(uint64_t job_id, const std::string& payload, std::string* out_result) {
    if (stop_requested_ || shutdown_done_) return -1;  // 关停期间不再受理
    if (job_id == 0) return -2;
    std::string p = payload.substr(0, proto::MAX_PAYLOAD);

    auto it = jobs_.find(job_id);
    if (it != jobs_.end()) {
        log_append("DUP_SUBMIT " + std::to_string(job_id));
        switch (it->second.state) {
            case JobState::SUCCEEDED:
                // 已成功的作业再次提交：直接返回原结果
                if (out_result) *out_result = it->second.result;
                log_line("作业 " + std::to_string(job_id) + " 重复提交，直接返回原结果: " +
                         it->second.result);
                return 1;
            case JobState::DEAD:
                return 3;
            default:
                return 2;
        }
    }

    if (!log_append("SUBMIT " + std::to_string(job_id) + " " + p)) return -3;
    JobRec rec;
    rec.id = job_id;
    rec.payload = p;
    jobs_[job_id] = rec;
    submit_order_.push_back(job_id);
    pending_.push_back(job_id);
    log_line("受理作业 " + std::to_string(job_id) + " payload=" + p);
    return 0;
}

size_t Coordinator::in_flight() const {
    size_t n = 0;
    for (const auto& kv : jobs_)
        if (kv.second.state == JobState::IN_FLIGHT) n++;
    return n;
}

void Coordinator::poll_once() {
    if (!ok_ || shutdown_done_) return;
    drain_messages();
    expire_leases();
    if (!stop_requested_) dispatch_pending();
}

void Coordinator::drain_messages() {
    proto::AckMsg ack;
    while (true) {
        ssize_t n = msgrcv(msqid_, &ack, sizeof(ack) - sizeof(long), proto::MTYPE_ACK, IPC_NOWAIT);
        if (n < 0) break;  // ENOMSG = 没有更多确认
        handle_ack(ack);
    }
    proto::CtrlMsg ctrl;
    while (true) {
        ssize_t n =
            msgrcv(msqid_, &ctrl, sizeof(ctrl) - sizeof(long), proto::MTYPE_CTRL, IPC_NOWAIT);
        if (n < 0) break;
        handle_ctrl(ctrl);
    }
}

void Coordinator::handle_ack(const proto::AckMsg& ack) {
    std::string result(ack.result, strnlen(ack.result, proto::MAX_RESULT));
    auto it = jobs_.find(ack.job_id);
    if (it == jobs_.end()) {
        log_append("STRAY_ACK " + std::to_string(ack.job_id) + " " +
                   std::to_string(ack.attempt));
        Logger::warn("COORD", "识别到未知作业的确认 job=" + std::to_string(ack.job_id) + "，已忽略");
        return;
    }
    JobRec& j = it->second;

    if (j.state == JobState::SUCCEEDED) {
        // 重复结果：识别并忽略，绝不重复计入
        log_append("DUP_ACK " + std::to_string(ack.job_id) + " " + std::to_string(ack.attempt));
        Logger::warn("COORD", "识别到重复结果 job=" + std::to_string(ack.job_id) +
                                  " attempt=" + std::to_string(ack.attempt) + "，已忽略");
        return;
    }
    if (j.state == JobState::DEAD) {
        log_append("LATE_ACK " + std::to_string(ack.job_id) + " " + std::to_string(ack.attempt));
        Logger::warn("COORD", "识别到死信作业的迟到确认 job=" + std::to_string(ack.job_id) + "，已忽略");
        return;
    }
    if (j.state == JobState::IN_FLIGHT && ack.attempt == j.attempts) {
        if (ack.ok) {
            log_append("ACK " + std::to_string(ack.job_id) + " " + std::to_string(ack.attempt) +
                       " " + result);
            j.state = JobState::SUCCEEDED;
            j.result = result;
            log_line("作业 " + std::to_string(ack.job_id) + " 完成: " + result);
        } else {
            log_append("NACK " + std::to_string(ack.job_id) + " " + std::to_string(ack.attempt) +
                       " " + result);
            Logger::warn("COORD", "作业 " + std::to_string(ack.job_id) + " 第 " +
                                      std::to_string(ack.attempt) + " 次尝试失败: " + result);
            requeue_or_dead(j, "worker-failure");
        }
    } else {
        // 迟到确认：对应的是已被取代的旧尝试
        log_append("LATE_ACK " + std::to_string(ack.job_id) + " " + std::to_string(ack.attempt));
        Logger::warn("COORD", "识别到迟到确认 job=" + std::to_string(ack.job_id) +
                                  " attempt=" + std::to_string(ack.attempt) +
                                  "（当前 attempt=" + std::to_string(j.attempts) + "），已忽略");
    }
}

void Coordinator::handle_ctrl(const proto::CtrlMsg& ctrl) {
    proto::CtrlReply rep;
    memset(&rep, 0, sizeof(rep));
    rep.mtype = proto::REPLY_BASE + ctrl.reply_to;
    std::string text;

    if (ctrl.cmd == proto::CTRL_SUBMIT) {
        std::string payload(ctrl.payload, strnlen(ctrl.payload, proto::MAX_PAYLOAD));
        std::string result;
        int rc = submit_job(ctrl.job_id, payload, &result);
        rep.code = rc;
        switch (rc) {
            case 0: text = "accepted job " + std::to_string(ctrl.job_id); break;
            case 1:
                text = "duplicate: job " + std::to_string(ctrl.job_id) +
                       " already succeeded, result: " + result;
                break;
            case 2:
                text = "duplicate: job " + std::to_string(ctrl.job_id) + " already pending/in-flight";
                break;
            case 3:
                text = "job " + std::to_string(ctrl.job_id) + " is in dead letter; use replay";
                break;
            default: text = "rejected: coordinator is shutting down or job_id invalid"; break;
        }
    } else if (ctrl.cmd == proto::CTRL_REPLAY) {
        int n = replay_job(ctrl.job_id);
        rep.code = (n >= 0) ? 0 : -1;
        text = (n >= 0) ? ("replayed " + std::to_string(n) + " job(s)")
                        : ("job " + std::to_string(ctrl.job_id) + " is not in dead letter");
    } else {
        rep.code = -1;
        text = "unknown command";
    }

    snprintf(rep.text, sizeof(rep.text), "%s", text.c_str());
    if (msgsnd(msqid_, &rep, sizeof(rep) - sizeof(long), IPC_NOWAIT) != 0) {
        Logger::warn("COORD", "控制应答发送失败（请求方可能已退出）");
    }
}

void Coordinator::expire_leases() {
    uint64_t now = mono_ms();
    std::vector<uint64_t> expired;
    for (auto& kv : jobs_) {
        const JobRec& j = kv.second;
        if (j.state == JobState::IN_FLIGHT && j.lease_deadline_mono <= now) expired.push_back(j.id);
    }
    for (uint64_t id : expired) {
        JobRec& j = jobs_[id];
        log_append("EXPIRE " + std::to_string(id) + " " + std::to_string(j.attempts));
        Logger::warn("COORD", "作业 " + std::to_string(id) + " 第 " + std::to_string(j.attempts) +
                                  " 次尝试租约过期，重新投递");
        requeue_or_dead(j, "lease-expired");
    }
}

void Coordinator::requeue_or_dead(JobRec& j, const std::string& reason) {
    if ((int)j.attempts >= cfg_.max_attempts) {
        log_append("DEAD " + std::to_string(j.id) + " " + reason);
        j.state = JobState::DEAD;
        Logger::warn("COORD", "作业 " + std::to_string(j.id) + " 达到重试上限（" +
                                  std::to_string(cfg_.max_attempts) + "），进入死信");
    } else {
        j.state = JobState::PENDING;
        pending_.push_back(j.id);
    }
}

void Coordinator::dispatch_pending() {
    while (!pending_.empty() && (int)in_flight() < cfg_.max_in_flight) {
        uint64_t id = pending_.front();
        auto it = jobs_.find(id);
        if (it == jobs_.end() || it->second.state != JobState::PENDING) {
            pending_.pop_front();  // 防御：队列中的陈旧项
            continue;
        }
        JobRec& j = it->second;
        uint32_t attempt = j.attempts + 1;
        uint64_t deadline = mono_ms() + (uint64_t)cfg_.lease_ms;

        // 先写日志再发消息（WAL）：崩溃后按日志恢复，租约到期自然重投
        if (!log_append("DISPATCH " + std::to_string(id) + " " + std::to_string(attempt) + " " +
                        std::to_string(wall_ms() + (uint64_t)cfg_.lease_ms)))
            return;

        proto::JobMsg msg;
        memset(&msg, 0, sizeof(msg));
        msg.mtype = proto::MTYPE_JOB;
        msg.job_id = id;
        msg.attempt = attempt;
        snprintf(msg.payload, sizeof(msg.payload), "%s", j.payload.c_str());
        if (msgsnd(msqid_, &msg, sizeof(msg) - sizeof(long), IPC_NOWAIT) != 0) {
            // 队列满等瞬时失败：保持待处理，下一轮再试（日志中的 DISPATCH
            // 在恢复时会被视为在途并重新排队，不会丢单）
            Logger::warn("COORD", "派发消息发送失败: " + std::string(strerror(errno)));
            return;
        }

        j.attempts = attempt;
        j.state = JobState::IN_FLIGHT;
        j.lease_deadline_mono = deadline;
        pending_.pop_front();
        log_line("派发作业 " + std::to_string(id) + " 第 " + std::to_string(attempt) + " 次尝试");
    }
}

int Coordinator::replay_job(uint64_t job_id) {
    int count = 0;
    for (auto& kv : jobs_) {
        JobRec& j = kv.second;
        if (j.state != JobState::DEAD) continue;
        if (job_id != 0 && j.id != job_id) continue;
        log_append("REPLAY " + std::to_string(j.id));
        j.state = JobState::PENDING;
        j.attempts = 0;  // 重放给予全新的尝试额度
        pending_.push_back(j.id);
        log_line("死信作业 " + std::to_string(j.id) + " 已重放，重新排队");
        count++;
    }
    if (job_id != 0 && count == 0) return -1;
    return count;
}

StatusInfo Coordinator::status() const {
    StatusInfo st;
    for (uint64_t id : submit_order_) {
        auto it = jobs_.find(id);
        if (it == jobs_.end()) continue;
        const JobRec& j = it->second;
        JobInfo info;
        info.job_id = j.id;
        info.state = j.state;
        info.attempts = j.attempts;
        info.payload = j.payload;
        info.result = j.result;
        st.jobs.push_back(info);
        switch (j.state) {
            case JobState::PENDING: st.pending++; break;
            case JobState::IN_FLIGHT: st.in_flight++; break;
            case JobState::SUCCEEDED: st.succeeded++; break;
            case JobState::DEAD: st.dead++; break;
        }
    }
    return st;
}

void Coordinator::write_snapshot() {
    std::string tmp = cfg_.state_dir + "/snapshot.txt.tmp";
    std::string dst = cfg_.state_dir + "/snapshot.txt";
    std::ofstream f(tmp);
    if (!f) return;
    StatusInfo st = status();
    f << "# coordinator snapshot (consistent state at shutdown)\n";
    f << "# time: " << fmt_time(wall_ms()) << "\n";
    f << "pending " << st.pending << "\n";
    f << "in_flight " << st.in_flight << "\n";
    f << "succeeded " << st.succeeded << "\n";
    f << "dead " << st.dead << "\n";
    for (const auto& j : st.jobs) {
        f << "job " << j.job_id << " " << state_name(j.state) << " attempts=" << j.attempts
          << " payload=" << j.payload;
        if (!j.result.empty()) f << " result=" << j.result;
        f << "\n";
    }
    f.flush();
    rename(tmp.c_str(), dst.c_str());  // 原子替换
}

void Coordinator::shutdown() {
    if (shutdown_done_ || !ok_) return;
    stop_requested_ = true;
    log_line("开始关停：停止受理新作业，等待在途租约结算（宽限 " +
             std::to_string(cfg_.shutdown_grace_ms) + "ms）...");

    // 等待在途租约结算：确认会被正常处理，过期的租约回到待处理而不再派发
    uint64_t deadline = mono_ms() + (uint64_t)cfg_.shutdown_grace_ms;
    while (in_flight() > 0 && mono_ms() < deadline) {
        drain_messages();
        expire_leases();
        usleep(10000);
    }
    // 宽限到期仍有在途：强制结算为待处理，保证落盘状态一致
    for (auto& kv : jobs_) {
        JobRec& j = kv.second;
        if (j.state == JobState::IN_FLIGHT) {
            log_append("EXPIRE " + std::to_string(j.id) + " " + std::to_string(j.attempts) +
                       " shutdown-grace-expired");
            requeue_or_dead(j, "shutdown");
        }
    }

    log_append("SHUTDOWN");
    write_snapshot();
    if (msqid_ >= 0) {
        msgctl(msqid_, IPC_RMID, nullptr);  // 删除自己创建的消息队列
        msqid_ = -1;
    }
    shutdown_done_ = true;
    log_line("协调器已关停：一致状态已保存（日志+快照），消息队列已删除");
}

// ---------------------------------------------------------------------------
// 运行模式
// ---------------------------------------------------------------------------

namespace {
volatile sig_atomic_t g_stop = 0;
void on_stop_signal(int) { g_stop = 1; }
}  // namespace

int run_coordinator(const CoordinatorConfig& cfg) {
    g_stop = 0;
    Coordinator c(cfg);
    if (!c.ok()) {
        Logger::error("COORD", "协调器启动失败: " + c.error());
        return 2;  // 后启动者/配置错误：失败退出
    }
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_stop_signal;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);

    while (!g_stop) {
        c.poll_once();
        usleep(10000);
    }
    c.request_stop();
    c.shutdown();
    return 0;
}

namespace {

// 连接协调器的消息队列；key 文件/队列暂未创建时在预算内重试
int connect_queue(const std::string& state_dir, long budget_ms) {
    std::string key_path = state_dir + "/queue.key";
    uint64_t deadline = mono_ms() + (uint64_t)budget_ms;
    while (true) {
        key_t key = ftok(key_path.c_str(), 'J');
        if (key != -1) {
            int q = msgget(key, 0666);
            if (q >= 0) return q;
            if (errno != ENOENT) return -1;  // 真实错误
        }
        // queue.key 或队列尚未创建（协调器启动中）：预算内重试
        if (mono_ms() >= deadline) return -1;
        usleep(100000);
    }
}

}  // namespace

int run_worker(const WorkerConfig& cfg) {
    auto log = [&](const std::string& m) {
        if (!cfg.quiet) Logger::info("WORKER", m);
    };

    int msqid = connect_queue(cfg.state_dir, cfg.reconnect_ms);
    if (msqid < 0) {
        Logger::error("WORKER", "无法连接协调器消息队列（协调器未运行？）");
        return 2;
    }
    log("worker 已启动 (pid=" + std::to_string(getpid()) + ")");

    while (true) {
        proto::JobMsg job;
        ssize_t n = msgrcv(msqid, &job, sizeof(job) - sizeof(long), proto::MTYPE_JOB, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EIDRM || errno == EINVAL) {
                // 队列被删除：协调器关停或重启，尝试重连
                int q = connect_queue(cfg.state_dir, cfg.reconnect_ms);
                if (q < 0) {
                    log("协调器队列已消失，worker 退出");
                    return 0;
                }
                msqid = q;
                log("已重连到新的协调器队列");
                continue;
            }
            Logger::error("WORKER", "msgrcv 失败: " + std::string(strerror(errno)));
            return 1;
        }

        std::string payload(job.payload, strnlen(job.payload, proto::MAX_PAYLOAD));
        if (job.job_id == cfg.crash_on_job) {
            log("收到作业 " + std::to_string(job.job_id) + "，模拟崩溃（不发送确认）");
            _exit(2);  // 模拟进程崩溃：不应答、不清理
        }

        usleep((useconds_t)cfg.work_ms * 1000);  // 模拟离线分析处理

        proto::AckMsg ack;
        memset(&ack, 0, sizeof(ack));
        ack.mtype = proto::MTYPE_ACK;
        ack.job_id = job.job_id;
        ack.attempt = job.attempt;
        if (job.job_id == cfg.fail_on_job) {
            ack.ok = 0;
            snprintf(ack.result, sizeof(ack.result), "simulated-failure");
        } else {
            ack.ok = 1;
            snprintf(ack.result, sizeof(ack.result), "%s", compute_result(payload).c_str());
        }

        // 发送确认；队列被重建（协调器重启）时重连后重发
        int sends = cfg.dup_ack ? 2 : 1;
        for (int s = 0; s < sends; s++) {
            for (int tries = 0; tries < 5; tries++) {
                if (msgsnd(msqid, &ack, sizeof(ack) - sizeof(long), 0) == 0) break;
                if (errno == EINTR) continue;
                if (errno == EIDRM || errno == EINVAL) {
                    int q = connect_queue(cfg.state_dir, cfg.reconnect_ms);
                    if (q < 0) {
                        Logger::error("WORKER", "确认无法送达（协调器已退出）");
                        return 0;
                    }
                    msqid = q;
                    continue;
                }
                Logger::error("WORKER", "确认发送失败: " + std::string(strerror(errno)));
                break;
            }
        }
        log("作业 " + std::to_string(job.job_id) + " 处理完成 (attempt=" +
            std::to_string(job.attempt) + ")");
    }
}

// ---------------------------------------------------------------------------
// CLI 命令
// ---------------------------------------------------------------------------

namespace {

// 发送控制消息并等待应答；返回应答 code，负值表示通信失败
int ctrl_roundtrip(const std::string& state_dir, proto::CtrlMsg* ctrl, std::string* out) {
    std::string key_path = state_dir + "/queue.key";
    key_t key = ftok(key_path.c_str(), 'J');
    if (key == -1) {
        *out = "state dir not found or invalid: " + state_dir;
        return -100;
    }
    int q = msgget(key, 0666);
    if (q < 0) {
        *out = (errno == ENOENT) ? "coordinator is not running (no message queue)"
                                 : std::string("msgget failed: ") + strerror(errno);
        return -100;
    }
    ctrl->mtype = proto::MTYPE_CTRL;
    ctrl->reply_to = getpid();
    if (msgsnd(q, ctrl, sizeof(*ctrl) - sizeof(long), 0) != 0) {
        *out = std::string("msgsnd failed: ") + strerror(errno);
        return -100;
    }
    uint64_t deadline = wall_ms() + 5000;
    while (wall_ms() < deadline) {
        proto::CtrlReply rep;
        ssize_t n = msgrcv(q, &rep, sizeof(rep) - sizeof(long), proto::REPLY_BASE + getpid(),
                           IPC_NOWAIT);
        if (n >= 0) {
            *out = rep.text;
            return rep.code;
        }
        if (errno == EIDRM || errno == EINVAL) {
            *out = "coordinator went away while waiting for reply";
            return -100;
        }
        usleep(20000);
    }
    *out = "timed out waiting for coordinator reply";
    return -100;
}

}  // namespace

int cmd_submit(const std::string& state_dir, uint64_t job_id, const std::string& payload,
               std::string* out) {
    proto::CtrlMsg ctrl;
    memset(&ctrl, 0, sizeof(ctrl));
    ctrl.cmd = proto::CTRL_SUBMIT;
    ctrl.job_id = job_id;
    snprintf(ctrl.payload, sizeof(ctrl.payload), "%s", payload.c_str());
    return ctrl_roundtrip(state_dir, &ctrl, out);
}

int cmd_replay(const std::string& state_dir, uint64_t job_id, std::string* out) {
    proto::CtrlMsg ctrl;
    memset(&ctrl, 0, sizeof(ctrl));
    ctrl.cmd = proto::CTRL_REPLAY;
    ctrl.job_id = job_id;
    return ctrl_roundtrip(state_dir, &ctrl, out);
}

// ---------------------------------------------------------------------------
// 离线日志查询
// ---------------------------------------------------------------------------

bool read_status(const std::string& state_dir, StatusInfo* out, std::string* err) {
    RecoveredState rs;
    if (!recover_log(state_dir + "/coordinator.log", &rs, err)) return false;
    *out = rs.status;
    return true;
}

std::string job_history(const std::string& state_dir, uint64_t job_id) {
    RecoveredState rs;
    std::string err;
    std::ostringstream oss;
    if (!recover_log(state_dir + "/coordinator.log", &rs, &err)) {
        oss << "error: " << err << "\n";
        return oss.str();
    }
    auto it = rs.history.find(job_id);
    if (it == rs.history.end()) {
        oss << "job " << job_id << ": no records\n";
        return oss.str();
    }
    oss << "job " << job_id << " history:\n";
    for (const std::string& rec : it->second) {
        size_t tab = rec.find('\t');
        uint64_t ts = strtoull(rec.c_str(), nullptr, 10);
        oss << "  " << fmt_time(ts) << "  " << rec.substr(tab + 1) << "\n";
    }
    for (const auto& j : rs.status.jobs) {
        if (j.job_id == job_id) {
            oss << "final state: " << state_name(j.state) << " (attempts=" << j.attempts << ")";
            if (!j.result.empty()) oss << " result=" << j.result;
            oss << "\n";
            break;
        }
    }
    return oss.str();
}

int cmd_status(const std::string& state_dir) {
    StatusInfo st;
    std::string err;
    if (!read_status(state_dir, &st, &err)) {
        Logger::error("STATUS", err);
        return 1;
    }
    std::cout << "state dir: " << state_dir << "\n";
    std::cout << "pending:   " << st.pending << "\n";
    std::cout << "in_flight: " << st.in_flight << "\n";
    std::cout << "succeeded: " << st.succeeded << "\n";
    std::cout << "dead:      " << st.dead << "\n";
    if (st.dead > 0) {
        std::cout << "dead jobs:";
        for (const auto& j : st.jobs)
            if (j.state == JobState::DEAD) std::cout << " " << j.job_id;
        std::cout << "\n";
    }
    return 0;
}

int cmd_history(const std::string& state_dir, uint64_t job_id) {
    std::cout << job_history(state_dir, job_id);
    return 0;
}

// ---------------------------------------------------------------------------
// 自包含演示：协调器 + worker 崩溃恢复 + 重复提交
// ---------------------------------------------------------------------------

}  // namespace coord

void demo_job_coordinator() {
    using namespace coord;
    Logger::demo("7. 任务协调器 (Job Coordinator) 演示");

    Logger::info("COORD", "演示内容：");
    Logger::info("COORD", "  - 协调器受理作业、租约管理、完成确认");
    Logger::info("COORD", "  - worker 崩溃后租约过期、作业重新投递（不丢单）");
    Logger::info("COORD", "  - 重复提交直接返回原结果（不重复完成）");
    Logger::info("COORD", "  - 优雅关停：一致状态落盘并删除消息队列\n");

    std::string dir = "/tmp/ipc_coord_demo_" + std::to_string(getpid());
    mkdir(dir.c_str(), 0755);
    std::string input = dir + "/jobs.txt";
    {
        std::ofstream f(input);
        f << "1 alpha\n2 beta\n3 gamma\n4 delta\n";
    }

    coord::CoordinatorConfig ccfg;
    ccfg.state_dir = dir;
    ccfg.input_file = input;
    ccfg.lease_ms = 1500;
    ccfg.max_attempts = 3;
    ccfg.max_in_flight = 2;

    pid_t cpid = fork();
    if (cpid == 0) _exit(coord::run_coordinator(ccfg));
    usleep(400000);

    // worker-1：处理到 job 2 时模拟崩溃
    coord::WorkerConfig w1cfg;
    w1cfg.state_dir = dir;
    w1cfg.work_ms = 150;
    w1cfg.crash_on_job = 2;
    w1cfg.reconnect_ms = 1500;
    pid_t w1 = fork();
    if (w1 == 0) _exit(coord::run_worker(w1cfg));

    int wstatus = 0;
    waitpid(w1, &wstatus, 0);
    Logger::warn("DEMO", "worker-1 已崩溃（job 2 未确认），等待租约过期后重新投递...");

    // worker-2：正常处理剩余作业
    coord::WorkerConfig w2cfg;
    w2cfg.state_dir = dir;
    w2cfg.work_ms = 150;
    w2cfg.reconnect_ms = 1500;
    pid_t w2 = fork();
    if (w2 == 0) _exit(coord::run_worker(w2cfg));

    // 等待全部成功
    bool all_done = false;
    for (int i = 0; i < 300 && !all_done; i++) {
        coord::StatusInfo st;
        std::string err;
        if (coord::read_status(dir, &st, &err) && st.succeeded == 4) all_done = true;
        usleep(50000);
    }
    std::cout << std::endl;
    if (all_done)
        Logger::info("DEMO", "全部 4 个作业成功完成（job 2 经历了崩溃重投）");
    else
        Logger::error("DEMO", "演示超时：仍有作业未完成");

    // 展示状态与 job 2 的尝试历史
    std::cout << std::endl;
    coord::cmd_status(dir);
    std::cout << std::endl << coord::job_history(dir, 2);

    // 重复提交：直接返回原结果
    std::string out;
    int rc = coord::cmd_submit(dir, 1, "alpha", &out);
    Logger::info("DEMO", "重复提交 job 1 → rc=" + std::to_string(rc) + ": " + out);

    // 优雅关停
    Logger::info("DEMO", "发送 SIGTERM，协调器开始优雅关停...");
    kill(cpid, SIGTERM);
    waitpid(cpid, nullptr, 0);

    // 验证：队列已删除、快照已保存
    key_t key = ftok((dir + "/queue.key").c_str(), 'J');
    if (key != -1 && msgget(key, 0666) == -1 && errno == ENOENT)
        Logger::info("DEMO", "消息队列已被协调器删除");
    std::ifstream snap(dir + "/snapshot.txt");
    if (snap.good()) {
        Logger::info("DEMO", "关停快照 snapshot.txt 内容：");
        std::cout << snap.rdbuf();
    }

    waitpid(w2, nullptr, 0);  // worker-2 在队列删除后自行退出

    // 清理演示目录
    std::string cmd = "rm -rf " + dir;
    if (system(cmd.c_str()) != 0) Logger::warn("DEMO", "清理演示目录失败: " + dir);

    Logger::info("COORD", "任务协调器演示完成\n");
}

}  // namespace ipc
