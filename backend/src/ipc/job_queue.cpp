/**
 * 离线任务队列：状态存储与日志恢复实现
 *
 * 日志格式：每条记录一帧
 *   [uint32 magic][uint32 len][uint32 crc32][payload]
 * payload 为文本：TYPE|ts_ms|job_id|field1|field2...
 *
 * 记录类型：
 *   BOOT / STOP                      协调器启动 / 干净关停标记
 *   SUBMIT|ts|id|payload             受理新作业
 *   DUP_SUBMIT|ts|id                 识别到重复提交
 *   DISPATCH|ts|id|attempt|token     派发（携带租约 token）
 *   SUCCESS|ts|id|token|result       完成确认被接受
 *   FAIL|ts|id|token|error           worker 报告失败，重新入队
 *   EXPIRE|ts|id|token|reason        租约过期，重新入队
 *   DEAD|ts|id|reason                进入死信
 *   REPLAY|ts|id                     死信重放，重新入队
 *   LATE_ACK|ts|id|token|result      迟到确认，已忽略
 *   DUP_ACK|ts|id|token|result       重复结果，已忽略
 */

#include "job_queue.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>
#include <ctime>

namespace ipc {
namespace jobq {

namespace {

constexpr uint32_t LOG_MAGIC = 0x4A514C31;   // "JQL1"
constexpr uint32_t MAX_RECORD = 1 << 20;     // 单条记录上限 1MB

uint64_t now_ms() {
    return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    size_t pos = 0;
    while (true) {
        size_t p = s.find(sep, pos);
        if (p == std::string::npos) {
            out.push_back(s.substr(pos));
            break;
        }
        out.push_back(s.substr(pos, p - pos));
        pos = p + 1;
    }
    return out;
}

} // namespace

// ================= 工具 =================

uint32_t crc32(const void* data, size_t len) {
    static uint32_t table[256];
    static bool initialized = false;
    if (!initialized) {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++)
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            table[i] = c;
        }
        initialized = true;
    }
    uint32_t c = 0xFFFFFFFFu;
    const uint8_t* p = (const uint8_t*)data;
    for (size_t i = 0; i < len; i++)
        c = table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

bool valid_job_id(const std::string& id) {
    if (id.empty() || id.size() >= JOB_ID_LEN) return false;
    for (char c : id) {
        bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                  (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
        if (!ok) return false;
    }
    return true;
}

std::string sanitize_field(const std::string& s) {
    std::string out = s;
    for (auto& c : out) {
        if (c == '|') c = '/';
        else if (c == '\n' || c == '\r') c = ' ';
        else if ((unsigned char)c < 0x20) c = '?';
    }
    return out;
}

std::string format_ts(uint64_t ts_ms) {
    time_t secs = (time_t)(ts_ms / 1000);
    int ms = (int)(ts_ms % 1000);
    struct tm tmv;
    localtime_r(&secs, &tmv);
    char buf[64];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03d",
             tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
             tmv.tm_hour, tmv.tm_min, tmv.tm_sec, ms);
    return buf;
}

key_t queue_key_for(const std::string& state_dir) {
    std::string key_file = state_dir + "/queue.key";
    if (access(key_file.c_str(), F_OK) != 0) return (key_t)-1;
    return ftok(key_file.c_str(), 'J');
}

const char* state_name(JobState s) {
    switch (s) {
        case JobState::PENDING: return "PENDING";
        case JobState::INFLIGHT: return "INFLIGHT";
        case JobState::SUCCEEDED: return "SUCCEEDED";
        case JobState::DEAD: return "DEAD";
    }
    return "?";
}

// ================= 日志扫描 =================

bool scan_job_log(const std::string& path,
                  const std::function<void(const LogRecord&)>& cb,
                  uint64_t* corrupt_offset) {
    if (corrupt_offset) *corrupt_offset = 0;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return errno == ENOENT;   // 没有日志 = 空状态

    uint64_t offset = 0;
    bool clean = true;
    for (;;) {
        char hdr[12];
        ssize_t n = pread(fd, hdr, 12, (off_t)offset);
        if (n == 0) break;                          // 正常结束
        if (n != 12) { clean = false; break; }      // 半条帧头
        uint32_t magic, len, crc;
        memcpy(&magic, hdr, 4);
        memcpy(&len, hdr + 4, 4);
        memcpy(&crc, hdr + 8, 4);
        if (magic != LOG_MAGIC || len == 0 || len > MAX_RECORD) {
            clean = false;
            break;
        }
        std::string payload(len, '\0');
        n = pread(fd, &payload[0], len, (off_t)(offset + 12));
        if (n != (ssize_t)len) { clean = false; break; }   // 半条记录体
        if (crc32(payload.data(), len) != crc) { clean = false; break; }

        std::vector<std::string> parts = split(payload, '|');
        if (parts.size() >= 3) {
            LogRecord rec;
            rec.type = parts[0];
            try {
                rec.ts_ms = std::stoull(parts[1]);
            } catch (...) {
                rec.ts_ms = 0;
            }
            rec.job_id = parts[2];
            rec.fields.assign(parts.begin() + 3, parts.end());
            cb(rec);
        }
        offset += 12 + len;
    }
    close(fd);
    if (!clean && corrupt_offset) *corrupt_offset = offset;
    return clean;
}

// ================= JobStore =================

bool JobStore::open(const std::string& log_path, bool writable) {
    writable_ = writable;
    uint64_t corrupt = 0;
    bool clean = scan_job_log(log_path, [this](const LogRecord& r) { apply(r); },
                              &corrupt);
    had_corrupt_tail_ = !clean;
    if (writable_) {
        fd_ = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) return false;
        if (!clean) {
            // 忽略损坏尾部：截断到最后一条完整记录之后继续追加
            ftruncate(fd_, (off_t)corrupt);
        }
    }
    return true;
}

void JobStore::close() {
    if (fd_ >= 0) {
        fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

void JobStore::apply(const LogRecord& r) {
    const std::string& t = r.type;
    try {
        if (t == "SUBMIT") {
            if (jobs_.count(r.job_id)) return;
            Job j;
            j.id = r.job_id;
            j.payload = r.fields.empty() ? "" : r.fields[0];
            jobs_[r.job_id] = j;
            pending_.push_back(r.job_id);
        } else if (t == "DUP_SUBMIT") {
            st_dup_submits_++;
        } else if (t == "DISPATCH") {
            auto it = jobs_.find(r.job_id);
            if (it == jobs_.end() || r.fields.size() < 2) return;
            it->second.state = JobState::INFLIGHT;
            it->second.attempts = (uint32_t)std::stoul(r.fields[0]);
            it->second.token = std::stoull(r.fields[1]);
            for (auto p = pending_.begin(); p != pending_.end(); ++p) {
                if (*p == r.job_id) {
                    pending_.erase(p);
                    break;
                }
            }
            if (it->second.token > max_token_) max_token_ = it->second.token;
            st_dispatches_++;
        } else if (t == "SUCCESS") {
            auto it = jobs_.find(r.job_id);
            if (it == jobs_.end() || r.fields.size() < 2) return;
            it->second.state = JobState::SUCCEEDED;
            it->second.token = 0;
            it->second.result = r.fields[1];
        } else if (t == "FAIL" || t == "EXPIRE") {
            auto it = jobs_.find(r.job_id);
            if (it == jobs_.end()) return;
            it->second.state = JobState::PENDING;
            it->second.token = 0;
            pending_.push_back(r.job_id);
        } else if (t == "DEAD") {
            auto it = jobs_.find(r.job_id);
            if (it == jobs_.end()) return;
            it->second.state = JobState::DEAD;
            it->second.token = 0;
        } else if (t == "REPLAY") {
            auto it = jobs_.find(r.job_id);
            if (it == jobs_.end()) return;
            it->second.state = JobState::PENDING;
            it->second.attempts = 0;   // 重放后重新计算尝试预算
            it->second.token = 0;
            pending_.push_back(r.job_id);
        } else if (t == "LATE_ACK") {
            st_late_acks_++;
        } else if (t == "DUP_ACK") {
            st_dup_acks_++;
        }
        // BOOT / STOP：仅时间标记，无状态迁移
    } catch (...) {
        // 字段解析失败：跳过该记录
    }
}

bool JobStore::append_and_apply(const std::string& type, const std::string& job_id,
                                const std::vector<std::string>& fields) {
    if (!writable_ || fd_ < 0) return false;

    LogRecord rec;
    rec.type = type;
    rec.ts_ms = now_ms();
    rec.job_id = job_id.empty() ? "-" : job_id;

    std::string line = type + "|" + std::to_string(rec.ts_ms) + "|" + rec.job_id;
    for (const auto& f : fields) {
        std::string sf = sanitize_field(f);
        line += "|" + sf;
        rec.fields.push_back(sf);
    }

    uint32_t magic = LOG_MAGIC;
    uint32_t len = (uint32_t)line.size();
    uint32_t crc = crc32(line.data(), line.size());
    char hdr[12];
    memcpy(hdr, &magic, 4);
    memcpy(hdr + 4, &len, 4);
    memcpy(hdr + 8, &crc, 4);

    // 单次 write：记录要么完整落盘，要么成为恢复时可检测的损坏尾部
    std::string frame;
    frame.reserve(12 + len);
    frame.append(hdr, 12);
    frame.append(line);
    ssize_t n = write(fd_, frame.data(), frame.size());
    if (n != (ssize_t)frame.size()) return false;
    fsync(fd_);

    apply(rec);
    return true;
}

SubmitVerdict JobStore::submit(const std::string& job_id, const std::string& payload) {
    auto it = jobs_.find(job_id);
    if (it != jobs_.end()) {
        append_and_apply("DUP_SUBMIT", job_id, {});
        switch (it->second.state) {
            case JobState::PENDING: return SubmitVerdict::ALREADY_PENDING;
            case JobState::INFLIGHT: return SubmitVerdict::ALREADY_INFLIGHT;
            case JobState::SUCCEEDED: return SubmitVerdict::ALREADY_SUCCEEDED;
            case JobState::DEAD: return SubmitVerdict::ALREADY_DEAD;
        }
    }
    append_and_apply("SUBMIT", job_id, {payload});
    return SubmitVerdict::NEW;
}

bool JobStore::dispatch(const std::string& job_id, uint64_t token, uint32_t attempt) {
    auto it = jobs_.find(job_id);
    if (it == jobs_.end() || it->second.state != JobState::PENDING) return false;
    return append_and_apply("DISPATCH", job_id,
                            {std::to_string(attempt), std::to_string(token)});
}

ResultVerdict JobStore::on_result(const std::string& job_id, uint64_t token,
                                  int32_t status, const std::string& result) {
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) return ResultVerdict::UNKNOWN_JOB;
    Job& j = it->second;

    if (j.state == JobState::SUCCEEDED) {
        // 重复结果：识别并忽略，不重复计入
        append_and_apply("DUP_ACK", job_id, {std::to_string(token), result});
        return ResultVerdict::DUP_ACK;
    }
    if (j.state != JobState::INFLIGHT || j.token != token) {
        // 迟到确认：租约已过期（或属于旧租约），识别并忽略
        append_and_apply("LATE_ACK", job_id, {std::to_string(token), result});
        return ResultVerdict::LATE_ACK;
    }
    if (status == 0) {
        append_and_apply("SUCCESS", job_id, {std::to_string(token), result});
        return ResultVerdict::ACCEPTED_SUCCESS;
    }
    append_and_apply("FAIL", job_id, {std::to_string(token), result});
    return ResultVerdict::ACCEPTED_FAILURE;
}

bool JobStore::expire(const std::string& job_id, const std::string& reason) {
    auto it = jobs_.find(job_id);
    if (it == jobs_.end() || it->second.state != JobState::INFLIGHT) return false;
    return append_and_apply("EXPIRE", job_id,
                            {std::to_string(it->second.token), reason});
}

bool JobStore::mark_dead(const std::string& job_id, const std::string& reason) {
    auto it = jobs_.find(job_id);
    if (it == jobs_.end() || it->second.state == JobState::DEAD) return false;
    return append_and_apply("DEAD", job_id, {reason});
}

bool JobStore::replay(const std::string& job_id) {
    auto it = jobs_.find(job_id);
    if (it == jobs_.end() || it->second.state != JobState::DEAD) return false;
    return append_and_apply("REPLAY", job_id, {});
}

size_t JobStore::expire_all_inflight(const std::string& reason, int max_attempts) {
    size_t n = 0;
    for (const auto& id : inflight_ids()) {
        if (expire(id, reason)) {
            n++;
            auto it = jobs_.find(id);
            if (it != jobs_.end() && max_attempts > 0 &&
                (int)it->second.attempts >= max_attempts) {
                mark_dead(id, "max-attempts");
            }
        }
    }
    return n;
}

void JobStore::log_boot() {
    append_and_apply("BOOT", "", {"pid=" + std::to_string((long)getpid())});
}

void JobStore::log_stop() {
    append_and_apply("STOP", "", {});
}

const Job* JobStore::get(const std::string& job_id) const {
    auto it = jobs_.find(job_id);
    return it == jobs_.end() ? nullptr : &it->second;
}

Job* JobStore::get_mut(const std::string& job_id) {
    auto it = jobs_.find(job_id);
    return it == jobs_.end() ? nullptr : &it->second;
}

Counts JobStore::counts() const {
    Counts c;
    for (const auto& kv : jobs_) {
        switch (kv.second.state) {
            case JobState::PENDING: c.pending++; break;
            case JobState::INFLIGHT: c.inflight++; break;
            case JobState::SUCCEEDED: c.succeeded++; break;
            case JobState::DEAD: c.dead++; break;
        }
    }
    c.dup_submits = st_dup_submits_;
    c.late_acks = st_late_acks_;
    c.dup_acks = st_dup_acks_;
    c.dispatches = st_dispatches_;
    return c;
}

std::vector<std::string> JobStore::pending_ids() const {
    return std::vector<std::string>(pending_.begin(), pending_.end());
}

std::vector<std::string> JobStore::inflight_ids() const {
    std::vector<std::string> out;
    for (const auto& kv : jobs_)
        if (kv.second.state == JobState::INFLIGHT) out.push_back(kv.first);
    return out;
}

std::vector<std::string> JobStore::dead_ids() const {
    std::vector<std::string> out;
    for (const auto& kv : jobs_)
        if (kv.second.state == JobState::DEAD) out.push_back(kv.first);
    return out;
}

} // namespace jobq
} // namespace ipc
