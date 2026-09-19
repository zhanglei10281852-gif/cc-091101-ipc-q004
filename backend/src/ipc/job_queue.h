#ifndef JOB_QUEUE_H
#define JOB_QUEUE_H

/**
 * 离线任务队列：基于 System V 消息队列的协调器 / Worker 协议与可恢复状态存储
 *
 * 在 message_queue_demo.cpp 的简单收发之上提供：
 * - 带唯一 job_id 的作业受理、租约派发、完成确认（只有收到确认才算成功）
 * - 租约过期重投递；迟到确认 / 重复结果通过租约 token 识别且不重复计入
 * - 追加式本地日志（magic + len + CRC32 帧），进程重启后重建
 *   待处理 / 处理中 / 成功 / 死信四个集合；损坏尾部被忽略并截断
 * - 重试上限进入死信，可显式重放
 */

#include <sys/types.h>
#include <sys/ipc.h>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace ipc {
namespace jobq {

// ================= 消息协议 =================

constexpr long MT_TASK = 1;     // 协调器 -> worker：任务下发
constexpr long MT_RESULT = 2;   // worker -> 协调器：完成确认
constexpr long MT_CONTROL = 3;  // 管理命令 -> 协调器（提交 / 重放）

constexpr size_t JOB_ID_LEN = 64;
constexpr size_t PAYLOAD_LEN = 256;
constexpr size_t RESULT_LEN = 256;

// 任务下发（每次派发携带新的租约 token）
struct TaskMsg {
    long mtype;                 // MT_TASK
    char job_id[JOB_ID_LEN];
    uint64_t token;             // 租约令牌，用于识别迟到确认
    uint32_t attempt;           // 第几次尝试（从 1 开始）
    char payload[PAYLOAD_LEN];
};

// 完成确认 / 失败上报
struct ResultMsg {
    long mtype;                 // MT_RESULT
    char job_id[JOB_ID_LEN];
    uint64_t token;             // 回显租约令牌
    int32_t status;             // 0 = 成功
    char result[RESULT_LEN];
};

// 控制命令
enum CtrlCmd : int32_t {
    CTRL_SUBMIT = 1,      // 提交新作业
    CTRL_REPLAY = 2,      // 重放单个死信
    CTRL_REPLAY_ALL = 3,  // 重放全部死信
};

struct ControlMsg {
    long mtype;                 // MT_CONTROL
    int32_t cmd;                // CtrlCmd
    int32_t reply_qid;          // 回复队列（IPC_PRIVATE），<0 表示不需要回复
    char job_id[JOB_ID_LEN];
    char payload[PAYLOAD_LEN];
};

// 控制命令回复码
enum ReplyStatus : int32_t {
    ST_ERROR = -1,
    ST_ACCEPTED = 0,       // 新作业已受理
    ST_IN_PROGRESS = 1,    // 已存在，排队或处理中
    ST_ALREADY_DONE = 2,   // 已成功，result 为原结果
    ST_SHUTTING_DOWN = 3,  // 协调器关停中，不再受理
    ST_REPLAYED = 4,       // 死信已重放（extra = 数量）
    ST_NOT_DEAD = 5,       // 不在死信集合中
    ST_UNKNOWN_JOB = 6,    // 未知 job_id
    ST_DEAD = 7,           // 在死信中，需先重放
};

struct ReplyMsg {
    long mtype;             // 固定 1（私有回复队列）
    int32_t status;         // ReplyStatus
    int32_t extra;          // 附加数值（如重放数量）
    char result[RESULT_LEN];
};

// ================= 状态存储 =================

enum class JobState { PENDING, INFLIGHT, SUCCEEDED, DEAD };
const char* state_name(JobState s);

// submit 的结果
enum class SubmitVerdict {
    NEW,                 // 新作业，已受理
    ALREADY_PENDING,     // 重复提交：排队中
    ALREADY_INFLIGHT,    // 重复提交：处理中
    ALREADY_DEAD,        // 重复提交：死信中
    ALREADY_SUCCEEDED,   // 重复提交：已成功（原结果可用）
};

// on_result 的结果
enum class ResultVerdict {
    ACCEPTED_SUCCESS,    // 确认接受，作业成功
    ACCEPTED_FAILURE,    // 确认接受，worker 报告失败（已重新入队）
    LATE_ACK,            // 迟到确认（租约已过期或 token 不匹配），已忽略
    DUP_ACK,             // 重复结果（作业已成功），已忽略
    UNKNOWN_JOB,         // 未知作业
};

struct Job {
    std::string id;
    JobState state = JobState::PENDING;
    std::string payload;
    std::string result;
    uint32_t attempts = 0;   // 已派发次数
    uint64_t token = 0;      // 当前租约令牌（INFLIGHT 时有效）
    std::chrono::steady_clock::time_point deadline{};  // 租约截止（仅运行时）
};

struct Counts {
    size_t pending = 0;
    size_t inflight = 0;
    size_t succeeded = 0;
    size_t dead = 0;
    size_t dup_submits = 0;  // 识别到的重复提交
    size_t late_acks = 0;    // 识别到的迟到确认
    size_t dup_acks = 0;     // 识别到的重复结果
    size_t dispatches = 0;   // 累计派发次数
};

// 一条日志记录（TYPE|ts_ms|job_id|field|field...）
struct LogRecord {
    std::string type;
    uint64_t ts_ms = 0;
    std::string job_id;      // "-" 表示无
    std::vector<std::string> fields;
};

// 顺序扫描日志；遇到损坏尾部时停止并返回 false，
// corrupt_offset（若非空）记录最后一个良好偏移（即损坏起点）。
bool scan_job_log(const std::string& path,
                  const std::function<void(const LogRecord&)>& cb,
                  uint64_t* corrupt_offset = nullptr);

// 可恢复的作业状态存储：内存集合 + 追加式日志（每记录一帧：magic|len|crc32|payload）
class JobStore {
public:
    JobStore() = default;
    ~JobStore() { close(); }

    // 打开并恢复日志；writable=true 时截断损坏尾部并允许追加
    bool open(const std::string& log_path, bool writable);
    void close();

    bool had_corrupt_tail() const { return had_corrupt_tail_; }

    // 以下变更操作均先写日志（fsync）再改内存
    SubmitVerdict submit(const std::string& job_id, const std::string& payload);
    bool dispatch(const std::string& job_id, uint64_t token, uint32_t attempt);
    ResultVerdict on_result(const std::string& job_id, uint64_t token,
                            int32_t status, const std::string& result);
    bool expire(const std::string& job_id, const std::string& reason);
    bool mark_dead(const std::string& job_id, const std::string& reason);
    bool replay(const std::string& job_id);
    // 让所有处理中租约立即过期（重启恢复 / 关停时用），返回过期数量；
    // 达到 max_attempts（>0 时）的作业同时进入死信
    size_t expire_all_inflight(const std::string& reason, int max_attempts);
    void log_boot();
    void log_stop();

    const Job* get(const std::string& job_id) const;
    Job* get_mut(const std::string& job_id);
    Counts counts() const;
    std::vector<std::string> pending_ids() const;   // FIFO 顺序
    std::vector<std::string> inflight_ids() const;
    std::vector<std::string> dead_ids() const;
    uint64_t max_token() const { return max_token_; }

private:
    void apply(const LogRecord& rec);
    bool append_and_apply(const std::string& type, const std::string& job_id,
                          const std::vector<std::string>& fields);

    int fd_ = -1;
    bool writable_ = false;
    bool had_corrupt_tail_ = false;
    std::unordered_map<std::string, Job> jobs_;
    std::deque<std::string> pending_;   // PENDING 的 FIFO 顺序
    uint64_t max_token_ = 0;
    size_t st_dup_submits_ = 0, st_late_acks_ = 0, st_dup_acks_ = 0, st_dispatches_ = 0;
};

// ================= 工具 =================

bool valid_job_id(const std::string& id);
uint32_t crc32(const void* data, size_t len);
std::string sanitize_field(const std::string& s);
std::string format_ts(uint64_t ts_ms);
// 由状态目录推导消息队列 key（queue.key 不存在时返回 -1）
key_t queue_key_for(const std::string& state_dir);

// ================= 运行模式（job_runner.cpp） =================

int run_coordinator(int argc, char** argv);
int run_worker(int argc, char** argv);
int run_submit_cmd(int argc, char** argv);
int run_status_cmd(int argc, char** argv);
int run_history_cmd(int argc, char** argv);
int run_replay_cmd(int argc, char** argv);

} // namespace jobq
} // namespace ipc

#endif // JOB_QUEUE_H
