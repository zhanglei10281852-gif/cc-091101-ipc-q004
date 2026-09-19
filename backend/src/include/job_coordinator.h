#ifndef JOB_COORDINATOR_H
#define JOB_COORDINATOR_H

/**
 * 离线分析任务协调器 (Job Coordinator)
 *
 * 在 System V 消息队列之上提供"至少一次投递 + 结果去重"的作业调度：
 * - 协调器受理带唯一 job_id 的作业，收到对应完成确认后才计为成功
 * - 超过租约未确认的作业重新投递；迟到确认与重复结果被识别且不计入
 * - 状态以可恢复的本地日志保存，重启后重建 待处理/处理中/成功/死信 集合
 * - 日志尾部半条记录在恢复时被忽略并截断
 * - 达到重试上限的作业进入死信，可查询、可显式重放
 * - 同一状态目录只允许一个协调器实例（后启动者失败退出）
 * - 优雅关停：停止受理新作业，等待在途租约结算，保存一致状态并删除消息队列
 */

#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <vector>

namespace ipc {
namespace coord {

// ---- 有线协议（协调器 / worker / CLI 均为同一二进制，结构体布局一致）----
namespace proto {

constexpr long MTYPE_JOB = 1;    // 协调器 → worker：派发作业
constexpr long MTYPE_ACK = 2;    // worker → 协调器：完成/失败确认
constexpr long MTYPE_CTRL = 3;   // CLI → 协调器：控制命令（提交/重放）
constexpr long REPLY_BASE = 1000;  // 协调器 → CLI：应答 mtype = REPLY_BASE + 请求方 pid

constexpr size_t MAX_PAYLOAD = 180;
constexpr size_t MAX_RESULT = 180;
constexpr size_t MAX_TEXT = 400;

struct JobMsg {
    long mtype;      // MTYPE_JOB
    uint64_t job_id;
    uint32_t attempt;  // 第几次尝试（从 1 开始）
    char payload[MAX_PAYLOAD + 1];
};

struct AckMsg {
    long mtype;      // MTYPE_ACK
    uint64_t job_id;
    uint32_t attempt;  // 对应哪一次派发（用于识别迟到确认）
    int32_t ok;        // 1=成功 0=失败
    char result[MAX_RESULT + 1];
};

constexpr uint32_t CTRL_SUBMIT = 1;
constexpr uint32_t CTRL_REPLAY = 2;  // job_id=0 表示重放全部死信

struct CtrlMsg {
    long mtype;      // MTYPE_CTRL
    long reply_to;   // 请求方 pid
    uint32_t cmd;
    uint64_t job_id;
    char payload[MAX_PAYLOAD + 1];
};

struct CtrlReply {
    long mtype;      // REPLY_BASE + 请求方 pid
    int32_t code;    // 0=受理 1=重复(已成功,附原结果) 2=重复(待处理/在途) 3=重复(死信) 负值=错误
    char text[MAX_TEXT + 1];
};

}  // namespace proto

// ---- 作业状态 ----
enum class JobState { PENDING, IN_FLIGHT, SUCCEEDED, DEAD };

const char* state_name(JobState s);

struct JobInfo {
    uint64_t job_id = 0;
    JobState state = JobState::PENDING;
    uint32_t attempts = 0;
    std::string payload;
    std::string result;
};

struct StatusInfo {
    size_t pending = 0;
    size_t in_flight = 0;
    size_t succeeded = 0;
    size_t dead = 0;
    std::vector<JobInfo> jobs;  // 按提交顺序
};

struct CoordinatorConfig {
    std::string state_dir;        // 状态目录（日志/锁/队列 key 文件）
    std::string input_file;       // 可选，启动时受理的作业文件："job_id payload" 每行一条
    long lease_ms = 2000;         // 租约时长：超时未确认则重新投递
    int max_attempts = 3;         // 最大尝试次数，耗尽进入死信
    int max_in_flight = 8;        // 同时在途租约上限
    long shutdown_grace_ms = 5000;  // 关停时等待租约结算的宽限
    bool quiet = false;           // 静默模式（测试用）
};

struct WorkerConfig {
    std::string state_dir;
    long work_ms = 100;           // 模拟处理耗时
    uint64_t crash_on_job = 0;    // 收到该作业后立即崩溃（不应答），用于故障演练
    uint64_t fail_on_job = 0;     // 对该作业总是返回失败确认
    bool dup_ack = false;         // 每条确认发送两遍，用于演练重复结果识别
    long reconnect_ms = 3000;     // 队列消失后的重连预算（协调器重启场景）
    bool quiet = false;
};

// worker 对作业 payload 的确定性"分析"：大写化 + 长度
std::string compute_result(const std::string& payload);

// ---- 协调器 ----
// 单线程对象：构造即恢复状态并创建消息队列；poll_once() 驱动事件循环。
// 析构不删除消息队列（相当于崩溃），正常退出必须调用 shutdown()。
class Coordinator {
public:
    explicit Coordinator(const CoordinatorConfig& cfg);
    ~Coordinator();
    Coordinator(const Coordinator&) = delete;
    Coordinator& operator=(const Coordinator&) = delete;

    bool ok() const { return ok_; }
    const std::string& error() const { return err_; }

    // 提交作业。返回：0=新受理 1=重复(已成功, out_result 为原结果)
    // 2=重复(待处理/在途) 3=重复(死信, 需重放) 负值=错误(关停中/非法 id)
    int submit_job(uint64_t job_id, const std::string& payload, std::string* out_result);

    void poll_once();             // 处理一轮：收确认/控制消息、租约检查、派发
    void request_stop() { stop_requested_ = true; }
    bool stop_requested() const { return stop_requested_; }

    int replay_job(uint64_t job_id);  // 重放死信（0=全部）；返回重放数量，<0 表示目标不是死信
    StatusInfo status() const;

    // 优雅关停：等待在途租约结算 → 写 SHUTDOWN 记录与一致快照 → 删除消息队列
    void shutdown();
    bool shutdown_done() const { return shutdown_done_; }

    int queue_id() const { return msqid_; }  // 供测试注入消息

private:
    struct JobRec {
        uint64_t id = 0;
        std::string payload;
        JobState state = JobState::PENDING;
        uint32_t attempts = 0;
        uint64_t lease_deadline_mono = 0;  // 单调时钟毫秒
        std::string result;
    };

    bool log_append(const std::string& body);
    void load_input(const std::string& path);
    void drain_messages();
    void handle_ack(const proto::AckMsg& ack);
    void handle_ctrl(const proto::CtrlMsg& ctrl);
    void expire_leases();
    void dispatch_pending();
    void requeue_or_dead(JobRec& j, const std::string& reason);
    size_t in_flight() const;
    void write_snapshot();
    void log_line(const std::string& msg) const;

    CoordinatorConfig cfg_;
    bool ok_ = false;
    std::string err_;
    int lock_fd_ = -1;
    int log_fd_ = -1;
    int msqid_ = -1;
    std::string log_path_;
    bool stop_requested_ = false;
    bool shutdown_done_ = false;
    std::map<uint64_t, JobRec> jobs_;
    std::vector<uint64_t> submit_order_;  // 提交顺序
    std::deque<uint64_t> pending_;
};

// ---- 运行模式（阻塞，供子命令/子进程调用）----
int run_coordinator(const CoordinatorConfig& cfg);  // 直到 SIGTERM/SIGINT 优雅关停
int run_worker(const WorkerConfig& cfg);            // 直到协调器队列消失且重连超时

// ---- CLI 命令（通过消息队列与运行中的协调器通信）----
int cmd_submit(const std::string& state_dir, uint64_t job_id, const std::string& payload,
               std::string* out);
int cmd_replay(const std::string& state_dir, uint64_t job_id, std::string* out);

// ---- 离线日志检查（无需协调器运行；日志即事实源）----
bool read_status(const std::string& state_dir, StatusInfo* out, std::string* err);
std::string job_history(const std::string& state_dir, uint64_t job_id);
int cmd_status(const std::string& state_dir);
int cmd_history(const std::string& state_dir, uint64_t job_id);

}  // namespace coord
}  // namespace ipc

#endif  // JOB_COORDINATOR_H
