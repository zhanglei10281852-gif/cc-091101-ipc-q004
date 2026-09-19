# IPC Demo Backend

C++ 进程间通信演示程序，包含 6 种 IPC 方式演示，以及基于 System V 消息队列的
离线任务队列（协调器 / Worker 模式）。

## 本地编译

```bash
mkdir -p build && cd build
cmake ..
make
./ipc_demo
```

## 运行选项

- 交互模式：`./ipc_demo`
- 运行所有演示：`./ipc_demo --all`
- 查看全部命令：`./ipc_demo --help`

## 离线任务队列

```bash
# 协调器：受理输入文件中的作业（每行 "job_id payload"），租约确认、重试、死信
./ipc_demo coordinator --state-dir /tmp/jobs --input jobs.txt \
    --lease-ms 2000 --max-attempts 3

# worker：从消息队列领取任务并回传结果（可启动多个）
./ipc_demo worker --state-dir /tmp/jobs --name w1

# 管理命令
./ipc_demo submit  --state-dir /tmp/jobs --job-id job-1 --payload "data"
./ipc_demo status  --state-dir /tmp/jobs
./ipc_demo history --state-dir /tmp/jobs --job-id job-1
./ipc_demo replay  --state-dir /tmp/jobs --job-id job-1   # 或 --all
```

语义要点：

- 作业只有收到对应租约 token 的完成确认才算成功；超时未确认会重新投递
- 同一 job_id 的迟到确认、重复结果会被识别且**不重复计入**
- 重复提交已成功的作业会直接返回原结果
- 状态保存在 `<state-dir>/jobs.log`（追加 + fsync + CRC 帧），重启后重建
  待处理 / 处理中 / 成功 / 死信集合；损坏的日志尾部被忽略
- 同一状态目录只允许一个协调器运行；SIGTERM 关停时停止受理新作业，
  等待当前租约结算后保存状态并删除消息队列

worker 测试钩子：`--crash-after N`（收到第 N+1 个任务后模拟崩溃）、
`--delay-ms N`（延迟确认，触发迟到确认）、`--send-duplicates`（重复发送确认）、
`--exit-after N`（处理 N 个任务后退出）。

## 依赖

- GCC 9+ 或 Clang 10+
- CMake 3.16+
- POSIX 兼容系统 (Linux/macOS)
