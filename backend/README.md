# IPC Demo Backend

C++ 进程间通信演示程序。

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

## 任务协调器 (Job Coordinator)

基于 System V 消息队列的离线分析任务调度，提供至少一次投递与结果去重：

```bash
# 协调器：从输入文件受理 "job_id payload" 作业，崩溃后可重启恢复
./ipc_demo coordinator --state-dir /tmp/coord --input /tmp/jobs.txt \
    [--lease-ms 2000] [--max-attempts 3] [--max-in-flight 8]

# worker：领取作业并回传结果（故障演练：--crash-on-job/--fail-on-job/--dup-ack）
./ipc_demo worker --state-dir /tmp/coord [--work-ms 100] [--reconnect-ms 3000]

# 值守命令
./ipc_demo submit  --state-dir /tmp/coord --job ID [--payload TEXT]  # 已成功→返回原结果
./ipc_demo status  --state-dir /tmp/coord                            # 各状态数量
./ipc_demo history --state-dir /tmp/coord --job ID                   # 单作业尝试历史
./ipc_demo replay  --state-dir /tmp/coord [--job ID | --all]         # 重放死信
```

- 状态目录内容：`coordinator.log`（预写日志，恢复的事实源）、`coordinator.lock`
  （单实例锁，后启动者失败退出）、`queue.key`（队列 ftok 锚点）、`snapshot.txt`
  （关停时的一致快照）。
- 优雅关停：`kill -TERM <coordinator_pid>`，停止受理新作业，等待在途租约结算后
  保存一致状态并删除自己创建的消息队列。

## 依赖

- GCC 9+ 或 Clang 10+
- CMake 3.16+
- POSIX 兼容系统 (Linux/macOS)
