/**
 * C++ 进程间通信 (IPC) 演示程序
 *
 * 本程序演示了 6 种主要的 IPC 方式：
 * 1. 管道 (Pipe)
 * 2. 命名管道 (Named Pipe / FIFO)
 * 3. 共享内存 (Shared Memory)
 * 4. 消息队列 (Message Queue)
 * 5. 信号 (Signal)
 * 6. Socket (Unix Domain Socket)
 *
 * 以及基于 System V 消息队列的离线任务协调器（见 job_coordinator.cpp）：
 *   ipc_demo coordinator --state-dir DIR [--input FILE] [--lease-ms N]
 *                        [--max-attempts N] [--max-in-flight N]
 *   ipc_demo worker      --state-dir DIR [--work-ms N] [--crash-on-job ID]
 *                        [--fail-on-job ID] [--dup-ack] [--reconnect-ms N]
 *   ipc_demo submit      --state-dir DIR --job ID [--payload TEXT]
 *   ipc_demo status      --state-dir DIR
 *   ipc_demo history     --state-dir DIR --job ID
 *   ipc_demo replay      --state-dir DIR [--job ID | --all]
 */

#include "ipc_demo.h"
#include "job_coordinator.h"

#include <cstdlib>
#include <map>
#include <set>

void print_menu() {
    std::cout << "\n\033[35m╔════════════════════════════════════════════╗\033[0m" << std::endl;
    std::cout << "\033[35m║   C++ 进程间通信 (IPC) 演示程序            ║\033[0m" << std::endl;
    std::cout << "\033[35m╠════════════════════════════════════════════╣\033[0m" << std::endl;
    std::cout << "\033[35m║  1. 管道 (Pipe)                            ║\033[0m" << std::endl;
    std::cout << "\033[35m║  2. 命名管道 (Named Pipe / FIFO)           ║\033[0m" << std::endl;
    std::cout << "\033[35m║  3. 共享内存 (Shared Memory)               ║\033[0m" << std::endl;
    std::cout << "\033[35m║  4. 消息队列 (Message Queue)               ║\033[0m" << std::endl;
    std::cout << "\033[35m║  5. 信号 (Signal)                          ║\033[0m" << std::endl;
    std::cout << "\033[35m║  6. Socket (Unix Domain Socket)            ║\033[0m" << std::endl;
    std::cout << "\033[35m║  7. 运行所有演示                           ║\033[0m" << std::endl;
    std::cout << "\033[35m║  8. 任务协调器 (Job Coordinator)           ║\033[0m" << std::endl;
    std::cout << "\033[35m║  0. 退出                                   ║\033[0m" << std::endl;
    std::cout << "\033[35m╚════════════════════════════════════════════╝\033[0m" << std::endl;
    std::cout << "\n请选择 (0-8): ";
}

void run_all_demos() {
    ipc::Logger::demo("运行所有 IPC 演示");

    ipc::demo_pipe();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ipc::demo_named_pipe();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ipc::demo_shared_memory();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ipc::demo_message_queue();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ipc::demo_signal();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ipc::demo_socket();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ipc::demo_job_coordinator();

    ipc::Logger::info("MAIN", "所有演示完成！");
}

// ---------------------------------------------------------------------------
// 任务协调器子命令
// ---------------------------------------------------------------------------

namespace {

void print_coord_usage() {
    std::cerr << "任务协调器用法：\n"
              << "  ipc_demo coordinator --state-dir DIR [--input FILE] [--lease-ms N]\n"
              << "                       [--max-attempts N] [--max-in-flight N]\n"
              << "  ipc_demo worker      --state-dir DIR [--work-ms N] [--crash-on-job ID]\n"
              << "                       [--fail-on-job ID] [--dup-ack] [--reconnect-ms N]\n"
              << "  ipc_demo submit      --state-dir DIR --job ID [--payload TEXT]\n"
              << "  ipc_demo status      --state-dir DIR\n"
              << "  ipc_demo history     --state-dir DIR --job ID\n"
              << "  ipc_demo replay      --state-dir DIR [--job ID | --all]\n";
}

// 简单参数解析：--key value / --flag
struct Args {
    std::map<std::string, std::string> kv;
    std::set<std::string> flags;
    Args(int argc, char* argv[]) {
        for (int i = 0; i < argc; i++) {
            std::string a = argv[i];
            if (a.rfind("--", 0) == 0) {
                if (i + 1 < argc && argv[i + 1][0] != '-')
                    kv[a.substr(2)] = argv[++i];
                else
                    flags.insert(a.substr(2));
            }
        }
    }
    std::string get(const std::string& k, const std::string& def = "") const {
        auto it = kv.find(k);
        return it == kv.end() ? def : it->second;
    }
    long get_long(const std::string& k, long def) const {
        auto it = kv.find(k);
        return it == kv.end() ? def : atol(it->second.c_str());
    }
    bool has(const std::string& k) const { return flags.count(k) > 0; }
};

int coord_cli_main(int argc, char* argv[]) {
    std::string cmd = argv[0];
    Args args(argc - 1, argv + 1);
    std::string dir = args.get("state-dir");

    if (cmd == "coordinator") {
        if (dir.empty()) {
            print_coord_usage();
            return 1;
        }
        ipc::coord::CoordinatorConfig cfg;
        cfg.state_dir = dir;
        cfg.input_file = args.get("input");
        cfg.lease_ms = args.get_long("lease-ms", cfg.lease_ms);
        cfg.max_attempts = (int)args.get_long("max-attempts", cfg.max_attempts);
        cfg.max_in_flight = (int)args.get_long("max-in-flight", cfg.max_in_flight);
        return ipc::coord::run_coordinator(cfg);
    }
    if (cmd == "worker") {
        if (dir.empty()) {
            print_coord_usage();
            return 1;
        }
        ipc::coord::WorkerConfig cfg;
        cfg.state_dir = dir;
        cfg.work_ms = args.get_long("work-ms", cfg.work_ms);
        cfg.crash_on_job = (uint64_t)args.get_long("crash-on-job", 0);
        cfg.fail_on_job = (uint64_t)args.get_long("fail-on-job", 0);
        cfg.dup_ack = args.has("dup-ack");
        cfg.reconnect_ms = args.get_long("reconnect-ms", cfg.reconnect_ms);
        return ipc::coord::run_worker(cfg);
    }
    if (cmd == "submit") {
        uint64_t job_id = (uint64_t)args.get_long("job", 0);
        if (dir.empty() || job_id == 0) {
            print_coord_usage();
            return 1;
        }
        std::string out;
        int rc = ipc::coord::cmd_submit(dir, job_id, args.get("payload"), &out);
        std::cout << out << std::endl;
        return rc >= 0 ? 0 : 1;
    }
    if (cmd == "status") {
        if (dir.empty()) {
            print_coord_usage();
            return 1;
        }
        return ipc::coord::cmd_status(dir);
    }
    if (cmd == "history") {
        uint64_t job_id = (uint64_t)args.get_long("job", 0);
        if (dir.empty() || job_id == 0) {
            print_coord_usage();
            return 1;
        }
        return ipc::coord::cmd_history(dir, job_id);
    }
    if (cmd == "replay") {
        if (dir.empty()) {
            print_coord_usage();
            return 1;
        }
        uint64_t job_id = args.has("all") ? 0 : (uint64_t)args.get_long("job", 0);
        if (!args.has("all") && job_id == 0) {
            print_coord_usage();
            return 1;
        }
        std::string out;
        int rc = ipc::coord::cmd_replay(dir, job_id, &out);
        std::cout << out << std::endl;
        return rc >= 0 ? 0 : 1;
    }

    print_coord_usage();
    return 1;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc > 1) {
        std::string first = argv[1];
        if (first == "--all") {
            run_all_demos();
            return 0;
        }
        // 任务协调器子命令
        if (first == "coordinator" || first == "worker" || first == "submit" ||
            first == "status" || first == "history" || first == "replay") {
            return coord_cli_main(argc - 1, argv + 1);
        }
    }

    int choice;

    while (true) {
        print_menu();
        std::cin >> choice;

        if (std::cin.fail()) {
            std::cin.clear();
            std::cin.ignore(10000, '\n');
            ipc::Logger::warn("MAIN", "无效输入，请输入数字");
            continue;
        }

        switch (choice) {
            case 0:
                ipc::Logger::info("MAIN", "程序退出，再见！");
                return 0;
            case 1:
                ipc::demo_pipe();
                break;
            case 2:
                ipc::demo_named_pipe();
                break;
            case 3:
                ipc::demo_shared_memory();
                break;
            case 4:
                ipc::demo_message_queue();
                break;
            case 5:
                ipc::demo_signal();
                break;
            case 6:
                ipc::demo_socket();
                break;
            case 7:
                run_all_demos();
                break;
            case 8:
                ipc::demo_job_coordinator();
                break;
            default:
                ipc::Logger::warn("MAIN", "无效选择，请输入 0-8");
        }
    }

    return 0;
}
