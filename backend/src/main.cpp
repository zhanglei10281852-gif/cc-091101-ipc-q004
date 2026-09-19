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
 */

#include "ipc_demo.h"
#include "job_queue.h"

void print_usage() {
    std::cout << "用法:" << std::endl;
    std::cout << "  ipc_demo                 交互式演示菜单" << std::endl;
    std::cout << "  ipc_demo --all           运行所有 IPC 演示" << std::endl;
    std::cout << std::endl;
    std::cout << "离线任务队列（基于 System V 消息队列）:" << std::endl;
    std::cout << "  ipc_demo coordinator --state-dir DIR [--input FILE] [--lease-ms N]" << std::endl;
    std::cout << "                       [--max-attempts N] [--poll-ms N]" << std::endl;
    std::cout << "  ipc_demo worker      --state-dir DIR [--name N] [--delay-ms N]" << std::endl;
    std::cout << "                       [--crash-after N] [--exit-after N] [--send-duplicates]" << std::endl;
    std::cout << "  ipc_demo submit      --state-dir DIR --job-id ID [--payload TEXT]" << std::endl;
    std::cout << "  ipc_demo status      --state-dir DIR" << std::endl;
    std::cout << "  ipc_demo history     --state-dir DIR --job-id ID" << std::endl;
    std::cout << "  ipc_demo replay      --state-dir DIR (--job-id ID | --all)" << std::endl;
}

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
    std::cout << "\033[35m║  0. 退出                                   ║\033[0m" << std::endl;
    std::cout << "\033[35m╚════════════════════════════════════════════╝\033[0m" << std::endl;
    std::cout << "\n请选择 (0-7): ";
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
    
    ipc::Logger::info("MAIN", "所有演示完成！");
}

int main(int argc, char* argv[]) {
    if (argc > 1) {
        std::string cmd = argv[1];
        // 如果有命令行参数 --all，直接运行所有演示
        if (cmd == "--all") {
            run_all_demos();
            return 0;
        }
        // 离线任务队列：协调器 / worker / 管理命令
        if (cmd == "coordinator") return ipc::jobq::run_coordinator(argc - 2, argv + 2);
        if (cmd == "worker")      return ipc::jobq::run_worker(argc - 2, argv + 2);
        if (cmd == "submit")      return ipc::jobq::run_submit_cmd(argc - 2, argv + 2);
        if (cmd == "status")      return ipc::jobq::run_status_cmd(argc - 2, argv + 2);
        if (cmd == "history")     return ipc::jobq::run_history_cmd(argc - 2, argv + 2);
        if (cmd == "replay")      return ipc::jobq::run_replay_cmd(argc - 2, argv + 2);
        if (cmd == "--help" || cmd == "-h") {
            print_usage();
            return 0;
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
            default:
                ipc::Logger::warn("MAIN", "无效选择，请输入 0-7");
        }
    }
    
    return 0;
}
