cmake_minimum_required(VERSION 3.16)

# 启用测试
enable_testing()

# 查找线程库
find_package(Threads REQUIRED)

# 测试源文件
set(TEST_SOURCES
    test_main.cpp
    test_pipe.cpp
    test_named_pipe.cpp
    test_shared_memory.cpp
    test_message_queue.cpp
    test_signal.cpp
    test_socket.cpp
    test_job_queue.cpp
    ${CMAKE_SOURCE_DIR}/src/ipc/job_queue.cpp
)

# 测试可执行文件
add_executable(ipc_tests ${TEST_SOURCES})

# 集成测试需要拉起 ipc_demo 进程
add_dependencies(ipc_tests ipc_demo)
target_compile_definitions(ipc_tests PRIVATE IPC_DEMO_BIN="$<TARGET_FILE:ipc_demo>")

# 包含目录
target_include_directories(ipc_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_SOURCE_DIR}/src/ipc
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# 链接库
target_link_libraries(ipc_tests PRIVATE Threads::Threads rt)

# 添加测试
add_test(NAME ipc_tests COMMAND ipc_tests)
