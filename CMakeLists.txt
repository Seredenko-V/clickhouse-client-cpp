cmake_minimum_required(VERSION 3.12)
project(application-example)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(contribs/clickhouse-client)

set (RESPONSES_HANDLER "responses_handler.hpp" "responses_handler.cpp")

add_executable(${PROJECT_NAME} "app.cpp" ${RESPONSES_HANDLER})

target_include_directories(${PROJECT_NAME} PRIVATE contribs/clickhouse-client/ contribs/clickhouse-client/contrib/absl)

target_link_libraries(${PROJECT_NAME} PRIVATE clickhouse-cpp-lib)
