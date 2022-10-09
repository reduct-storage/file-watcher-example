cmake_minimum_required(VERSION 3.23)
project(file_watcher_example)

set(CMAKE_CXX_STANDARD 20)


find_package(ReductCpp 1.0.0)
find_package(ZLIB)
find_package(OpenSSL)

add_executable(file_watcher main.cc)
target_link_libraries(file_watcher ${REDUCT_CPP_LIBRARIES} ${ZLIB_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto)

