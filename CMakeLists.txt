cmake_minimum_required(VERSION 3.0)
project(raft-kv)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g")

# 设置包含目录
include_directories(
        ${CMAKE_SOURCE_DIR}
)

find_package(PkgConfig REQUIRED)
pkg_search_module(GLIB2 REQUIRED glib-2.0)
pkg_search_module(HIREDIS REQUIRED hiredis)
find_package(Boost REQUIRED COMPONENTS filesystem system)

include_directories(${GLIB2_INCLUDE_DIRS} ${Boost_INCLUDE_DIRS})

# 添加pthread库
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

set(SRC
        common/log.h
        common/slice.h
        common/status.cpp
        common/bytebuffer.cpp
        common/random_device.cpp
        raft/proto.cpp
        raft/config.cpp
        raft/raft.cpp
        raft/storage.cpp
        raft/node.cpp
        raft/raft_log.cpp
        raft/unstable.cpp
        raft/progress.cpp
        raft/readonly.cpp
        raft/raft_status.cpp
        raft/ready.cpp
        raft/util.cpp
        server/raft_node.cpp
        server/redis_session.cpp
        server/redis_store.cpp
        snap/snapshotter.cpp
        transport/proto.h
        transport/transport.h
        transport/transport.cpp
        transport/peer.h
        transport/raft_server.cpp
        transport/peer.cpp
        wal/wal.cpp
        raft-kv.cpp
)

add_executable(raft-kv ${SRC})

# 设置链接库
target_link_libraries(
        raft-kv
        ${GLIB2_LIBRARIES}
        ${Boost_FILESYSTEM_LIBRARY}
        ${Boost_SYSTEM_LIBRARY}
        ${HIREDIS_LIBRARIES}
        Threads::Threads
)
