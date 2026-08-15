cmake_minimum_required(VERSION 3.20)
project(MQBroker CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -Wpedantic)

option(ENABLE_ASAN  "Build with AddressSanitizer"  OFF)
option(ENABLE_TSAN  "Build with ThreadSanitizer"   OFF)

if(ENABLE_ASAN)
  add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address)
endif()
if(ENABLE_TSAN)
  add_compile_options(-fsanitize=thread)
  add_link_options(-fsanitize=thread)
endif()

# ── Core library ─────────────────────────────────────────────────────────────
add_library(broker_core STATIC
  src/TopicRouter.cpp
  src/TcpServer.cpp
  src/AckEngine.cpp
  src/WalWriter.cpp
)
target_include_directories(broker_core PUBLIC include)
target_link_libraries(broker_core PUBLIC pthread)

if(WIN32)
  target_link_libraries(broker_core PUBLIC ws2_32)
endif()

# ── Broker executable ────────────────────────────────────────────────────────
add_executable(broker src/main.cpp)
target_link_libraries(broker PRIVATE broker_core)

# ── Unit tests ───────────────────────────────────────────────────────────────
add_executable(test_core tests/test_core.cpp)
target_link_libraries(test_core PRIVATE broker_core)
