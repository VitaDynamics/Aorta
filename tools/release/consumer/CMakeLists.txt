# Clean CMake consumer for the staged Aorta C++ SDK bundle.
#
# This project knows nothing about the Aorta source tree: it is configured with
# the unpacked archive as the ONLY prefix, so anything it needs — the Aorta
# package, the schema targets, the FlatBuffers runtime — has to come out of the
# bundle. That is the point: the previous CI consumer test fabricated a
# FlatBuffers package config and built on the bundle's own build host, so a
# bundle that no consumer could use still passed.
cmake_minimum_required(VERSION 3.16)
project(aorta_sdk_consumer_proof LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# The driver passes the Core ABI it parsed from core/src/ffi/c_api/mod.rs; the
# bundle's package must request-match and advertise exactly that generation.
if(NOT DEFINED AORTA_EXPECTED_CORE_ABI)
  message(FATAL_ERROR "AORTA_EXPECTED_CORE_ABI must be set to the Core ABI generation")
endif()
find_package(Aorta ${AORTA_EXPECTED_CORE_ABI} CONFIG REQUIRED)

if(NOT Aorta_CORE_ABI_VERSION EQUAL AORTA_EXPECTED_CORE_ABI)
  message(FATAL_ERROR "unexpected Aorta Core ABI: ${Aorta_CORE_ABI_VERSION} (expected ${AORTA_EXPECTED_CORE_ABI})")
endif()
message(STATUS "Aorta_VERSION=${Aorta_VERSION} ABI=${Aorta_CORE_ABI_VERSION}")

foreach(_required_target Aorta::cpp Aorta::cpp_static Aorta::core_ffi Aorta::schemas Aorta::schema_aorta_base Aorta::schema_action_common)
  if(NOT TARGET ${_required_target})
    message(FATAL_ERROR "Missing ${_required_target}")
  endif()
endforeach()

foreach(_tool proof_server proof_client)
  add_executable(${_tool} ${_tool}.cc)
  target_compile_features(${_tool} PRIVATE cxx_std_17)
  target_link_libraries(${_tool} PRIVATE Aorta::cpp)
endforeach()
