# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

include_guard()

include(cmake/utils.cmake)

if(NOT EXISTS ${PROJECT_BINARY_DIR}/libevent-src)
  FetchContent_DeclareGitHubWithMirror(libevent
    libevent/libevent release-2.1.11-stable
    MD5=b3185885cad72a4fc2f2d2194dfee2cc
  )
else()
 FetchContent_Declare(libevent
    SOURCE_DIR ${PROJECT_BINARY_DIR}/libevent-src
  )
endif() 

FetchContent_MakeAvailableWithArgs(libevent
  EVENT__DISABLE_TESTS=ON
  EVENT__DISABLE_REGRESS=ON
  EVENT__DISABLE_SAMPLES=ON
  EVENT__DISABLE_OPENSSL=ON
  EVENT__LIBRARY_TYPE=STATIC
  EVENT__DISABLE_BENCHMARK=ON
)

add_library(event_with_headers INTERFACE)
target_include_directories(event_with_headers INTERFACE ${libevent_SOURCE_DIR}/include ${libevent_BINARY_DIR}/include)
target_link_libraries(event_with_headers INTERFACE event event_pthreads)
