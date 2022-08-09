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

if(NOT EXISTS ${PROJECT_BINARY_DIR}/gtest-src)
  FetchContent_DeclareGitHubWithMirror(gtest
    google/googletest release-1.11.0
    MD5=52943a59cefce0ae0491d4d2412c120b
  )
else()
 FetchContent_Declare(gtest
    SOURCE_DIR ${PROJECT_BINARY_DIR}/gtest-src
  )
endif() 

FetchContent_MakeAvailableWithArgs(gtest
  BUILD_GMOCK=OFF
  INSTALL_GTEST=OFF
)
