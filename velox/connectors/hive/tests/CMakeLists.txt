# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
add_executable(
  velox_hive_connector_test
  FileHandleTest.cpp
  HiveConfigTest.cpp
  HiveDataSinkTest.cpp
  HiveConnectorTest.cpp
  HiveConnectorUtilTest.cpp
  HiveConnectorSerDeTest.cpp
  HivePartitionFunctionTest.cpp
  HivePartitionUtilTest.cpp
  HiveSplitTest.cpp
  PartitionIdGeneratorTest.cpp
  TableHandleTest.cpp)
add_test(velox_hive_connector_test velox_hive_connector_test)

target_link_libraries(
  velox_hive_connector_test
  PUBLIC velox_hive_connector
  PUBLIC velox_hive_partition_function
  PUBLIC velox_dwio_common_exception
  PRIVATE velox_dwio_registered_flush_policies
  PUBLIC velox_vector_fuzzer
  PUBLIC velox_vector_test_lib
  PUBLIC velox_exec
  PUBLIC velox_exec_test_lib
  PUBLIC GTest::gtest
  PUBLIC GTest::gtest_main)


if(VELOX_ENABLE_PARQUET)

  target_include_directories(velox_hive_connector_test
                             PUBLIC ${ARROW_PREFIX}/install/include)
  target_link_libraries(velox_hive_connector_test PUBLIC velox_dwio_parquet_writer
                        PUBLIC velox_dwio_parquet_reader)

endif()
