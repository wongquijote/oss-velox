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
add_subdirectory(compression)
add_subdirectory(encryption)
add_subdirectory(exception)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
elseif(${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests/utils)
endif()

velox_add_library(
  velox_dwio_common
  BitConcatenation.cpp
  BitPackDecoder.cpp
  BufferedInput.cpp
  CacheInputStream.cpp
  CachedBufferedInput.cpp
  ColumnLoader.cpp
  ColumnSelector.cpp
  DataBufferHolder.cpp
  DecoderUtil.cpp
  DirectBufferedInput.cpp
  DirectDecoder.cpp
  DirectInputStream.cpp
  DwioMetricsLog.cpp
  ExecutorBarrier.cpp
  FileSink.cpp
  FlatMapHelper.cpp
  OnDemandUnitLoader.cpp
  InputStream.cpp
  IntDecoder.cpp
  MetadataFilter.cpp
  Options.cpp
  OutputStream.cpp
  ParallelFor.cpp
  Range.cpp
  Reader.cpp
  ReaderFactory.cpp
  ScanSpec.cpp
  SeekableInputStream.cpp
  SelectiveByteRleColumnReader.cpp
  SelectiveColumnReader.cpp
  SelectiveFlatMapColumnReader.cpp
  SelectiveRepeatedColumnReader.cpp
  SelectiveStructColumnReader.cpp
  SortingWriter.cpp
  SortingWriter.h
  Throttler.cpp
  TypeUtils.cpp
  TypeWithId.cpp
  Writer.cpp
  WriterFactory.cpp)

velox_include_directories(velox_dwio_common PRIVATE ${Protobuf_INCLUDE_DIRS} ${PROJECT_BINARY_DIR})

velox_link_libraries(
  velox_dwio_common
  PUBLIC velox_buffer
  PUBLIC velox_caching
  PUBLIC velox_common_io
  PUBLIC velox_common_compression
  PUBLIC velox_common_config
  PUBLIC velox_dwio_common_encryption
  PUBLIC velox_dwio_common_exception
  PRIVATE velox_dwio_registered_flush_policies
  PUBLIC velox_exception
  PUBLIC velox_expression
  PUBLIC velox_memory
  PUBLIC velox_type_tz
  PUBLIC Boost::regex
  PUBLIC Folly::folly
  PUBLIC glog::glog
  PUBLIC protobuf::libprotobuf)
