/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "VeloxExecution.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>

#include "VeloxBackend.h"
#include "compute/ResultIterator.h"
#include "compute/Runtime.h"
#include "compute/VeloxPlanConverter.h"
#include "config/VeloxConfig.h"
#include "operators/serializer/VeloxRowToColumnarConverter.h"
#include "shuffle/VeloxHashShuffleWriter.h"
#include "shuffle/VeloxRssSortShuffleWriter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "utils/ConfigExtractor.h"
#include "utils/VeloxArrowUtils.h"

#ifdef ENABLE_HDFS

#include "operators/writer/VeloxParquetDatasourceHDFS.h"

#endif

#ifdef ENABLE_S3
#include "operators/writer/VeloxParquetDatasourceS3.h"
#endif

#ifdef ENABLE_GCS
#include "operators/writer/VeloxParquetDatasourceGCS.h"
#endif

#ifdef ENABLE_ABFS
#include "operators/writer/VeloxParquetDatasourceABFS.h"
#endif

using namespace facebook;

namespace gluten {

VeloxExecution::VeloxExecution(Runtime* runtime) : Execution(runtime) {
  // Refresh session config.
  vmm_ = std::make_unique<VeloxMemoryManager>(runtime_->allocationListener());
  veloxCfg_ = std::make_shared<facebook::velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>(runtime_->getConfMap()));
  debugModeEnabled_ = veloxCfg_->get<bool>(kDebugModeEnabled, false);
  FLAGS_minloglevel = veloxCfg_->get<uint32_t>(kGlogSeverityLevel, FLAGS_minloglevel);
  FLAGS_v = veloxCfg_->get<uint32_t>(kGlogVerboseLevel, FLAGS_v);
}

void VeloxExecution::parsePlan(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile) {
  if (debugModeEnabled_ || dumpFile.has_value()) {
    try {
      auto planJson = substraitFromPbToJson("Plan", data, size, dumpFile);
      LOG_IF(INFO, debugModeEnabled_) << std::string(50, '#') << " received substrait::Plan: " << taskInfo_ << std::endl
                                      << planJson;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting Substrait plan to JSON: " << e.what();
    }
  }

  GLUTEN_CHECK(parseProtobuf(data, size, &substraitPlan_) == true, "Parse substrait plan failed");
}

void VeloxExecution::parseSplitInfo(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile) {
  if (debugModeEnabled_ || dumpFile.has_value()) {
    try {
      auto splitJson = substraitFromPbToJson("ReadRel.LocalFiles", data, size, dumpFile);
      LOG_IF(INFO, debugModeEnabled_) << std::string(50, '#')
                                      << " received substrait::ReadRel.LocalFiles: " << taskInfo_ << std::endl
                                      << splitJson;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting Substrait plan to JSON: " << e.what();
    }
  }
  ::substrait::ReadRel_LocalFiles localFile;
  GLUTEN_CHECK(parseProtobuf(data, size, &localFile) == true, "Parse substrait plan failed");
  localFiles_.push_back(localFile);
}

void VeloxExecution::getInfoAndIds(
    const std::unordered_map<velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
    const std::unordered_set<velox::core::PlanNodeId>& leafPlanNodeIds,
    std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    std::vector<velox::core::PlanNodeId>& scanIds,
    std::vector<velox::core::PlanNodeId>& streamIds) {
  for (const auto& leafPlanNodeId : leafPlanNodeIds) {
    auto it = splitInfoMap.find(leafPlanNodeId);
    if (it == splitInfoMap.end()) {
      throw std::runtime_error("Could not find leafPlanNodeId.");
    }
    auto splitInfo = it->second;
    if (splitInfo->isStream) {
      streamIds.emplace_back(leafPlanNodeId);
    } else {
      scanInfos.emplace_back(splitInfo);
      scanIds.emplace_back(leafPlanNodeId);
    }
  }
}

std::string VeloxExecution::planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) {
  std::vector<std::shared_ptr<ResultIterator>> inputs;
  auto veloxMemoryPool = gluten::defaultLeafVeloxMemoryPool();
  VeloxPlanConverter veloxPlanConverter(inputs, veloxMemoryPool.get(), sessionConf, std::nullopt, true);
  auto veloxPlan = veloxPlanConverter.toVeloxPlan(substraitPlan_, localFiles_);
  return veloxPlan->toString(details, true);
}

void VeloxExecution::injectWriteFilesTempPath(const std::string& path) {
  writeFilesTempPath_ = path;
}

VeloxMemoryManager* VeloxExecution::memoryManager() {
  return vmm_.get();
}

std::shared_ptr<ResultIterator> VeloxExecution::createResultIterator(
    const std::string& spillDir,
    const std::vector<std::shared_ptr<ResultIterator>>& inputs,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  LOG_IF(INFO, debugModeEnabled_) << "VeloxExecution session config:" << printConfig(runtime_->getConfMap());

  VeloxPlanConverter veloxPlanConverter(inputs, vmm_->getLeafMemoryPool().get(), sessionConf, writeFilesTempPath_);
  veloxPlan_ = veloxPlanConverter.toVeloxPlan(substraitPlan_, std::move(localFiles_));

  // Scan node can be required.
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(veloxPlanConverter.splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto wholestageIter = std::make_unique<WholeStageResultIterator>(
      vmm_.get(), veloxPlan_, scanIds, scanInfos, streamIds, spillDir, sessionConf, taskInfo_);
  return std::make_shared<ResultIterator>(std::move(wholestageIter), this);
}

std::shared_ptr<ColumnarToRowConverter> VeloxExecution::createColumnar2RowConverter(int64_t column2RowMemThreshold) {
  auto veloxPool = vmm_->getLeafMemoryPool();
  return std::make_shared<VeloxColumnarToRowConverter>(veloxPool, column2RowMemThreshold);
}

std::shared_ptr<ColumnarBatch> VeloxExecution::createOrGetEmptySchemaBatch(int32_t numRows) {
  auto& lookup = emptySchemaBatchLoopUp_;
  if (lookup.find(numRows) == lookup.end()) {
    const std::shared_ptr<ColumnarBatch>& batch = gluten::createZeroColumnBatch(numRows);
    lookup.emplace(numRows, batch); // the batch will be released after Spark task ends
  }
  return lookup.at(numRows);
}

std::shared_ptr<ColumnarBatch> VeloxExecution::select(
    std::shared_ptr<ColumnarBatch> batch,
    std::vector<int32_t> columnIndices) {
  auto veloxPool = vmm_->getLeafMemoryPool();
  auto veloxBatch = gluten::VeloxColumnarBatch::from(veloxPool.get(), batch);
  auto outputBatch = veloxBatch->select(veloxPool.get(), std::move(columnIndices));
  return outputBatch;
}

std::shared_ptr<RowToColumnarConverter> VeloxExecution::createRow2ColumnarConverter(struct ArrowSchema* cSchema) {
  auto veloxPool = vmm_->getLeafMemoryPool();
  return std::make_shared<VeloxRowToColumnarConverter>(cSchema, veloxPool);
}

std::shared_ptr<ShuffleWriter> VeloxExecution::createShuffleWriter(
    int numPartitions,
    std::unique_ptr<PartitionWriter> partitionWriter,
    ShuffleWriterOptions options) {
  auto veloxPool = vmm_->getLeafMemoryPool();
  auto arrowPool = vmm_->getArrowMemoryPool();
  GLUTEN_ASSIGN_OR_THROW(
      std::shared_ptr<ShuffleWriter> shuffleWriter,
      VeloxShuffleWriter::create(
          options.shuffleWriterType,
          numPartitions,
          std::move(partitionWriter),
          std::move(options),
          veloxPool,
          arrowPool));
  return shuffleWriter;
}

std::shared_ptr<Datasource> VeloxExecution::createDatasource(
    const std::string& filePath,
    std::shared_ptr<arrow::Schema> schema) {
  static std::atomic_uint32_t id{0UL};
  auto veloxPool = vmm_->getAggregateMemoryPool()->addAggregateChild("datasource." + std::to_string(id++));
  // Pass a dedicate pool for S3 and GCS sinks as can't share veloxPool
  // with parquet writer.
  auto sinkPool = vmm_->getLeafMemoryPool();
  if (isSupportedHDFSPath(filePath)) {
#ifdef ENABLE_HDFS
    return std::make_shared<VeloxParquetDatasourceHDFS>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is hdfs path but the HDFS haven't been enabled when writing parquet data in velox runtime!");
#endif
  } else if (isSupportedS3SdkPath(filePath)) {
#ifdef ENABLE_S3
    return std::make_shared<VeloxParquetDatasourceS3>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is S3 path but the S3 haven't been enabled when writing parquet data in velox runtime!");
#endif
  } else if (isSupportedGCSPath(filePath)) {
#ifdef ENABLE_GCS
    return std::make_shared<VeloxParquetDatasourceGCS>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is GCS path but the GCS haven't been enabled when writing parquet data in velox runtime!");
#endif
  } else if (isSupportedABFSPath(filePath)) {
#ifdef ENABLE_ABFS
    return std::make_shared<VeloxParquetDatasourceABFS>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is ABFS path but the ABFS haven't been enabled when writing parquet data in velox runtime!");
#endif
  }
  return std::make_shared<VeloxParquetDatasource>(filePath, veloxPool, sinkPool, schema);
}

std::shared_ptr<ShuffleReader> VeloxExecution::createShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    ShuffleReaderOptions options) {
  auto rowType = facebook::velox::asRowType(gluten::fromArrowSchema(schema));
  auto codec = gluten::createArrowIpcCodec(options.compressionType, options.codecBackend);
  auto ctxVeloxPool = vmm_->getLeafMemoryPool();
  auto veloxCompressionType = facebook::velox::common::stringToCompressionKind(options.compressionTypeStr);
  auto deserializerFactory = std::make_unique<gluten::VeloxColumnarBatchDeserializerFactory>(
      schema,
      std::move(codec),
      veloxCompressionType,
      rowType,
      options.batchSize,
      vmm_->getArrowMemoryPool(),
      ctxVeloxPool,
      options.shuffleWriterType);
  auto reader = std::make_shared<VeloxShuffleReader>(std::move(deserializerFactory));
  return reader;
}

std::unique_ptr<ColumnarBatchSerializer> VeloxExecution::createColumnarBatchSerializer(struct ArrowSchema* cSchema) {
  auto arrowPool = vmm_->getArrowMemoryPool();
  auto veloxPool = vmm_->getLeafMemoryPool();
  return std::make_unique<VeloxColumnarBatchSerializer>(arrowPool, veloxPool, cSchema);
}

void VeloxExecution::dumpConf(const std::string& path) {
  const auto& backendConfMap = VeloxBackend::get()->getBackendConf()->rawConfigs();
  auto allConfMap = backendConfMap;

  for (const auto& pair : runtime_->getConfMap()) {
    allConfMap.insert_or_assign(pair.first, pair.second);
  }

  // Open file "velox.conf" for writing, automatically creating it if it doesn't exist,
  // or overwriting it if it does.
  std::ofstream outFile(path);
  if (!outFile.is_open()) {
    LOG(ERROR) << "Failed to open file for writing: " << path;
    return;
  }

  // Calculate the maximum key length for alignment.
  size_t maxKeyLength = 0;
  for (const auto& pair : allConfMap) {
    maxKeyLength = std::max(maxKeyLength, pair.first.length());
  }

  // Write each key-value pair to the file with adjusted spacing for alignment
  outFile << "[Backend Conf]" << std::endl;
  for (const auto& pair : backendConfMap) {
    outFile << std::left << std::setw(maxKeyLength + 1) << pair.first << ' ' << pair.second << std::endl;
  }
  outFile << std::endl << "[Session Conf]" << std::endl;
  for (const auto& pair : runtime_->getConfMap()) {
    outFile << std::left << std::setw(maxKeyLength + 1) << pair.first << ' ' << pair.second << std::endl;
  }

  outFile.close();
}

} // namespace gluten
