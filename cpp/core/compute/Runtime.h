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

#pragma once

#include <glog/logging.h>

#include "memory/AllocationListener.h"
#include "utils/ObjectStore.h"

namespace gluten {

class ResultIterator;

class Runtime : public std::enable_shared_from_this<Runtime> {
 public:
  Runtime(
      const std::string& kind,
      std::unique_ptr<AllocationListener> listener,
      const std::unordered_map<std::string, std::string>& confMap)
      : kind_(kind), listener_(std::move(listener)), confMap_(confMap) {}

  static Runtime* create(
      const std::string& kind,
      std::unique_ptr<AllocationListener> listener,
      const std::unordered_map<std::string, std::string>& sessionConf = {});
  static void release(Runtime*);

  virtual ~Runtime() = default;
  virtual AllocationListener* allocationListener() {
    return listener_.get();
  };

  const std::unordered_map<std::string, std::string>& getConfMap() {
    return confMap_;
  }

  ObjectHandle saveObject(std::shared_ptr<void> obj) {
    return objStore_->save(obj);
  }

 protected:
  std::unique_ptr<ObjectStore> objStore_ = ObjectStore::create();
  std::string kind_;
  std::unique_ptr<AllocationListener> listener_;
  std::unordered_map<std::string, std::string> confMap_; // Session conf map
};
} // namespace gluten
