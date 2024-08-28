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

#include "Execution.h"

namespace gluten {

namespace {

class FactoryRegistry {
 public:
  void registerFactory(const std::string& kind, Execution::Factory factory) {
    std::lock_guard<std::mutex> l(mutex_);
    GLUTEN_CHECK(map_.find(kind) == map_.end(), "Execution factory already registered for " + kind);
    map_[kind] = std::move(factory);
  }

  Execution::Factory& getFactory(const std::string& kind) {
    std::lock_guard<std::mutex> l(mutex_);
    GLUTEN_CHECK(map_.find(kind) != map_.end(), "Execution factory not registered for " + kind);
    return map_[kind];
  }

  bool unregisterFactory(const std::string& kind) {
    std::lock_guard<std::mutex> l(mutex_);
    GLUTEN_CHECK(map_.find(kind) != map_.end(), "Execution factory not registered for " + kind);
    return map_.erase(kind);
  }

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, Execution::Factory> map_;
};

FactoryRegistry& executionFactories() {
  static FactoryRegistry registry;
  return registry;
}
}

void Execution::registerFactory(const std::string& kind, Execution::Factory factory) {
  executionFactories().registerFactory(kind, std::move(factory));
}

Execution* Execution::create(const std::string& kind, Runtime* runtime) {
  auto& factory = executionFactories().getFactory(kind);
  return factory(runtime);
}

void Execution::release(Execution* execution) {
  delete execution;
}

Execution::Execution(gluten::Runtime* runtime) : runtime_(runtime) {}

}
