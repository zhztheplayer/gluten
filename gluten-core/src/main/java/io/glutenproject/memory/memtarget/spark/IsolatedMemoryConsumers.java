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
package io.glutenproject.memory.memtarget.spark;

import io.glutenproject.GlutenConfig;
import io.glutenproject.memory.MemoryUsageStatsBuilder;

import org.apache.spark.memory.TaskMemoryManager;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * A hub to provide memory target instances whose shared size (in the same task) is limited to X, X
 * = executor memory / task slots.
 *
 * <p>Using this to prevent OOMs if the delegated memory target could possibly hold large memory
 * blocks that are not spillable.
 *
 * <p>See <a href="https://github.com/oap-project/gluten/issues/3030">GLUTEN-3030</a>
 */
public class IsolatedMemoryConsumers {
  private static final WeakHashMap<TaskMemoryManager, TreeMemoryConsumerNode> MAP =
      new WeakHashMap<>();

  private IsolatedMemoryConsumers() {}

  private TreeMemoryConsumerNode getSharedAccount(TaskMemoryManager tmm) {
    synchronized (MAP) {
      return MAP.computeIfAbsent(
          tmm,
          m -> {
            TreeMemoryConsumerNode tmc = new TreeMemoryConsumer(m);
            return tmc.newChild(
                "ROOT",
                GlutenConfig.getConf().conservativeTaskOffHeapMemorySize(),
                Spiller.NO_OP,
                Collections.emptyMap());
          });
    }
  }

  public TreeMemoryConsumerNode newConsumer(
      TaskMemoryManager tmm,
      String name,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    return getSharedAccount(tmm).newChild(name, Long.MAX_VALUE, spiller, virtualChildren);
  }
}
