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
package org.apache.gluten.columnarbatch;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.Runtimes;
import org.apache.gluten.utils.ArrowAbiUtil;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.task.TaskResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A pool for all native batch handles that point to offloaded column batches that have zero
 * effective columns (though they may have meaningful row numbers). For zero-column batches,
 * since we don't have a way to map them to light column batches with indicator vectors as
 * there is no place in a Spark ColumnBatch with zero columns to populate an indicator vector.
 * Thus, we manage the batch handles directly from Java side. One handle for batches with certain
 * row number will be reused.
 */
class EmptySchemaBatchHandlePool implements TaskResource {
  private static final Logger LOG = LoggerFactory.getLogger(EmptySchemaBatchHandlePool.class);

  private final Map<Integer, Long> uniqueHandles = new ConcurrentHashMap<>();


  @Override
  public void release() throws Exception {
    uniqueHandles.forEach((numRows, handle) -> {

    });
  }

  long obtain(int numRows) {
    return uniqueHandles.computeIfAbsent(numRows, r -> {
      final Runtime runtime = Runtimes.contextInstance("EmptySchemaBatchHandlePool#obtain");
      final BufferAllocator allocator = ArrowBufferAllocators.contextInstance();
      try (ArrowArray cArray = ArrowArray.allocateNew(allocator);
           ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
           ColumnarBatch batch = new ColumnarBatch(new ColumnVector[0], r)) {
        ArrowAbiUtil.exportFromSparkColumnarBatch(allocator, batch, cSchema, cArray);
        return ColumnarBatchJniWrapper.create(runtime)
                .createWithArrowArray(cSchema.memoryAddress(), cArray.memoryAddress());
      }
    });
  }

  @Override
  public int priority() {
    // Higher than indicator vector pool.
    return 15;
  }

  @Override
  public String resourceName() {
    return EmptySchemaBatchHandlePool.class.getName();
  }
}
