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

package io.glutenproject.memory.arrow.pool;

import io.glutenproject.memory.alloc.NativeMemoryAllocator;
import io.glutenproject.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.spark.util.memory.TaskMemoryResourceManager;
import org.apache.spark.util.memory.TaskMemoryResources;

/**
 * "Memory pool" for Arrow is a term especially from Arrow C++ library.
 * If you are looking for Java Arrow allocator, use
 * io.glutenproject.memory.arrow.alloc.ArrowBufferAllocators instead.
 */
public class ArrowMemoryPools {
  private ArrowMemoryPools() {

  }

  public static ArrowMemoryPool contextInstance(NativeMemoryAllocator alloc) {
    if (!TaskMemoryResources.inSparkTask()) {
      throw new UnsupportedOperationException(
          "ArrowMemoryPools#contextInstance can only be called in the context of Spark task");
    }
    String id = ArrowBufferAllocators.ArrowBufferAllocatorManager.class.toString();
    if (!TaskMemoryResources.isResourceManagerRegistered(id)) {
      TaskMemoryResources.addResourceManager(id, new ArrowMemoryPoolManager());
    }
    return ((ArrowMemoryPoolManager) TaskMemoryResources.getResourceManager(id)).managed;
  }

  public static ArrowMemoryPool wrap(NativeMemoryAllocator alloc) {
    String id = ArrowBufferAllocators.ArrowBufferAllocatorManager.class.toString()
        + alloc.getNativeInstanceId();
    if (!TaskMemoryResources.isResourceManagerRegistered(id)) {
      TaskMemoryResources.addResourceManager(id,
          new ArrowMemoryPoolManager(ArrowMemoryPool.wrapWithAllocator(alloc)));
    }
    return ((ArrowMemoryPoolManager) TaskMemoryResources.getResourceManager(id)).managed;
  }

  public static class ArrowMemoryPoolManager implements TaskMemoryResourceManager {

    public final ArrowMemoryPool managed;

    public ArrowMemoryPoolManager(ArrowMemoryPool managed) {
      this.managed = managed;
    }

    @Override
    public void release() throws Exception {
      managed.close();
    }
  }

}
