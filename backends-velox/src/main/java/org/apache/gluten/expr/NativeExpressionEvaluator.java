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

package org.apache.gluten.expr;

import io.substrait.proto.Expression;
import io.substrait.proto.Type;
import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.columnarbatch.ColumnarBatches;
import org.apache.gluten.runtime.Runtime;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class NativeExpressionEvaluator {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeExpressionEvaluator.class);
  private static final AtomicInteger id = new AtomicInteger(0);

  private final Runtime runtime;
  private final ExpressionEvaluatorJniWrapper jniWrapper;
  private final long evaluatorHandle;

  public NativeExpressionEvaluator(Runtime runtime) {
    this.runtime = runtime;
    this.jniWrapper = ExpressionEvaluatorJniWrapper.create(runtime);
    evaluatorHandle = this.jniWrapper.nativeCreateEvaluator();
  }

  public NativeExprSet createExprSet(Expression expr, Type inputSchema, String[] substraitFunctionArray) {
    final long exprSetHandle = jniWrapper.nativeCreateExprSet(evaluatorHandle, expr.toByteArray(), inputSchema.toByteArray(), substraitFunctionArray);
    return new NativeExprSet(exprSetHandle);
  }

  public ColumnarBatch evaluate(NativeExprSet exprSet, ColumnarBatch batch) {
    final long outBatchHandle = jniWrapper.nativeEvaluate(
        evaluatorHandle,
        exprSet.getHandle(),
        ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), batch));
    return ColumnarBatches.create(outBatchHandle);
  }

}
