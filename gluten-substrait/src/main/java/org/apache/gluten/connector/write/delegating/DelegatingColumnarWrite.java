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

package org.apache.gluten.connector.write.delegating;

import org.apache.gluten.connector.write.ColumnarWrite;
import org.apache.gluten.extension.columnar.transition.ConventionReq;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.write.Write;

/**
 * A simple wrapper for Spark row-based {@link Write} to comply with Gluten's API
 * {@link ColumnarWrite}.
 * <p>
 * The argument `factoryCreator` will is specified for creating a columnar based
 * writer which consumes columnar batches that are of the `requiredBatchType` passed in.
 */
public class DelegatingColumnarWrite implements ColumnarWrite {
  private final Write rowBasedWrite;
  private final ConventionReq.BatchType requiredBatchType;
  private final ColumnarDataWriterFactoryCreator factoryCreator;

  public DelegatingColumnarWrite(Write rowBasedWrite,
      ConventionReq.BatchType requiredBatchType,
      ColumnarDataWriterFactoryCreator factoryCreator) {
    this.rowBasedWrite = rowBasedWrite;
    this.requiredBatchType = requiredBatchType;
    this.factoryCreator = factoryCreator;
  }

  @Override
  public String description() {
    return this.getClass().toString();
  }

  @Override
  public ConventionReq.BatchType requiredBatchType() {
    return requiredBatchType;
  }

  @Override
  public CustomMetric[] supportedCustomMetrics() {
    // By default, all the metrics defined by the delegated row-based `Write` will still be
    // used. Thus, the underlying columnar writer is expected to report the same metrics through
    // `currentMetricsValues` method of `DataWriter<ColumnarBatch>`.
    //
    // The behavior can be overridden.
    return rowBasedWrite.supportedCustomMetrics();
  }

  public ColumnarDataWriterFactoryCreator getFactoryCreator() {
    return factoryCreator;
  }
}
