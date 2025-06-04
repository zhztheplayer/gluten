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

package org.apache.gluten.connector.write.simple;

import org.apache.gluten.connector.write.ColumnarBatchWrite;
import org.apache.gluten.connector.write.ColumnarDataWriterFactory;
import org.apache.gluten.exception.GlutenException;
import org.apache.gluten.execution.BatchCarrierRow;
import org.apache.gluten.extension.columnar.transition.ConventionReq;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Function1;

import java.io.IOException;

public class SimpleColumnarBatchWrite implements ColumnarBatchWrite {
  private final BatchWrite rowBasedBatchWrite;
  private final SimpleColumnarWrite parent;

  public SimpleColumnarBatchWrite(BatchWrite rowBasedBatchWrite, SimpleColumnarWrite parent) {
    this.rowBasedBatchWrite = rowBasedBatchWrite;
    this.parent = parent;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    final ColumnarDataWriterFactory columnarDataWriterFactory = parent.getFactoryCreator().create(parent, this, info);
    return new DataWriterFactory() {
      @Override
      public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        final DataWriter<ColumnarBatch> columnarWriter = columnarDataWriterFactory.createColumnarWriter(partitionId, taskId);
        return new DataWriter<InternalRow>() {

          @Override
          public void close() throws IOException {
            columnarWriter.close();
          }

          @Override
          public void write(InternalRow record) throws IOException {
            BatchCarrierRow.unwrap(record).foreach(new Function1<ColumnarBatch, Object>() {
              @Override
              public Object apply(ColumnarBatch cb) {
                try {
                  columnarWriter.write(cb);
                  return null;
                } catch (IOException e) {
                  throw new GlutenException(e);
                }
              }
            });
          }

          @Override
          public WriterCommitMessage commit() throws IOException {
            return columnarWriter.commit();
          }

          @Override
          public void abort() throws IOException {
            columnarWriter.abort();
          }
        };
      }
    };
  }

  @Override
  public boolean useCommitCoordinator() {
    return rowBasedBatchWrite.useCommitCoordinator();
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {
    rowBasedBatchWrite.onDataWriterCommit(message);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    rowBasedBatchWrite.commit(messages);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    rowBasedBatchWrite.abort(messages);
  }

  @Override
  public ConventionReq.BatchType requiredBatchType() {
    return parent.requiredBatchType();
  }
}
