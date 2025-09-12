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
package org.apache.spark.sql.delta

import org.apache.gluten.config.VeloxDelta24Config
import org.apache.gluten.exception.GlutenException

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.DeltaParquetFileFormat.{ColumnMetadata, DeletionVectorDescriptorWithFilterType, IS_ROW_DELETED_COLUMN_NAME, ROW_INDEX_COLUMN_NAME}
import org.apache.spark.sql.delta.VeloxRowIndexMarkingFilters.{VeloxDropMarkedRowsFilter, VeloxKeepMarkedRowsFilter}
import org.apache.spark.sql.delta.deletionvectors.{DropMarkedRowsFilter, KeepAllRowsFilter, KeepMarkedRowsFilter}
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ByteType, DataType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

case class VeloxDeltaParquetFileFormat(sparkDeltaFileFormat: DeltaParquetFileFormat)
  extends ParquetFileFormat {
  override def shortName(): String = sparkDeltaFileFormat.shortName()

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    sparkDeltaFileFormat.prepareWrite(sparkSession, job, options, dataSchema)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    sparkDeltaFileFormat.inferSchema(sparkSession, parameters, files)
  }

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    sparkDeltaFileFormat.supportBatch(sparkSession, schema)
  }

  override def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = {
    sparkDeltaFileFormat.vectorTypes(requiredSchema, partitionSchema, sqlConf)
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    sparkDeltaFileFormat.isSplitable(sparkSession, options, path)
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val pushdownFilters = if (sparkDeltaFileFormat.disablePushDowns) Seq.empty else filters

    val parquetDataReader: PartitionedFile => Iterator[InternalRow] =
      super.buildReaderWithPartitionValues(
        sparkSession,
        sparkDeltaFileFormat.prepareSchema(dataSchema),
        sparkDeltaFileFormat.prepareSchema(partitionSchema),
        sparkDeltaFileFormat.prepareSchema(requiredSchema),
        pushdownFilters,
        options,
        hadoopConf
      )

    val schemaWithIndices = requiredSchema.fields.zipWithIndex
    def findColumn(name: String): Option[ColumnMetadata] = {
      val results = schemaWithIndices.filter(_._1.name == name)
      if (results.length > 1) {
        throw new IllegalArgumentException(
          s"There are more than one column with name=`$name` requested in the reader output")
      }
      results.headOption.map(e => ColumnMetadata(e._2, e._1))
    }
    val isRowDeletedColumn = findColumn(IS_ROW_DELETED_COLUMN_NAME)
    val rowIndexColumn = findColumn(ROW_INDEX_COLUMN_NAME)

    if (isRowDeletedColumn.isEmpty && rowIndexColumn.isEmpty) {
      return parquetDataReader // no additional metadata is needed.
    } else {
      // verify the file splitting and filter pushdown are disabled. The new additional
      // metadata columns cannot be generated with file splitting and filter pushdowns
      require(
        !sparkDeltaFileFormat.isSplittable,
        "Cannot generate row index related metadata with file splitting")
      require(
        sparkDeltaFileFormat.disablePushDowns,
        "Cannot generate row index related metadata with filter pushdown")
    }

    if (sparkDeltaFileFormat.hasDeletionVectorMap && isRowDeletedColumn.isEmpty) {
      throw new IllegalArgumentException(
        s"Expected a column $IS_ROW_DELETED_COLUMN_NAME in the schema")
    }

    val useOffHeapBuffers = sparkSession.sessionState.conf.offHeapColumnVectorEnabled
    (partitionedFile: PartitionedFile) => {
      val rowIteratorFromParquet = parquetDataReader(partitionedFile)
      val iterToReturn =
        iteratorWithAdditionalMetadataColumns(
          partitionedFile,
          rowIteratorFromParquet,
          isRowDeletedColumn,
          useOffHeapBuffers = useOffHeapBuffers,
          rowIndexColumn = rowIndexColumn)
      iterToReturn.asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def supportDataType(dataType: DataType): Boolean = {
    sparkDeltaFileFormat.supportDataType(dataType)
  }

  private def createRowIndexFilter(
      dvDescriptor: DeletionVectorDescriptorWithFilterType): RowIndexFilter = {
    (VeloxDelta24Config.get.deltaNativeDvEnabled(), dvDescriptor.filterType) match {
      case (false, RowIndexFilterType.IF_CONTAINED) =>
        DropMarkedRowsFilter.createInstance(
          dvDescriptor.descriptor,
          sparkDeltaFileFormat.broadcastHadoopConf.get.value.value,
          sparkDeltaFileFormat.tablePath.map(new Path(_)))
      case (false, RowIndexFilterType.IF_NOT_CONTAINED) =>
        KeepMarkedRowsFilter.createInstance(
          dvDescriptor.descriptor,
          sparkDeltaFileFormat.broadcastHadoopConf.get.value.value,
          sparkDeltaFileFormat.tablePath.map(new Path(_)))
      case (true, RowIndexFilterType.IF_CONTAINED) =>
        VeloxDropMarkedRowsFilter.createInstance(
          dvDescriptor.descriptor,
          sparkDeltaFileFormat.broadcastHadoopConf.get.value.value,
          sparkDeltaFileFormat.tablePath.map(new Path(_)))
      case (true, RowIndexFilterType.IF_NOT_CONTAINED) =>
        VeloxKeepMarkedRowsFilter.createInstance(
          dvDescriptor.descriptor,
          sparkDeltaFileFormat.broadcastHadoopConf.get.value.value,
          sparkDeltaFileFormat.tablePath.map(new Path(_)))
      case _ =>
        throw new GlutenException("Unknown row-index filter type")
    }
  }

  private def iteratorWithAdditionalMetadataColumns(
      partitionedFile: PartitionedFile,
      iterator: Iterator[Object],
      isRowDeletedColumn: Option[ColumnMetadata],
      rowIndexColumn: Option[ColumnMetadata],
      useOffHeapBuffers: Boolean): Iterator[Object] = {
    val pathUri = partitionedFile.pathUri

    val rowIndexFilter = isRowDeletedColumn.map {
      col =>
        // Fetch the DV descriptor from the broadcast map and create a row index filter
        sparkDeltaFileFormat.broadcastDvMap.get.value
          .get(pathUri)
          .map(dvDescriptor => createRowIndexFilter(dvDescriptor))
          .getOrElse(KeepAllRowsFilter)
    }

    val metadataColumns = Seq(isRowDeletedColumn, rowIndexColumn).filter(_.nonEmpty).map(_.get)

    // Unfortunately there is no way to verify the Parquet index is starting from 0.
    // We disable the splits, so the assumption is ParquetFileFormat respects that
    var rowIndex: Long = 0

    // Used only when non-column row batches are received from the Parquet reader
    val tempVector = new OnHeapColumnVector(1, ByteType)

    iterator.map {
      row =>
        row match {
          case batch: ColumnarBatch => // When vectorized Parquet reader is enabled
            val size = batch.numRows()
            // Create vectors for all needed metadata columns.
            // We can't use the one from Parquet reader as it set the
            // [[WritableColumnVector.isAllNulls]] to true and it can't be reset with using any
            // public APIs.
            trySafely(useOffHeapBuffers, size, metadataColumns) {
              writableVectors =>
                val indexVectorTuples = new ArrayBuffer[(Int, ColumnVector)]
                var index = 0
                isRowDeletedColumn.foreach {
                  columnMetadata =>
                    val isRowDeletedVector = writableVectors(index)
                    rowIndexFilter.get
                      .materializeIntoVector(rowIndex, rowIndex + size, isRowDeletedVector)
                    indexVectorTuples += (columnMetadata.index -> isRowDeletedVector)
                    index += 1
                }

                rowIndexColumn.foreach {
                  columnMetadata =>
                    val rowIndexVector = writableVectors(index)
                    // populate the row index column value
                    for (i <- 0 until size) {
                      rowIndexVector.putLong(i, rowIndex + i)
                    }

                    indexVectorTuples += (columnMetadata.index -> rowIndexVector)
                    index += 1
                }

                val newBatch = replaceVectors(batch, indexVectorTuples.toSeq: _*)
                rowIndex += size
                newBatch
            }

          case rest: InternalRow => // When vectorized Parquet reader is disabled
            // Temporary vector variable used to get DV values from RowIndexFilter
            // Currently the RowIndexFilter only supports writing into a columnar vector
            // and doesn't have methods to get DV value for a specific row index.
            // TODO: This is not efficient, but it is ok given the default reader is vectorized
            // reader and this will be temporary until Delta upgrades to Spark with Parquet
            // reader that automatically generates the row index column.
            isRowDeletedColumn.foreach {
              columnMetadata =>
                rowIndexFilter.get.materializeIntoVector(rowIndex, rowIndex + 1, tempVector)
                rest.setLong(columnMetadata.index, tempVector.getByte(0))
            }

            rowIndexColumn.foreach(columnMetadata => rest.setLong(columnMetadata.index, rowIndex))
            rowIndex += 1
            rest
          case others =>
            throw new RuntimeException(
              s"Parquet reader returned an unknown row type: ${others.getClass.getName}")
        }
    }
  }

  /** Try the operation, if the operation fails release the created resource */
  private def trySafely[R <: WritableColumnVector, T](
      useOffHeapBuffers: Boolean,
      size: Int,
      columns: Seq[ColumnMetadata])(f: Seq[WritableColumnVector] => T): T = {
    val resources = new ArrayBuffer[WritableColumnVector](columns.size)
    try {
      columns.foreach(col => resources.append(newVector(useOffHeapBuffers, size, col.structField)))
      f(resources.toSeq)
    } catch {
      case NonFatal(e) =>
        resources.foreach(closeQuietly(_))
        throw e
    }
  }

  /** Utility method to create a new writable vector */
  private def newVector(
      useOffHeapBuffers: Boolean,
      size: Int,
      dataType: StructField): WritableColumnVector = {
    if (useOffHeapBuffers) {
      OffHeapColumnVector.allocateColumns(size, Seq(dataType).toArray)(0)
    } else {
      OnHeapColumnVector.allocateColumns(size, Seq(dataType).toArray)(0)
    }
  }

  private def closeQuietly(closeable: AutoCloseable): Unit = {
    if (closeable != null) {
      try {
        closeable.close()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  private def replaceVectors(
      batch: ColumnarBatch,
      indexVectorTuples: (Int, ColumnVector)*): ColumnarBatch = {
    val vectors = ArrayBuffer[ColumnVector]()
    for (i <- 0 until batch.numCols()) {
      var replaced: Boolean = false
      for (indexVectorTuple <- indexVectorTuples) {
        val index = indexVectorTuple._1
        val vector = indexVectorTuple._2
        if (indexVectorTuple._1 == i) {
          vectors += indexVectorTuple._2
          // Make sure to close the existing vector allocated in the Parquet
          batch.column(i).close()
          replaced = true
        }
      }
      if (!replaced) {
        vectors += batch.column(i)
      }
    }
    new ColumnarBatch(vectors.toArray, batch.numRows())
  }
}
