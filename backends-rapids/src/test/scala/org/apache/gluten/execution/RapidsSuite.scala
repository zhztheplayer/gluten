package org.apache.gluten.execution

import org.apache.gluten.utils.Arm

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession

import java.io.File

import scala.io.Source

class RapidsSuite extends SharedSparkSession {
  import RapidsSuite._
  private val tpchQueries: String =
    getClass
      .getResource("/")
      .getPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"
  private val dataDirPath: String =
    getClass
      .getResource("/tpch-data-parquet")
      .getFile

  private val tpchTables: Seq[Table] = Seq(
    Table("part", partitionColumns = "p_brand" :: Nil),
    Table("supplier", partitionColumns = Nil),
    Table("partsupp", partitionColumns = Nil),
    Table("customer", partitionColumns = "c_mktsegment" :: Nil),
    Table("orders", partitionColumns = "o_orderdate" :: Nil),
    Table("lineitem", partitionColumns = "l_shipdate" :: Nil),
    Table("nation", partitionColumns = Nil),
    Table("region", partitionColumns = Nil)
  )

  private def createTpchTables(spark: SparkSession): Unit = {
    tpchTables
      .map(_.name)
      .map {
        table =>
          val tablePath = new File(dataDirPath, table).getAbsolutePath
          val tableDF = spark.read.format("parquet").load(tablePath)
          tableDF.createOrReplaceTempView(table)
          (table, tableDF)
      }
      .toMap
  }

  private def tpchSQL(queryId: String): String =
    Arm.withResource(Source.fromFile(new File(s"$tpchQueries/$queryId.sql"), "UTF-8"))(_.mkString)

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.sql.adaptive.enabled", "false")
  }

  test("sanity") {
    createTpchTables(spark)
    val df = spark.sql("select * from lineitem limit 10")
    df.explain()
  }

  test("q1 Velox + Rapids") {
    createTpchTables(spark)
    withSQLConf(
      "spark.rapids.sql.format.parquet.enabled" -> "false",
      "spark.rapids.sql.hashAgg.replaceMode" -> "final") {
      val df = spark.sql(tpchSQL("q1"))
      df.explain()
    }
  }
}

object RapidsSuite {
  case class Table(name: String, partitionColumns: Seq[String])
}
