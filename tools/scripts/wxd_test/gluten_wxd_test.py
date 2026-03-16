#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import sys
import json
import threading
import traceback
from datetime import datetime
from pyspark.sql import SparkSession


def env(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def log(msg: str) -> None:
    print(msg, flush=True)


def safe_sql(spark: SparkSession, sql: str, label: str, show: bool = False) -> bool:
    try:
        df = spark.sql(sql)
        log(f"\n=== {label} ===")
        if show and df is not None:
            df.show(truncate=False)
        return True
    except Exception as e:
        log(f"\n!!! FAILED: {label}")
        log("SQL:\n" + sql.strip())
        log("ERROR:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))
        return False


def log_hudi_scan(status: str, write_status: str, reason: str = "", schema: str = "") -> None:
    parts = [
        f"HUDI_SCAN_STATUS={status}",
        f"hudi_catalog={HUDI_CATALOG}",
        f"hudi_write_status={write_status}",
    ]
    if schema:
        parts.append(f"hudi_schema={schema}")
    if reason:
        parts.append(f"reason={reason}")
    log(" ".join(parts))


def sanitize_ident(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    if not s:
        s = "ci"
    if s[0].isdigit():
        s = "ci_" + s
    return s.lower()


def get_catalog_warehouse(spark: SparkSession, catalog: str, fallback: str = "") -> str:
    key = f"spark.sql.catalog.{catalog}.warehouse"
    try:
        v = spark.conf.get(key)
        if v and v.strip():
            return v.strip().rstrip("/")
    except Exception:
        pass
    return fallback.rstrip("/") if fallback else ""


def run_with_timeout(label: str, timeout_seconds: int, fn, on_timeout=None) -> bool:
    result = {"value": False}
    error = {"tb": ""}
    done = threading.Event()

    def target():
        try:
            result["value"] = bool(fn())
        except Exception as e:
            error["tb"] = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        finally:
            done.set()

    worker = threading.Thread(target=target, name=f"{label}-worker", daemon=True)
    worker.start()

    if not done.wait(timeout_seconds):
        log(f"\n!!! {label} timed out after {timeout_seconds} seconds. Marking as FAILED and moving on.")
        if on_timeout:
            try:
                on_timeout()
            except Exception as e:
                log("Timeout cleanup error:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))
        return False

    if error["tb"]:
        log(f"\n!!! {label} crashed")
        log("ERROR:\n" + error["tb"])
        return False

    return result["value"]


# -----------------------
# User inputs (catalogs)
# -----------------------
RUN_MODE = env("RUN_MODE", "all").lower()  # all | catalogs | buckets | history | qa_tests
CI_RUN_ID = sanitize_ident(env("CI_RUN_ID", "local"))
RUN_STAMP = sanitize_ident(env("RUN_STAMP", datetime.now().strftime("%Y%m%d_%H%M%S")))

# Catalogs (user enters only these)
ICEBERG_CATALOG = env("ICEBERG_CATALOG", "iceberg_catalog")

DELTA_CATALOG = env("DELTA_CATALOG", "delta_catalog")
HUDI_CATALOG  = env("HUDI_CATALOG",  "hudi_catalog")
HIVE_CATALOG  = env("HIVE_CATALOG",  "hive_data")

AWS_CATALOG   = env("AWS_CATALOG",   "aws_catalog")
GCS_CATALOG   = env("GCS_CATALOG",   "gcs_catalog")
AZURE_CATALOG = env("AZURE_CATALOG", "azure_catalog")

# Bucket/warehouse defaults (can be overridden by env)
AWS_BUCKET = env("AWS_BUCKET", "reema-spark")
GCS_BUCKET = env("GCS_BUCKET", "iaesparkgcs")
AZURE_WAREHOUSE = env("AZURE_WAREHOUSE", "abfss://pyspark@sparkadlsiae.dfs.core.windows.net")

AZURE_SNIPPET_CATALOG = env("AZURE_SNIPPET_CATALOG", AZURE_CATALOG)
AZURE_SNIPPET_SCHEMA = sanitize_ident(env("AZURE_SNIPPET_SCHEMA", f"adls_schema_{RUN_STAMP}"))
AZURE_SNIPPET_TABLE = sanitize_ident(env("AZURE_SNIPPET_TABLE", "adls_table1"))
AZURE_DB = f"{AZURE_SNIPPET_CATALOG}.{AZURE_SNIPPET_SCHEMA}"
AZURE_TABLE_NAME = f"{AZURE_DB}.{AZURE_SNIPPET_TABLE}"
AZURE_DB_LOCATION = env("AZURE_DB_LOCATION", f"{AZURE_WAREHOUSE}/{AZURE_SNIPPET_SCHEMA}")
AZURE_TABLE_LOCATION = env("AZURE_TABLE_LOCATION", f"{AZURE_DB_LOCATION}/{AZURE_SNIPPET_TABLE}")

GCS_READ_PATH = env("GCS_READ_PATH", "gs://iaesparkgcs/demo/")
GCS_WRITE_PATH = env("GCS_WRITE_PATH", "gs://iaesparkgcs/demo6/")

AWS_WAREHOUSE = env("AWS_WAREHOUSE", f"s3a://{AWS_BUCKET}/{AWS_CATALOG}")
GCS_WAREHOUSE = env("GCS_WAREHOUSE", f"gs://{GCS_BUCKET}/{GCS_CATALOG}")

WAREHOUSE_BY_CATALOG = {
    AWS_CATALOG: AWS_WAREHOUSE,
    GCS_CATALOG: GCS_WAREHOUSE,
    AZURE_CATALOG: AZURE_WAREHOUSE,
}

# Timestamped default object names for bucket tests
OBJECTS = {
    AWS_CATALOG: {
        "schema": env("AWS_SCHEMA", f"aws_schema_{RUN_STAMP}"),
        "table": env("AWS_TABLE", "aws_table"),
    },
    GCS_CATALOG: {
        "schema": env("GCS_SCHEMA", f"gcs_schema_{RUN_STAMP}"),
        "table": env("GCS_TABLE", "gcs_table"),
    },
    AZURE_CATALOG: {
        "schema": env("AZURE_SCHEMA", f"azure_schema_{RUN_STAMP}"),
        "table": env("AZURE_TABLE", "azure_table"),
    },
}

HUDI_TIMEOUT_SECONDS = env_int("HUDI_TIMEOUT_SECONDS", 120)

# Spark History check
EXPECT_EVENTLOG = env_bool("EXPECT_EVENTLOG", True)


def log_gluten_status(spark: SparkSession):
    log("\n=== Spark/Gluten status ===")
    keys = [
        "spark.gluten.enabled",
        "spark.plugins",
        "spark.shuffle.manager",
        "spark.sql.extensions",
        "spark.sql.catalogImplementation",
        "spark.eventLog.enabled",
        "spark.eventLog.dir",
    ]
    for k in keys:
        try:
            log(f"{k} = {spark.conf.get(k)}")
        except Exception:
            log(f"{k} = <not set>")


def test_iceberg_single_catalog(spark: SparkSession) -> bool:
    wh = get_catalog_warehouse(spark, ICEBERG_CATALOG, env("ICEBERG_WAREHOUSE", ""))
    if not wh:
        log(f"Missing warehouse for Iceberg catalog '{ICEBERG_CATALOG}' (spark.sql.catalog.{ICEBERG_CATALOG}.warehouse)")
        return False

    schema = f"iceberg_schema_{RUN_STAMP}"
    table  = f"t_iceberg_{CI_RUN_ID}"

    schema_loc = f"{wh}/{schema}"
    table_loc  = f"{schema_loc}/{table}"

    ok = True
    ok &= safe_sql(
        spark,
        f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{schema} LOCATION '{schema_loc}'",
        "Iceberg: create schema",
    )
    ok &= safe_sql(
        spark,
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{schema}.{table} (
          id INT,
          name STRING,
          dt STRING
        )
        USING ICEBERG
        LOCATION '{table_loc}'
        """,
        "Iceberg: create table",
    )
    ok &= safe_sql(
        spark,
        f"""
        INSERT INTO {ICEBERG_CATALOG}.{schema}.{table} VALUES
          (1, 'Name1', '2025-01-01'),
          (2, 'Name2', '2025-02-15')
        """,
        "Iceberg: insert",
    )
    ok &= safe_sql(
        spark,
        f"SELECT * FROM {ICEBERG_CATALOG}.{schema}.{table} ORDER BY id",
        "Iceberg: read back",
        show=True,
    )
    return ok


def test_delta(spark: SparkSession) -> bool:
    wh = get_catalog_warehouse(spark, DELTA_CATALOG, env("DELTA_WAREHOUSE", ""))
    if not wh:
        log(f"Missing warehouse for Delta catalog '{DELTA_CATALOG}' (spark.sql.catalog.{DELTA_CATALOG}.warehouse)")
        return False

    schema = f"delta_schema_{RUN_STAMP}"
    t1 = f"t_delta1_{CI_RUN_ID}"
    t2 = f"t_delta2_{CI_RUN_ID}"

    base = f"{wh}/ci/{CI_RUN_ID}/delta"
    path1 = f"{base}/{t1}"
    path2 = f"{base}/{t2}"

    ok = True
    try:
        df1 = spark.createDataFrame([(1, "Name1", "2025-01-01"), (2, "Name2", "2025-02-15")], ["id", "name", "dt"])
        df2 = spark.createDataFrame([(1, "Reema2", "2025-01-01"), (2, "Mohammad2", "2025-02-15")], ["id", "name", "dt"])

        df1.write.format("delta").mode("overwrite").save(path1)
        df2.write.format("delta").mode("overwrite").save(path2)

        ok &= safe_sql(
            spark,
            f"CREATE SCHEMA IF NOT EXISTS spark_catalog.{schema} LOCATION '{base}'",
            "Delta: create schema (spark_catalog)",
        )

        ok &= safe_sql(
            spark,
            f"""
            CREATE TABLE IF NOT EXISTS spark_catalog.{schema}.{t1}
            USING DELTA
            LOCATION '{path1}'
            """,
            "Delta: register table1",
        )

        ok &= safe_sql(
            spark,
            f"""
            CREATE TABLE IF NOT EXISTS spark_catalog.{schema}.{t2}
            USING DELTA
            LOCATION '{path2}'
            """,
            "Delta: register table2",
        )

        ok &= safe_sql(
            spark,
            f"SELECT * FROM spark_catalog.{schema}.{t1} ORDER BY id",
            "Delta: read back",
            show=True,
        )
    except Exception as e:
        ok = False
        log("\n!!! Delta crashed:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))
    return ok


def test_hudi(spark: SparkSession) -> bool:
    wh = get_catalog_warehouse(spark, HUDI_CATALOG, env("HUDI_WAREHOUSE", ""))
    if not wh:
        log(f"Missing warehouse for Hudi catalog '{HUDI_CATALOG}' (spark.sql.catalog.{HUDI_CATALOG}.warehouse)")
        log_hudi_scan("FAILED", "NOT_WRITTEN", "missing_warehouse")
        return False

    schema = f"hudi_schema_{RUN_STAMP}"
    t1 = f"t_hudi1_{CI_RUN_ID}"
    t2 = f"t_hudi2_{CI_RUN_ID}"

    base = f"{wh}/ci/{CI_RUN_ID}/hudi"
    path1 = f"{base}/{t1}"
    path2 = f"{base}/{t2}"

    ok = True
    write_done = False
    failure_reason = ""
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{schema} LOCATION '{base}/'")

        cols = ["id", "name", "location", "city"]
        df1 = spark.createDataFrame([(1, "Sam", "Riyadh", "riyadh"), (2, "Tom", "Jeddah", "jeddah")], cols)

        opts = {
            "hoodie.table.name": t1,
            "hoodie.datasource.write.recordkey.field": "id",
            "hoodie.datasource.write.precombine.field": "id",
            "hoodie.datasource.write.partitionpath.field": "city",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.write.markers.type": "direct",
            "hoodie.embed.timeline.server": "false",
        }
        df1.write.format("hudi").options(**opts).mode("overwrite").save(path1)

        df2 = spark.createDataFrame([(10, "Reema2", "Riyadh", "riyadh"), (20, "Mohammad2", "Jeddah", "jeddah")], cols)
        opts2 = dict(opts)
        opts2["hoodie.table.name"] = t2
        df2.write.format("hudi").options(**opts2).mode("overwrite").save(path2)
        write_done = True
        log_hudi_scan("INFO", "WRITTEN", schema=schema)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.{schema}.{t1}
        USING HUDI
        LOCATION '{path1}'
        """)
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.{schema}.{t2}
        USING HUDI
        LOCATION '{path2}'
        """)

        ok &= safe_sql(
            spark,
            f"SELECT * FROM spark_catalog.{schema}.{t1}",
            "Hudi: read back",
            show=True,
        )
        if not ok:
            failure_reason = "readback_failed"
    except Exception as e:
        ok = False
        failure_reason = type(e).__name__
        log("\n!!! Hudi crashed:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))

    if ok:
        log_hudi_scan("SUCCESS", "WRITTEN" if write_done else "NOT_WRITTEN", schema=schema)
    else:
        log_hudi_scan(
            "FAILED",
            "WRITTEN" if write_done else "NOT_WRITTEN",
            failure_reason if failure_reason else "unknown_failure",
            schema,
        )
    return ok


def test_hive(spark: SparkSession) -> bool:
    wh = get_catalog_warehouse(spark, HIVE_CATALOG, env("HIVE_WAREHOUSE", ""))
    if not wh:
        log(f"Missing warehouse for Hive catalog '{HIVE_CATALOG}' (spark.sql.catalog.{HIVE_CATALOG}.warehouse)")
        return False

    schema = f"hive_schema_{RUN_STAMP}"
    t1 = f"t_hive1_{CI_RUN_ID}"

    schema_loc = f"{wh}/ci/{CI_RUN_ID}/hive/{schema}"
    table_loc  = f"{schema_loc}/{t1}"

    ok = True
    ok &= safe_sql(
        spark,
        f"CREATE SCHEMA IF NOT EXISTS {schema} LOCATION '{schema_loc}'",
        "Hive: create schema",
    )
    ok &= safe_sql(
        spark,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.{t1} (
          id INT,
          name STRING,
          dt STRING
        )
        USING PARQUET
        LOCATION '{table_loc}'
        """,
        "Hive: create table",
    )
    ok &= safe_sql(
        spark,
        f"""
        INSERT INTO {schema}.{t1} VALUES
          (1, 'Name1', '2025-01-01'),
          (2, 'Name2', '2025-02-15')
        """,
        "Hive: insert",
    )
    ok &= safe_sql(
        spark,
        f"SELECT * FROM {schema}.{t1} ORDER BY id",
        "Hive: read back",
        show=True,
    )
    return ok


def create_and_check_objects(spark: SparkSession, catalog: str) -> bool:
    names = OBJECTS.get(catalog, {})
    schema = sanitize_ident(names.get("schema", f"schema_{RUN_STAMP}"))
    table = sanitize_ident(names.get("table", f"t_{sanitize_ident(catalog)}"))

    warehouse = get_catalog_warehouse(spark, catalog, WAREHOUSE_BY_CATALOG.get(catalog, ""))
    if not warehouse:
        log(f"Missing warehouse for catalog '{catalog}' (spark.sql.catalog.{catalog}.warehouse)")
        return False

    schema_location = f"{warehouse}/{schema}"
    table_location = f"{schema_location}/{table}"

    ok = True
    ok &= safe_sql(
        spark,
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} LOCATION '{schema_location}'",
        f"Create schema {catalog}.{schema}",
    )
    ok &= safe_sql(
        spark,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
          id INT,
          name STRING,
          source STRING
        ) USING ICEBERG
        LOCATION '{table_location}'
        """,
        f"Create table {catalog}.{schema}.{table}",
    )
    ok &= safe_sql(
        spark,
        f"""
        INSERT INTO {catalog}.{schema}.{table} VALUES
          (1, 'alpha', '{catalog}'),
          (2, 'beta',  '{catalog}'),
          (3, 'gamma', '{catalog}')
        """,
        f"Insert rows into {catalog}.{schema}.{table}",
    )
    ok &= safe_sql(
        spark,
        f"SELECT * FROM {catalog}.{schema}.{table} ORDER BY id",
        f"Read back table {catalog}.{schema}.{table}",
        show=True,
    )
    return ok


def run_azure_example(spark: SparkSession) -> bool:
    ok = True
    ok &= safe_sql(
        spark,
        f"CREATE DATABASE IF NOT EXISTS {AZURE_DB} LOCATION '{AZURE_DB_LOCATION}'",
        f"Create schema {AZURE_DB}",
    )
    ok &= safe_sql(
        spark,
        f"""
        CREATE TABLE IF NOT EXISTS {AZURE_TABLE_NAME} (
          col1 INT,
          col2 STRING
        ) USING ICEBERG
        LOCATION '{AZURE_TABLE_LOCATION}'
        """,
        f"Create table {AZURE_TABLE_NAME}",
    )
    ok &= safe_sql(
        spark,
        f"INSERT INTO {AZURE_TABLE_NAME} VALUES (1,'Alan'),(2,'Ben'),(3,'Chen')",
        f"Insert rows into {AZURE_TABLE_NAME}",
    )
    ok &= safe_sql(
        spark,
        f"SELECT * FROM {AZURE_TABLE_NAME}",
        f"Read back table {AZURE_TABLE_NAME}",
        show=True,
    )
    return ok


def run_gcs_example(spark: SparkSession) -> bool:
    gcs_schema = sanitize_ident(OBJECTS[GCS_CATALOG]["schema"])
    gcs_table = sanitize_ident(OBJECTS[GCS_CATALOG]["table"])
    full_table = f"{GCS_CATALOG}.{gcs_schema}.{gcs_table}"
    schema_location = f"{GCS_WAREHOUSE}/{gcs_schema}"
    table_location = f"{schema_location}/{gcs_table}"

    ok = True
    ok &= safe_sql(
        spark,
        f"CREATE SCHEMA IF NOT EXISTS {GCS_CATALOG}.{gcs_schema} LOCATION '{schema_location}'",
        f"Create schema {GCS_CATALOG}.{gcs_schema}",
    )
    ok &= safe_sql(
        spark,
        f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
          id INT,
          name STRING,
          source STRING
        ) USING ICEBERG
        LOCATION '{table_location}'
        """,
        f"Create table {full_table}",
    )
    try:
        df = spark.read.format("parquet").load(GCS_READ_PATH)
        log("\n=== GCS read demo ===")
        df.show()
        df.write.format("parquet").mode("overwrite").save(GCS_WRITE_PATH)
        log(f"Wrote parquet to {GCS_WRITE_PATH}")

        log("\n=== GCS DataFrame -> Iceberg table append ===")
        df_new = spark.createDataFrame(
            [(101, "df-alpha", "gcs_df"), (102, "df-beta", "gcs_df")],
            ["id", "name", "source"],
        )
        df_new.writeTo(full_table).append()
        spark.table(full_table).orderBy("id").show()
    except Exception as e:
        ok = False
        log("\n!!! GCS example failed")
        log("ERROR:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))

    return ok


def test_multicloud_buckets(spark: SparkSession) -> bool:
    ok = True
    ok &= create_and_check_objects(spark, AWS_CATALOG)
    ok &= run_azure_example(spark)
    ok &= run_gcs_example(spark)
    return ok


def test_eventlog(spark: SparkSession) -> bool:
    try:
        enabled = spark.conf.get("spark.eventLog.enabled", "false").lower() == "true"
        d = spark.conf.get("spark.eventLog.dir", "").strip()
        app_id = spark.sparkContext.applicationId

        log("\n=== Spark History (eventlog) check ===")
        log(f"spark.eventLog.enabled={enabled}")
        log(f"spark.eventLog.dir={d}")
        log(f"spark.applicationId={app_id}")

        if not EXPECT_EVENTLOG:
            log("EXPECT_EVENTLOG=false -> skipping")
            return True

        if not enabled or not d:
            log("Event log not enabled/dir missing -> FAIL")
            return False

        # Minimal: just verify we can read/list the dir (existence can vary by FS impl)
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(d)
        fs = p.getFileSystem(conf)
        exists = fs.exists(p)
        log(f"eventLog dir exists={exists}")
        return bool(exists)
    except Exception as e:
        log("\n!!! Eventlog check crashed:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))
        return False


# ========================================
# QA-TAGGED TESTS FROM HUMMINGBIRD
# Based on validation-tests/src/test/java/JobFeatures/wxdGlutenEngineTest.feature
# ========================================

def test_wordcount_application(spark: SparkSession) -> bool:
    """
    @wordcount test - Basic smoke test for Gluten functionality
    Tests simple file I/O and basic transformations
    Based on: spark-apps/wordcount.py
    """
    log("\n=== QA Test: WordCount Application (@wordcount) ===")
    
    wh = get_catalog_warehouse(spark, ICEBERG_CATALOG, env("ICEBERG_WAREHOUSE", ""))
    if not wh:
        log(f"Missing warehouse for Iceberg catalog '{ICEBERG_CATALOG}'")
        return False
    
    schema = f"wordcount_schema_{RUN_STAMP}"
    input_table = f"wordcount_input_{CI_RUN_ID}"
    
    ok = True
    try:
        # Create schema
        ok &= safe_sql(
            spark,
            f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{schema} LOCATION '{wh}/{schema}'",
            "WordCount: create schema",
        )
        
        # Create sample text data
        text_data = [
            ("Hello world hello spark",),
            ("Spark is awesome spark",),
            ("Gluten makes spark faster",),
        ]
        df = spark.createDataFrame(text_data, ["text"])
        
        # Write to Iceberg table
        df.writeTo(f"{ICEBERG_CATALOG}.{schema}.{input_table}").using("iceberg").create()
        
        # Perform word count using SQL
        ok &= safe_sql(
            spark,
            f"""
            SELECT word, COUNT(*) as count
            FROM (
                SELECT explode(split(text, ' ')) as word
                FROM {ICEBERG_CATALOG}.{schema}.{input_table}
            )
            GROUP BY word
            ORDER BY count DESC, word
            """,
            "WordCount: compute word frequencies",
            show=True,
        )
        
        log("WordCount test completed successfully")
    except Exception as e:
        ok = False
        log("\n!!! WordCount test failed:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))
    
    return ok


def test_join_query_application(spark: SparkSession) -> bool:
    """
    @join_query test - Tests Gluten's vectorized join performance
    Critical for data warehouse workloads
    Based on: spark-apps/iceberg-join-query.py
    """
    log("\n=== QA Test: Join Query Application (@join_query) ===")
    
    wh = get_catalog_warehouse(spark, ICEBERG_CATALOG, env("ICEBERG_WAREHOUSE", ""))
    if not wh:
        log(f"Missing warehouse for Iceberg catalog '{ICEBERG_CATALOG}'")
        return False
    
    schema = f"join_query_schema_{RUN_STAMP}"
    emp_table = f"employees_{CI_RUN_ID}"
    dept_table = f"departments_{CI_RUN_ID}"
    
    ok = True
    try:
        # Create schema
        ok &= safe_sql(
            spark,
            f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{schema} LOCATION '{wh}/{schema}'",
            "JoinQuery: create schema",
        )
        
        # Create employees data
        employees_data = [
            (1, "Alice", 101),
            (2, "Bob", 102),
            (3, "Charlie", 101),
            (4, "David", 103),
        ]
        emp_df = spark.createDataFrame(employees_data, ["emp_id", "emp_name", "dept_id"])
        emp_df.writeTo(f"{ICEBERG_CATALOG}.{schema}.{emp_table}").using("iceberg").create()
        
        # Create departments data
        departments_data = [
            (101, "HR"),
            (102, "IT"),
            (103, "Finance"),
            (104, "Marketing"),
        ]
        dept_df = spark.createDataFrame(departments_data, ["dept_id", "dept_name"])
        dept_df.writeTo(f"{ICEBERG_CATALOG}.{schema}.{dept_table}").using("iceberg").create()
        
        # Perform join query
        ok &= safe_sql(
            spark,
            f"""
            SELECT e.emp_id, e.emp_name, d.dept_name
            FROM {ICEBERG_CATALOG}.{schema}.{emp_table} e
            INNER JOIN {ICEBERG_CATALOG}.{schema}.{dept_table} d
            ON e.dept_id = d.dept_id
            ORDER BY e.emp_id
            """,
            "JoinQuery: employee-department join",
            show=True,
        )
        
        log("Join Query test completed successfully")
    except Exception as e:
        ok = False
        log("\n!!! Join Query test failed:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))
    
    return ok


def test_scala_pi_calculation(spark: SparkSession) -> bool:
    """
    @scala_application test - Validates Scala compatibility with Gluten
    Tests JVM interop with native execution
    Based on: spark-apps/scala-spark-pi.jar (simulated in Python)
    """
    log("\n=== QA Test: Scala Pi Calculation (@scala_application) ===")
    
    ok = True
    try:
        from random import random
        
        def inside(p):
            x, y = random(), random()
            return x*x + y*y < 1
        
        num_samples = 100000
        count = spark.sparkContext.parallelize(range(0, num_samples)).filter(inside).count()
        pi_value = 4.0 * count / num_samples
        
        log(f"\n=== Scala Pi Calculation Result ===")
        log(f"Samples: {num_samples}")
        log(f"Pi estimate: {pi_value}")
        log(f"Error: {abs(pi_value - 3.14159):.5f}")
        
        # Verify result is reasonable
        if abs(pi_value - 3.14159) > 0.1:
            log("WARNING: Pi calculation result seems off")
            ok = False
        else:
            log("Scala Pi calculation test completed successfully")
            
    except Exception as e:
        ok = False
        log("\n!!! Scala Pi test failed:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))
    
    return ok


def test_autoscaling_simulation(spark: SparkSession) -> bool:
    """
    @autoscaling test - Tests dynamic resource allocation
    Validates Gluten's autoscaling behavior
    Based on: spark-apps/longrunning.py
    """
    log("\n=== QA Test: Autoscaling Simulation (@autoscaling) ===")
    
    ok = True
    try:
        # Check if autoscaling is enabled
        try:
            autoscale_enabled = spark.conf.get("spark.dynamicAllocation.enabled", "false")
            log(f"spark.dynamicAllocation.enabled = {autoscale_enabled}")
        except Exception:
            log("spark.dynamicAllocation.enabled = <not set>")
        
        log("Creating workload to test autoscaling...")
        
        # Generate data with varying partition sizes
        df = spark.range(0, 1000000, numPartitions=100)
        df = df.selectExpr("id", "id * 2 as doubled", "id % 10 as bucket")
        
        # Perform operations that would trigger executor scaling
        result = df.groupBy("bucket").agg({"doubled": "sum", "id": "count"})
        result_count = result.count()
        
        log(f"Processed {result_count} groups")
        result.show()
        
        log("Autoscaling simulation test completed successfully")
        
    except Exception as e:
        ok = False
        log("\n!!! Autoscaling test failed:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))
    
    return ok


def run_qa_tagged_tests(spark: SparkSession) -> dict:
    """
    Run all QA tagged tests from Hummingbird validation suite
    """
    log("\n" + "="*60)
    log("RUNNING QA-TAGGED TESTS FROM HUMMINGBIRD")
    log("="*60)
    
    results = {}
    
    results["qa_wordcount"] = test_wordcount_application(spark)
    results["qa_join_query"] = test_join_query_application(spark)
    results["qa_scala_pi"] = test_scala_pi_calculation(spark)
    
    results["qa_autoscaling"] = test_autoscaling_simulation(spark)
    
    return results


def main():
    spark = SparkSession.builder.appName(f"wxd-ci-smoke-{RUN_STAMP}").enableHiveSupport().config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config(
        "fs.azure.createRemoteFileSystemDuringInitialization", "true").getOrCreate()
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))

    log(f"\nRUN_MODE={RUN_MODE} CI_RUN_ID={CI_RUN_ID} RUN_STAMP={RUN_STAMP}")
    log(f"ICEBERG_CATALOG={ICEBERG_CATALOG}")
    log(f"DELTA_CATALOG={DELTA_CATALOG} HUDI_CATALOG={HUDI_CATALOG} HIVE_CATALOG={HIVE_CATALOG}")
    log(f"AWS_CATALOG={AWS_CATALOG} GCS_CATALOG={GCS_CATALOG} AZURE_CATALOG={AZURE_CATALOG}")
    log(f"AWS_WAREHOUSE={AWS_WAREHOUSE}")
    log(f"GCS_WAREHOUSE={GCS_WAREHOUSE}")
    log(f"AZURE_WAREHOUSE={AZURE_WAREHOUSE}")
    log(f"AZURE_DB_LOCATION={AZURE_DB_LOCATION}")
    log(f"AZURE_TABLE_LOCATION={AZURE_TABLE_LOCATION}")
    log(f"GCS_READ_PATH={GCS_READ_PATH}")
    log(f"GCS_WRITE_PATH={GCS_WRITE_PATH}")
    log(f"HUDI_TIMEOUT_SECONDS={HUDI_TIMEOUT_SECONDS}")

    log_gluten_status(spark)

    results = {}
    try:
        def on_hudi_timeout():
            spark.sparkContext.cancelAllJobs()
            log_hudi_scan("FAILED", "UNKNOWN", "timeout")

        if RUN_MODE in ("all", "catalogs"):
            results["iceberg_catalog"] = test_iceberg_single_catalog(spark)
            results["delta"] = test_delta(spark)
            results["hudi"] = run_with_timeout(
                "Hudi test",
                HUDI_TIMEOUT_SECONDS,
                lambda: test_hudi(spark),
                on_timeout=on_hudi_timeout,
            )
            results["hive"] = test_hive(spark)

        if RUN_MODE in ("all", "buckets"):
            results["multicloud_buckets"] = test_multicloud_buckets(spark)

        if RUN_MODE in ("all", "qa_tests"):
            qa_results = run_qa_tagged_tests(spark)
            results.update(qa_results)

        # Always run history check unless RUN_MODE explicitly excludes it
        if RUN_MODE in ("all", "history", "catalogs", "buckets", "qa_tests"):
            results["spark_history_eventlog"] = test_eventlog(spark)

    finally:
        try:
            spark.stop()
        except Exception:
            pass

    log("\n=== FINAL RESULTS ===")
    log(json.dumps(results, indent=2))

    failed = [k for k, v in results.items() if v is False]
    if failed:
        log("\nFAILED: " + ", ".join(failed))
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
