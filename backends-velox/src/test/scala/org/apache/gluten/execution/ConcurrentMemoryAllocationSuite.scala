package org.apache.gluten.execution

import org.apache.gluten.memory.MemoryUsageStatsBuilder
import org.apache.gluten.memory.listener.ReservationListeners
import org.apache.gluten.memory.memtarget.{MemoryTarget, Spiller, Spillers}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.TaskResources

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.collection.JavaConverters._
import scala.util.Random

class ConcurrentMemoryAllocationSuite extends SparkFunSuite with SharedSparkSession {
  test("concurrent allocation with spill - shared listener") {
    val numThreads = 50
    val offHeapSize = 500
    val minExtraSpillSize = 2
    val maxExtraSpillSize = 5
    val numAllocations = 100
    val minAllocationSize = 40
    val maxAllocationSize = 100
    val minAllocationDelayMs = 0
    val maxAllocationDelayMs = 0
    withSQLConf("spark.memory.offHeap.size" -> s"$offHeapSize") {
      val total = new AtomicLong(0L)
      TaskResources.runUnsafe {
        val spiller = Spillers.appendable()
        val listener = ReservationListeners.create(
          s"listener",
          spiller,
          Map[String, MemoryUsageStatsBuilder]().asJava)
        spiller.append(new Spiller() {
          override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = {
            val extraSpillSize = randomInt(minExtraSpillSize, maxExtraSpillSize)
            val spillSize = size + extraSpillSize
            val released = listener.unreserve(spillSize)
            assert(released <= spillSize)
            total.getAndAdd(-released)
            spillSize
          }
        })
        val pool = Executors.newFixedThreadPool(numThreads)
        val tasks = (0 until numThreads).map {
          _ =>
            new Callable[Unit]() {
              override def call(): Unit = {
                (0 until numAllocations).foreach {
                  _ =>
                    val allocSize =
                      randomInt(minAllocationSize, maxAllocationSize)
                    val granted = listener.reserve(allocSize)
                    assert(granted == allocSize)
                    total.getAndAdd(granted)
                    val sleepMs =
                      randomInt(minAllocationDelayMs, maxAllocationDelayMs)
                    Thread.sleep(sleepMs)
                }
              }
            }
        }.toList
        val futures = pool.invokeAll(tasks.asJava)
        pool.shutdown()
        pool.awaitTermination(60, TimeUnit.SECONDS)
        futures.forEach(_.get())
        val totalBytes = total.get()
        val released = listener.unreserve(totalBytes)
        assert(released == totalBytes)
        assert(listener.getUsedBytes == 0)
      }
    }
  }

  test("concurrent allocation with spill - dedicated listeners") {
    val numThreads = 50
    val offHeapSize = 500
    val minExtraSpillSize = 2
    val maxExtraSpillSize = 5
    val numAllocations = 100
    val minAllocationSize = 40
    val maxAllocationSize = 100
    val minAllocationDelayMs = 0
    val maxAllocationDelayMs = 0
    withSQLConf("spark.memory.offHeap.size" -> s"$offHeapSize") {
      TaskResources.runUnsafe {
        val total = new AtomicLong(0L)
        def newListener(id: Int) = {
          val spiller = Spillers.appendable()
          val listener = ReservationListeners.create(
            s"listener $id",
            spiller,
            Map[String, MemoryUsageStatsBuilder]().asJava)
          spiller.append(new Spiller() {
            override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = {
              val extraSpillSize = randomInt(minExtraSpillSize, maxExtraSpillSize)
              val spillSize = size + extraSpillSize
              val released = listener.unreserve(spillSize)
              assert(released <= spillSize)
              total.getAndAdd(-released)
              spillSize
            }
          })
          listener
        }

        val listeners = (0 until numThreads).map(newListener).toList
        val pool = Executors.newFixedThreadPool(numThreads)
        val tasks = (0 until numThreads).map {
          i =>
            new Callable[Unit]() {
              override def call(): Unit = {
                val listener = listeners(i)
                (0 until numAllocations).foreach {
                  _ =>
                    val allocSize =
                      randomInt(minAllocationSize, maxAllocationSize)
                    val granted = listener.reserve(allocSize)
                    assert(granted == allocSize)
                    total.getAndAdd(granted)
                    val sleepMs =
                      randomInt(minAllocationDelayMs, maxAllocationDelayMs)
                    Thread.sleep(sleepMs)
                }
              }
            }
        }.toList
        val futures = pool.invokeAll(tasks.asJava)
        pool.shutdown()
        pool.awaitTermination(60, TimeUnit.SECONDS)
        futures.forEach(_.get())
        val totalBytes = total.get()
        val remaining = listeners.foldLeft(totalBytes) {
          case (remainingBytes, listener) =>
            assert(remainingBytes >= 0)
            val unreserved = listener.unreserve(remainingBytes)
            remainingBytes - unreserved
        }
        assert(remaining == 0)
        assert(listeners.map(_.getUsedBytes).sum == 0)
      }
    }
  }

  private def randomInt(from: Int, to: Int): Int = {
    from + Random.nextInt(to - from + 1)
  }
}
