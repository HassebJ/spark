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

package org.apache.spark.shuffle.hash

import org.apache.spark._
import org.apache.spark.executor.{ExecutorSharedVars, ExecutorBackend, ShuffleWriteMetrics}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.storage.DiskBlockObjectWriter

import scala.concurrent.Lock
import scala.util.Random

private[spark] class HashShuffleWriter[K, V](
    shuffleBlockResolver: FileShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, _],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val numOutputSplits = dep.partitioner.numPartitions
  private val metrics = context.taskMetrics

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false
  private val lock = new Lock()

  private val writeMetrics = new ShuffleWriteMetrics()
  metrics.shuffleWriteMetrics = Some(writeMetrics)

  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  private val shuffle = shuffleBlockResolver.forMapTask(dep.shuffleId, mapId, numOutputSplits, ser,
    writeMetrics)

  /** Write a bunch of records to this task's output */
  override def write(tempRecords: Iterator[Product2[K, V]], context: TaskContext): Unit = {
    val randInit = new Random(System.currentTimeMillis)
    val randNumber = randInit.nextString(4)

    println(s"\n<<<<<<<<<<<<<<<<<< $mapId >>>>>>>>>>>>>>>>>")
    val (records, printRecords) = tempRecords.duplicate
    printRecords.foreach(kv => {
      val bucketId = dep.partitioner.getPartition(kv._1)
      println("MapId "+ mapId +" BucketId "+ bucketId +" " + kv)


    })


    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }

    lockExecutor(context.getSharedVars())
    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      println("BucketId "+ bucketId +" " + elem)
      shuffle.writers(bucketId).write(elem._1, elem._2)
    }
  }

  def lockExecutor(sharedVars: ExecutorSharedVars): Unit ={
    sharedVars.getExecBackend().lockAcquired(sharedVars.getExecId())
    println("Executor locked!")
//    lockStartTime = System.currentTimeMillis()
    lock.acquire()
    lock.acquire()
    lock.release()
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(initiallySuccess: Boolean): Option[MapStatus] = {
    var success = initiallySuccess
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        try {
          Some(commitWritesAndBuildStatus())
        } catch {
          case e: Exception =>
            success = false
            revertWrites()
            throw e
        }
      } else {
        revertWrites()
        None
      }
    } finally {
      // Release the writers back to the shuffle block manager.
      if (shuffle != null && shuffle.writers != null) {
        try {
          shuffle.releaseWriters(success)
        } catch {
          case e: Exception => logError("Failed to release shuffle writers", e)
        }
      }
    }
  }

  private def commitWritesAndBuildStatus(): MapStatus = {
    // Commit the writes. Get the size of each bucket block (total block size).
    val sizes: Array[Long] = shuffle.writers.map { writer: DiskBlockObjectWriter =>
      writer.commitAndClose()
      writer.fileSegment().length
    }
    MapStatus(blockManager.shuffleServerId, sizes)
  }

  private def revertWrites(): Unit = {
    if (shuffle != null && shuffle.writers != null) {
      for (writer <- shuffle.writers) {
        writer.revertPartialWritesAndClose()
      }
    }
  }
}
