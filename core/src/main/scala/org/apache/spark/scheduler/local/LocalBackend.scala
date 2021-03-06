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

package org.apache.spark.scheduler.local

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import org.apache.spark.util.{ThreadUtils, Utils}

import scala.util.control.NonFatal

//import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisteredExecutor, ReleaseLock, LockAcquired, StragglerInfo}
import scala.language.existentials
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import scala.collection.immutable.HashMap

private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class StragglerInfo(executorId: String, partitionSize: Int, executionTime: Long)

private case class LockAcquired(executorId: String)

private case class KeyCounts(executorId: String, data: HashMap[Any, Int])

private case class KillTask(taskId: Long, interruptThread: Boolean)

private case class StopExecutor()

private case class ReleaseLock()

/**
 * Calls to LocalBackend are all serialized through LocalEndpoint. Using an RpcEndpoint makes the
 * calls on LocalBackend asynchronous, which is necessary to prevent deadlock between LocalBackend
 * and the TaskSchedulerImpl.
 */
private[spark] class LocalEndpoint(
    override val rpcEnv: RpcEnv,
    userClassPath: Seq[URL],
    scheduler: TaskSchedulerImpl,
    executorBackend: LocalBackend,
    private val totalCores: Int)
  extends ThreadSafeRpcEndpoint with Logging {

  private var freeCores = totalCores

  private var lockStartTime = System.currentTimeMillis()
  private var lockReleaseTime = System.currentTimeMillis()

  val keyCountsMap :scala.collection.concurrent.TrieMap[Any, Int] = new scala.collection.concurrent.TrieMap[Any, Int]()


  private val THREADS = SparkEnv.get.conf.getInt("spark.resultGetter.threads", 4)
  private val getTaskResultExecutor = ThreadUtils.newDaemonFixedThreadPool(
    THREADS, "task-result-getter")

  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = "localhost"

  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, userClassPath, isLocal = true)

  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      reviveOffers()

    case StatusUpdate(taskId, state, serializedData) =>
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }

    case KillTask(taskId, interruptThread) =>
      executor.killTask(taskId, interruptThread)

    case ReleaseLock =>
      executor.releaseLock()

//    case KeyCounts(executorId, serializedData) =>
//      val env = SparkEnv.get
//      val execId = executorId
//      println(s"keyCounts of $executorId shakalaka boom boom")
//      getTaskResultExecutor.execute(new Runnable {
//        override def run(): Unit = Utils.logUncaughtExceptions {
//          try {
//            val (result, size) = env.closureSerializer.newInstance().deserialize[TaskResult[_]](serializedData) match {
//              case directResult: DirectTaskResult[_] =>
//                directResult.value()
//                (directResult, serializedData.limit())
//              case IndirectTaskResult(blockId, size) =>
//
//                val serializedTaskResult = env.blockManager.getRemoteBytes(blockId)
//                if (!serializedTaskResult.isDefined) {
//                  logError("Exception while getting task result: serializedTaskResult undefined")
//                  return
//                }
//                val deserializedResult = env.closureSerializer.newInstance().deserialize[DirectTaskResult[_]](
//                  serializedTaskResult.get)
//                env.blockManager.master.removeBlock(blockId)
//                (deserializedResult, size)
//            }
//
//            val recMap = result.value().asInstanceOf[Map[_, Int]]
//
//            keyCountsMap ++= recMap.map{ case (k,v) => k -> (v + keyCountsMap.getOrElse(k,0)) }
//            keyCountsMap.foreach(x => println(s"ExecutorId : $execId => $x"))
////            result.metrics.setResultSize(size)
//
//          } catch {
//            case cnf: ClassNotFoundException =>
//              val loader = Thread.currentThread.getContextClassLoader
//              logError("Exception while getting task result", cnf)
//            case NonFatal(ex) =>
//              logError("Exception while getting task result", ex)
//
//          }
//        }
//      })

    case StragglerInfo(executorId, partitionSize, executionTime) =>
      println(s"hurrah! received at driver executorId: $executorId bucketSize: $partitionSize executionTime: $executionTime")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)

    case KeyCounts(executorId, data) =>

      println(s"keyCounts of $executorId " )
      val recMap = data.asInstanceOf[HashMap[_, Int]]

      keyCountsMap ++= recMap.map{ case (k,v) => k -> (v + keyCountsMap.getOrElse(k,0)) }
//      keyCountsMap.take(5).foreach(x => println(s"ExecutorId : $executorId => $x"))
      val kcArray = keyCountsMap.take(5).toArray
//      kcArray.foreach(k => println(k._1))



      //
//      class DomainPartitioner extends Partitioner {
//        def numPartitions = 2 //get number of executors
//        def getPartition(key: Any): Int = key match {
//
//          case "five" => {
//            println("test")
//            1
//          }
//          case _ => 0
//        }
//
//        override def equals(other: Any): Boolean = other.isInstanceOf[DomainPartitioner]
//      }

      context.reply(true)

    case LockAcquired(executorId) =>
      lockStartTime = System.currentTimeMillis()
      println(s"Lock for $executorId received by driver \n Press any key to release the lock")
      Console.readLine()
      lockReleaseTime = System.currentTimeMillis() - lockStartTime
      println(s"Lock kept by driver for $lockReleaseTime ms")
      executor.releaseLock()

  }

  def reviveOffers() {
    val offers = Seq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    for (task <- scheduler.resourceOffers(offers).flatten) {
      freeCores -= scheduler.CPUS_PER_TASK
      executor.launchTask(executorBackend, taskId = task.taskId, attemptNumber = task.attemptNumber,
        task.name, task.serializedTask)
    }
  }
}

/**
 * LocalBackend is used when running a local version of Spark where the executor, backend, and
 * master all run in the same JVM. It sits behind a TaskSchedulerImpl and handles launching tasks
 * on a single Executor (created by the LocalBackend) running locally.
 */
private[spark] class LocalBackend(
    conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    val totalCores: Int)
  extends SchedulerBackend with ExecutorBackend with Logging {

  private val appId = "local-" + System.currentTimeMillis
  private var localEndpoint: RpcEndpointRef = null
  private val userClassPath = getUserClasspath(conf)
  private val listenerBus = scheduler.sc.listenerBus

  /**
   * Returns a list of URLs representing the user classpath.
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    val userClassPathStr = conf.getOption("spark.executor.extraClassPath")
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }

  override def start() {
    val rpcEnv = SparkEnv.get.rpcEnv
    val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
    localEndpoint = rpcEnv.setupEndpoint("LocalBackendEndpoint", executorEndpoint)
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId,
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty)))
  }

  override def stop() {
    localEndpoint.ask(StopExecutor)
  }

  override def reviveOffers() {
    localEndpoint.send(ReviveOffers)
  }

  override def defaultParallelism(): Int =
    scheduler.conf.getInt("spark.default.parallelism", totalCores)

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    localEndpoint.send(KillTask(taskId, interruptThread))
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
  }

  override def lockAcquired(executorId: String) {
    println(s"lock acquired by $executorId and sending to driver")
    localEndpoint.ask[LockAcquired.type](LockAcquired(executorId))
  }

  override def sendStragglerInfo(executorId: String, partitionSize: Int, executionTime: Long) {
    localEndpoint.send(StragglerInfo(executorId, partitionSize, executionTime) )
  }

  override def sendKeyCounts(executorId: String, data: scala.collection.immutable.HashMap[Any, Int]) {
//    localEndpoint.send(KeyCounts(executorId, data) )
    localEndpoint.askWithRetry[Boolean](KeyCounts(executorId, data))
  }

  override def applicationId(): String = appId

}
