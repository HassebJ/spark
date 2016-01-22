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
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.CustomPartitoner
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.HashMap
import scala.collection.mutable.HashMap
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

private case class KeyCounts(executorId: String, data: collection.immutable.HashMap[Any, Int])

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

  class StragglerInfo (val executorId: String,
                       val bucketSize: Int,
                       val executionTime: Long,
                       val speed: Int)

  private var freeCores = totalCores

  private var lockStartTime = System.currentTimeMillis()
  private var lockReleaseTime = System.currentTimeMillis()

  val keyCountsMap :scala.collection.concurrent.TrieMap[Any, Int] = new scala.collection.concurrent.TrieMap[Any, Int]()
  //to keep the execution speed info sent by executors
  val stragglerInfoMap = new collection.mutable.HashMap[String, StragglerInfo]

  private var stragglerTuple = ("0",Double.MaxValue)
  private var avgSpeedTuple = (0,0)
  private var numInfoReceived = new AtomicInteger(0)


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

    case KeyCounts(executorId, data) =>

      //      println(s"keyCounts of $executorId " )
      val recMap = data.asInstanceOf[collection.immutable.HashMap[_, Int]]
      numInfoReceived.incrementAndGet()

      keyCountsMap ++= recMap.map{ case (k,v) => k -> (v + keyCountsMap.getOrElse(k,0)) }
      val sortedMap = collection.immutable.ListMap(keyCountsMap.toSeq.sortWith(_._2 < _._2):_*)
      //      println("sortedMap")
      //      println(sortedMap)
      var k = 0
      val comFred = sortedMap.mapValues(freq => {
        k = k+freq
        k
      })

//      context.reply(true)
      println("numInfoReceived "+ numInfoReceived+ "== executorDataMap size "+ (2))
      if (numInfoReceived.intValue() ==  (2)){
        numInfoReceived.set(0)
        sendPartitoner()

      }

    case CustomPartitoner(cumFrqncy, numExecutors, speedUp, partitionId) =>
      println("in custom partitoner")
//      cumFrqncy.asInstanceOf[TrieMap[_, Int]].foreach(println)

      println(s"Lock released by Executor")

      executor.releaseLock(cumFrqncy.asInstanceOf[TrieMap[_, Int]], numExecutors, speedUp)

    case KillTask(taskId, interruptThread) =>
      executor.killTask(taskId, interruptThread)

//    case ReleaseLock =>
//      executor.releaseLock()

    case StragglerInfo(executorId, partitionSize, executionTime) =>
      println(s"hurrah! received at driver executorId: $executorId bucketSize: $partitionSize executionTime: $executionTime")
      val execSpeed = partitionSize.toDouble/executionTime
      if (execSpeed < stragglerTuple._2){
        stragglerTuple = (executorId, execSpeed)
      }
      stragglerInfoMap.put(executorId, new StragglerInfo(executorId, partitionSize, executionTime, execSpeed.toInt))
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)

    case LockAcquired(executorId) =>
      lockStartTime = System.currentTimeMillis()

      lockReleaseTime = System.currentTimeMillis() - lockStartTime
      println(s"Lock kept by driver for $lockReleaseTime ms")


  }

  private def sendPartitoner(): Unit ={
    val nonStragglers = stragglerInfoMap.map(x => (x._2.bucketSize, x._2.executionTime ))

    val answerTuple = nonStragglers.reduce((x, y) =>
      (x._1 + y._1, x._2 + y._2)
    )
//    println("total buckets / total time " +answerTuple )
    val avgNonStragglerSpeed =  (answerTuple._1.toDouble / answerTuple._2)

    val speedRatio = avgNonStragglerSpeed/stragglerTuple._2

//    println("avgNonStragglerSpeed: "+ avgNonStragglerSpeed + "speedRatio: " + speedRatio +" stragglerSpeed: "+ stragglerTuple._2)
    val speedUp = avgNonStragglerSpeed*100/(avgNonStragglerSpeed+stragglerTuple._2)
//    println("%normalexectorspeed: " + speedUp)
    val sortedMap = collection.immutable.ListMap(keyCountsMap.toSeq.sortWith(_._2 > _._2):_*)
    var k = 0
    val comFred = sortedMap.mapValues(freq => {
      k+=freq
      k
    })
    comFred.get("one") match {
            case Some(x)  =>
              if(x.toDouble*100/20 > speedUp){
//                println("x/20 "+ (x.toDouble*100/20)+ "speedUp" + speedUp)
//                println(s"x: $x, bucket:1")
              }else{
//                println("x/20 "+ (x.toDouble*100/20)+ "speedUp" + speedUp)
//                println(s"x: $x, bucket:0")
              }
            case _ => println("default")

          }

    val msg = CustomPartitoner(keyCountsMap, 2, speedUp.toInt, 0)
    executorBackend.sendPartitionerLocal(msg);

    //executorDataMap.foreach(x => {
      //val lockReleaseTime = System.currentTimeMillis() - lockStartTime
      //println(s"Lock for ${x._1} kept for $lockReleaseTime ms ")
      //localEndpoint.send(msg)
    //})


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
    localEndpoint.send(KeyCounts(executorId, data) )
//    localEndpoint.askWithRetry[Boolean](KeyCounts(executorId, data))
  }

  override def sendPartitionerLocal(msg:CustomPartitoner ) {
    localEndpoint.send(msg)
  }

  override def applicationId(): String = appId

}
