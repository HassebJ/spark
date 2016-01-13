package org.apache.spark.executor

import org.apache.spark.DomainPartitioner

class ExecutorSharedVars (
    val customPartitioner: DomainPartitioner,
    val isPartitionerAvailable: Boolean,
    val executorBackend : ExecutorBackend,
    val execId : String) {

  def getCustomPartitioner() : DomainPartitioner = {
    customPartitioner
  }
  def isPartitionerAvailable_() : Boolean = {
    isPartitionerAvailable
  }

  def getExecBackend() : ExecutorBackend = {
    executorBackend
  }

  def getExecId() : String = {
    execId
  }


}
