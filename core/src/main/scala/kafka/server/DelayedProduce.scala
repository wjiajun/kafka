/**
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

package kafka.server

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Implicits._
import kafka.utils.Pool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString = s"[acksPending: $acksPending, error: ${responseStatus.error.code}, " +
    s"startOffset: ${responseStatus.baseOffset}, requiredOffset: $requiredOffset]"
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = s"[requiredAcks: $produceRequiredAcks, partitionStatus: $produceStatus]"
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
 */
class DelayedProduce(delayMs: Long, // DelayedProduce的延迟时长
                     produceMetadata: ProduceMetadata, // ProduceMetadata中为一个ProducerRequest中的所有相关分区记录了一些追加消息后的返回结果，主要用于判断DelayedProduce是否满足执行条件
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                     lockOpt: Option[Lock] = None)
  extends DelayedOperation(delayMs, lockOpt) {

  // first update the acks pending variable according to the error code
  // 根据前面写入消息的返回结果，设置ProducePartitionStatus的ackPending字段和responseStatus字段的值
  produceMetadata.produceStatus.forKeyValue { (topicPartition, status) =>
    if (status.responseStatus.error == Errors.NONE) {
      // Timeout error state will be cleared when required acks are received
      // 对应分区写入成功，等待ISR集合中的副本完成同步
      // 如果写入异常，分区不需要等待
      status.acksPending = true
      // 预设错误码，如果ISR集合中的副本在此请求超时之前顺利完成了同步，会清除错误码
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      // 追加日志已经抛出异常，则不必等待此Partition对应ISR返回ACK
      status.acksPending = false
    }

    trace(s"Initial partition status for $topicPartition is $status")
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: Replica not assigned to partition
   * Case B: Replica is no longer the leader of this partition
   * Case C: This broker is the leader:
   *   C.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
   *   C.2 - Otherwise, set the response with no error.
   *
   *   检测是否满足DelayedProduce的执行条件，并在满足执行条件时调用forceComplete()方法完成该延迟任务
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    // 遍历produceMetadata中所有分区状态
    produceMetadata.produceStatus.forKeyValue { (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // skip those partitions that have already been satisfied
      if (status.acksPending) {// 检查此分区是否已经满足DelayedProduce执行条件
        // 获取对应的Partition对象
        val (hasEnough, error) = replicaManager.getPartitionOrError(topicPartition) match {
          case Left(err) =>
            // Case A
            // 找不到分区的Leader
            (false, err)

          case Right(partition) =>
            // 检查此分区的HW位置是否大于 requiredOffset
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
        }

        // Case B || C.1 || C.2
        if (error != Errors.NONE || hasEnough) {
          // leader副本的HW位置大于 requiredOffset
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }

    // check if every partition has satisfied at least one of case A, B or C
    // 检查全部的分区是否已经符合DelayedProduce的执行条件
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    produceMetadata.produceStatus.forKeyValue { (topicPartition, status) =>
      if (status.acksPending) {
        debug(s"Expiring produce request for partition $topicPartition with status $status")
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete(): Unit = {
    // 根据ProduceMetadata记录的相关信息，为每个Partition产生响应状态
    val responseStatus = produceMetadata.produceStatus.map { case (k, status) => k -> status.responseStatus }
    // 调用回调函数
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition): Unit = {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

