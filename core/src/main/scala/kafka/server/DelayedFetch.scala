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

import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}

import scala.collection._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

  override def toString: String = {
    "[startOffsetMetadata: " + startOffsetMetadata +
      ", fetchInfo: " + fetchInfo +
      "]"
  }
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 */
case class FetchMetadata(fetchMinBytes: Int,
                         fetchMaxBytes: Int,
                         hardMaxBytesLimit: Boolean,
                         fetchOnlyLeader: Boolean,
                         fetchIsolation: FetchIsolation,
                         isFromFollower: Boolean,
                         replicaId: Int,
                         fetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)]) {

  override def toString = "FetchMetadata(minBytes=" + fetchMinBytes + ", " +
    "maxBytes=" + fetchMaxBytes + ", " +
    "onlyLeader=" + fetchOnlyLeader + ", " +
    "fetchIsolation=" + fetchIsolation + ", " +
    "replicaId=" + replicaId + ", " +
    "partitionStatus=" + fetchPartitionStatus + ")"
}
/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
 */
class DelayedFetch(delayMs: Long,// 延迟操作的延迟时长
                   fetchMetadata: FetchMetadata,// FetchRequest中的所有相关分区记录了相关状态，主要用于判断DelayedProduce是否满足执行条件
                   replicaManager: ReplicaManager,
                   quota: ReplicaQuota,
                   clientMetadata: Option[ClientMetadata],
                   // 任务满足条件或到期执行时，在DelayedFetch.onComplete()方法中调用的回调函数，其主要功能是创建FetchResponse并添加到RequestChannels中对应的responseQueue队列中
                   responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   *
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
   * Case B: The replica is no longer available on this broker
   * Case C: This broker does not know of some partitions it tries to fetch
   * Case D: The partition is in an offline log directory on this broker
   * Case E: This broker is the leader, but the requested epoch is now fenced
   * Case F: The fetch offset locates not on the last segment of the log
   * Case G: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   * Case H: A diverging epoch was found, return response to trigger truncation
   * Upon completion, should return whatever data is available for each valid partition
   *
   * （1）发生Leader副本迁移，当前节点不再是该分区的Leader副本所在的节点。
   * （2）当前Broker找不到需要读取数据的分区副本。
   * （3）开始读取的offset不在activeSegment中，此时可能是发生了Log截断，也有可能是发生了roll操作产生了新的activeSegment。
   * （4）累计读取的字节数超过最小字节数限制。
   */
  override def tryComplete(): Boolean = {
    var accumulatedSize = 0
    // 遍历fetchMetadata中所有Partition的状态
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicIdPartition, fetchStatus) =>
        // 获取前面读取Log时的结束位置
        val fetchOffset = fetchStatus.startOffsetMetadata
        val fetchLeaderEpoch = fetchStatus.fetchInfo.currentLeaderEpoch
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            // 查找分区leader副本
            val partition = replicaManager.getPartitionOrException(topicIdPartition.topicPartition)
            val offsetSnapshot = partition.fetchOffsetSnapshot(fetchLeaderEpoch, fetchMetadata.fetchOnlyLeader)

            // 根据FetchRequest请求的来源来设置能读取的最大offset值
            //    消费者对应的endOffset是HW,而Follower副本对应的endOffset是LEO
            val endOffset = fetchMetadata.fetchIsolation match {
              case FetchLogEnd => offsetSnapshot.logEndOffset
              case FetchHighWatermark => offsetSnapshot.highWatermark
              case FetchTxnCommitted => offsetSnapshot.lastStableOffset
            }

            // Go directly to the check for Case G if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case F.

            // 检测上次读取后endOffset是否发生变化，如果没变，之前读不到足够的数据现在还是读不到，即任务条件依然不满足。
            // 如果变了，则继续下面的检查，看是否真正满足任务执行条件
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // 此时，endOffsetch出现减小的情况，跑到baseOffset较小的Segment上了
                // 可能是Leader副本的Log出现了truncate操作
                // Case F, this can happen when the new fetch operation is on a truncated leader
                debug(s"Satisfying fetch $fetchMetadata since it is fetching later segments of partition $topicIdPartition.")
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case F, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment
                // 此时，fetchOffset虽然依然在endOffset之前，但产生了新的activeSegment
                //    fetchOffset在较旧的LogSegment，而endOffset在activeSegment
                debug(s"Satisfying fetch $fetchMetadata immediately since it is fetching older segments.")
                // We will not force complete the fetch request if a replica should be throttled.
                if (!fetchMetadata.isFromFollower || !replicaManager.shouldLeaderThrottle(quota, partition, fetchMetadata.replicaId))
                  return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (!fetchMetadata.isFromFollower || !replicaManager.shouldLeaderThrottle(quota, partition, fetchMetadata.replicaId))
                // endOffset和fetchOffset依然在同一个LogSegment中，且endOffset向后移动，那就尝试计算累计的字节数
                  accumulatedSize += bytesAvailable
              }
            }

            // Case H: If truncation has caused diverging epoch while this request was in purgatory, return to trigger truncation
            fetchStatus.fetchInfo.lastFetchedEpoch.ifPresent { fetchEpoch =>
              val epochEndOffset = partition.lastOffsetForLeaderEpoch(fetchLeaderEpoch, fetchEpoch, fetchOnlyFromLeader = false)
              if (epochEndOffset.errorCode != Errors.NONE.code()
                || epochEndOffset.endOffset == UNDEFINED_EPOCH_OFFSET
                || epochEndOffset.leaderEpoch == UNDEFINED_EPOCH) {
                debug(s"Could not obtain last offset for leader epoch for partition $topicIdPartition, epochEndOffset=$epochEndOffset.")
                return forceComplete()
              } else if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchStatus.fetchInfo.fetchOffset) {
                debug(s"Satisfying fetch $fetchMetadata since it has diverging epoch requiring truncation for partition " +
                  s"$topicIdPartition epochEndOffset=$epochEndOffset fetchEpoch=$fetchEpoch fetchOffset=${fetchStatus.fetchInfo.fetchOffset}.")
                return forceComplete()
              }
            }
          }
        }catch {
          case _: NotLeaderOrFollowerException =>  // Case A or Case B
            debug(s"Broker is no longer the leader or follower of $topicIdPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // Case C
            debug(s"Broker no longer knows of partition $topicIdPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: KafkaStorageException => // Case D
            debug(s"Partition $topicIdPartition is in an offline log directory, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: FencedLeaderEpochException => // Case E
            debug(s"Broker is the leader of partition $topicIdPartition, but the requested epoch " +
              s"$fetchLeaderEpoch is fenced by the latest leader epoch, satisfy $fetchMetadata immediately")
            return forceComplete()
        }
    }

    // Case G
    // 累计读取的字节数足够
    if (accumulatedSize >= fetchMetadata.fetchMinBytes)
       forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete(): Unit = {
    // 重新从Log中读取数据
    val logReadResults = replicaManager.readFromLocalLog(
      replicaId = fetchMetadata.replicaId,
      fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
      fetchIsolation = fetchMetadata.fetchIsolation,
      fetchMaxBytes = fetchMetadata.fetchMaxBytes,
      hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
      readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
      clientMetadata = clientMetadata,
      quota = quota)

    // 将读取结果进行封装
    val fetchPartitionData = logReadResults.map { case (tp, result) =>
      val isReassignmentFetch = fetchMetadata.isFromFollower &&
        replicaManager.isAddingReplica(tp.topicPartition, fetchMetadata.replicaId)

      tp -> result.toFetchPartitionData(isReassignmentFetch)
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

