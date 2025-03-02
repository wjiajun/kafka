/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator.group

import java.io.PrintStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.api.{ApiVersion, KAFKA_0_10_1_IV0, KAFKA_2_1_IV0, KAFKA_2_1_IV1, KAFKA_2_3_IV0}
import kafka.common.OffsetAndMetadata
import kafka.internals.generated.{GroupMetadataValue, OffsetCommitKey, OffsetCommitValue, GroupMetadataKey => GroupMetadataKeyData}
import kafka.log.AppendOrigin
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{FetchLogEnd, ReplicaManager, RequestLocal}
import kafka.utils.CoreUtils.inLock
import kafka.utils.Implicits._
import kafka.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.{Metrics, Sensor}
import org.apache.kafka.common.metrics.stats.{Avg, Max, Meter}
import org.apache.kafka.common.protocol.{ByteBufferAccessor, Errors, MessageUtil}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{OffsetCommitRequest, OffsetFetchResponse}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, MessageFormatter, TopicPartition}

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

// brokerId：所在Broker的Id
// interBrokerProtocolVersion：保存 Broker 间通讯使用的请求版本。 Broker端参数inter.broker.protocol.version值 这个参数的主要用途是确定位移主题消息格式的版本。
// config: 内部位移主题配置类
// replicaManager: 副本管理器类
// zkClient: ZooKeeper客户端
class GroupMetadataManager(brokerId: Int,
                           interBrokerProtocolVersion: ApiVersion,
                           config: OffsetConfig,
                           val replicaManager: ReplicaManager,
                           time: Time,
                           metrics: Metrics) extends Logging with KafkaMetricsGroup {

  // 压缩器类型。向位移主题写入消息时执行压缩操作
  private val compressionType: CompressionType = CompressionType.forId(config.offsetsTopicCompressionCodec.codec)

  // 消费者组元数据容器，保存Broker管理的所有消费者组的数据
  // 该字段是 GroupMetadataManager 类上最重要的属性，它保存这个 Broker 上 GroupCoordinator 组件管理的所有消费者组元数据。
  // 它的 Key 是消费者组名称，Value是消费者组元数据，也就是 GroupMetadata。
  private val groupMetadataCache = new Pool[String, GroupMetadata]

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* partitions of consumer groups that are being loaded, its lock should be always called BEFORE the group lock if needed */
  // 位移主题下正在执行加载操作的分区
  // 首先，这些分区都是位移主题分区，也就是 __consumer_offsets 主题下的分区；
  // 其次，所谓的加载，是指读取位移主题消息数据，填充 GroupMetadataCache 字段的操作。
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /* partitions of consumer groups that are assigned, using the same loading partition lock */
  // 位移主题下完成加载操作的分区
  // 该字段保存的分区也是位移主题下的分区。
  // 和 loadingPartitions 不同的是，它保存的分区都是已经完成加载操作的分区。
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  /* shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /* number of partitions for the consumer metadata topic */
  // 位移主题总分区数
  @volatile private var groupMetadataTopicPartitionCount: Int = _

  /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "group-metadata-manager-")

  /* The groups with open transactional offsets commits per producer. We need this because when the commit or abort
   * marker comes in for a transaction, it is for a particular partition on the offsets topic and a particular producerId.
   * We use this structure to quickly find the groups which need to be updated by the commit/abort marker. */
  private val openGroupsForProducer = mutable.HashMap[Long, mutable.Set[String]]()

  /* Track the epoch in which we (un)loaded group state to detect racing LeaderAndIsr requests */
  private [group] val epochForPartitionId = new ConcurrentHashMap[Int, java.lang.Integer]()

  /* setup metrics*/
  private val partitionLoadSensor = metrics.sensor(GroupMetadataManager.LoadTimeSensor)

  partitionLoadSensor.add(metrics.metricName("partition-load-time-max",
    GroupMetadataManager.MetricsGroup,
    "The max time it took to load the partitions in the last 30sec"), new Max())
  partitionLoadSensor.add(metrics.metricName("partition-load-time-avg",
    GroupMetadataManager.MetricsGroup,
    "The avg time it took to load the partitions in the last 30sec"), new Avg())

  val offsetCommitsSensor: Sensor = metrics.sensor(GroupMetadataManager.OffsetCommitsSensor)

  offsetCommitsSensor.add(new Meter(
    metrics.metricName("offset-commit-rate",
      "group-coordinator-metrics",
      "The rate of committed offsets"),
    metrics.metricName("offset-commit-count",
      "group-coordinator-metrics",
      "The total number of committed offsets")))

  val offsetExpiredSensor: Sensor = metrics.sensor(GroupMetadataManager.OffsetExpiredSensor)

  offsetExpiredSensor.add(new Meter(
    metrics.metricName("offset-expiration-rate",
      "group-coordinator-metrics",
      "The rate of expired offsets"),
    metrics.metricName("offset-expiration-count",
      "group-coordinator-metrics",
      "The total number of expired offsets")))

  this.logIdent = s"[GroupMetadataManager brokerId=$brokerId] "

  private def recreateGauge[T](name: String, gauge: Gauge[T]): Gauge[T] = {
    removeMetric(name)
    newGauge(name, gauge)
  }

  recreateGauge("NumOffsets",
    () => groupMetadataCache.values.map { group =>
      group.inLock { group.numOffsets }
    }.sum
  )

  recreateGauge("NumGroups",
    () => groupMetadataCache.size
  )

  recreateGauge("NumGroupsPreparingRebalance",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(PreparingRebalance)
      }
    })

  recreateGauge("NumGroupsCompletingRebalance",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(CompletingRebalance)
      }
    })

  recreateGauge("NumGroupsStable",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(Stable)
      }
    })

  recreateGauge("NumGroupsDead",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(Dead)
      }
    })

  recreateGauge("NumGroupsEmpty",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(Empty)
      }
    })

  def startup(retrieveGroupMetadataTopicPartitionCount: () => Int, enableMetadataExpiration: Boolean): Unit = {
    groupMetadataTopicPartitionCount = retrieveGroupMetadataTopicPartitionCount()
    scheduler.startup()
    if (enableMetadataExpiration) {
      scheduler.schedule(name = "delete-expired-group-metadata",
        fun = () => cleanupGroupMetadata(),
        period = config.offsetsRetentionCheckIntervalMs,
        unit = TimeUnit.MILLISECONDS)
    }
  }

  def currentGroups: Iterable[GroupMetadata] = groupMetadataCache.values

  def isPartitionOwned(partition: Int): Boolean = inLock(partitionLock) { ownedPartitions.contains(partition) }

  def isPartitionLoading(partition: Int): Boolean = inLock(partitionLock) { loadingPartitions.contains(partition) }

  // 位移主题某个特定分区 Leader 副本所在的 Broker 被选定为指定消费者组的 Coordinator。  特定分区计算: 消费者组名哈希值与位移主题分区数求模的绝对值结果，就是该消费者组要写入位移主题的目标分区
  def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount

  // 检测当前GroupCoordinator是否管理指定的Consumer Group
  def isGroupLocal(groupId: String): Boolean = isPartitionOwned(partitionFor(groupId))

  // 检测指定的group对应的Offsets Topic分区是否还处于加载中
  def isGroupLoading(groupId: String): Boolean = isPartitionLoading(partitionFor(groupId))

  def isLoading: Boolean = inLock(partitionLock) { loadingPartitions.nonEmpty }

  // return true iff group is owned and the group doesn't exist
  def groupNotExists(groupId: String): Boolean = inLock(partitionLock) {
    isGroupLocal(groupId) && getGroup(groupId).forall { group =>
      group.inLock(group.is(Dead))
    }
  }

  // visible for testing
  private[group] def isGroupOpenForProducer(producerId: Long, groupId: String) = openGroupsForProducer.get(producerId) match {
    case Some(groups) =>
      groups.contains(groupId)
    case None =>
      false
  }

  /**
   * Get the group associated with the given groupId or null if not found
   */
  // getGroup方法：返回给定消费者组的元数据信息。
  // 若该组信息不存在，返回None
  def getGroup(groupId: String): Option[GroupMetadata] = {
    Option(groupMetadataCache.get(groupId))
  }

  /**
   * Get the group associated with the given groupId - the group is created if createIfNotExist
   * is true - or null if not found
   */
  // getOrMaybeCreateGroup方法：返回给定消费者组的元数据信息。
  // 若不存在，则视createIfNotExist参数值决定是否需要添加该消费者组
  def getOrMaybeCreateGroup(groupId: String, createIfNotExist: Boolean): Option[GroupMetadata] = {
    if (createIfNotExist)
    // 若不存在且允许添加，则添加一个状态是Empty的消费者组元数据对象
      Option(groupMetadataCache.getAndMaybePut(groupId, new GroupMetadata(groupId, Empty, time)))
    else
      Option(groupMetadataCache.get(groupId))
  }

  /**
   * Add a group or get the group associated with the given groupId if it already exists
   * 将该消费者组元数据对象添加进消费者组元数据缓存，实现加载消费者组元数据的目的。
   */
  def addGroup(group: GroupMetadata): GroupMetadata = {
    val currentGroup = groupMetadataCache.putIfNotExists(group.groupId, group)
    if (currentGroup != null) {
      currentGroup
    } else {
      group
    }
  }

  /**
   * 向 Coordinator 注册消费者组
   *
   * @param group
   * @param groupAssignment
   * @param responseCallback
   * @param requestLocal
   */
  def storeGroup(group: GroupMetadata,
                 groupAssignment: Map[String, Array[Byte]],
                 responseCallback: Errors => Unit,
                 requestLocal: RequestLocal = RequestLocal.NoCaching): Unit = {
    // 判断当前Broker是否是该消费者组的Coordinator(尝试去获取位移主题目标分区的底层日志对象)
    getMagic(partitionFor(group.groupId)) match {
      // 如果当前Broker是Coordinator
      case Some(magicValue) =>
        // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
        val timestampType = TimestampType.CREATE_TIME
        val timestamp = time.milliseconds()
        // 构建注册消息的Key
        val key = GroupMetadataManager.groupMetadataKey(group.groupId)
        // 构建注册消息的Value(对应group的分配结果)
        val value = GroupMetadataManager.groupMetadataValue(group, groupAssignment, interBrokerProtocolVersion)

        // 使用Key和Value构建待写入消息集合
        val records = {
          val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compressionType,
            Seq(new SimpleRecord(timestamp, key, value)).asJava))
          val builder = MemoryRecords.builder(buffer, magicValue, compressionType, timestampType, 0L)
          builder.append(timestamp, key, value)
          builder.build()
        }

        // 计算要写入的目标分区(group 对应的offset topic 分区)
        val groupMetadataPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
        val groupMetadataRecords = Map(groupMetadataPartition -> records)
        val generationId = group.generationId

        // set the callback function to insert the created group into cache after log append completed
        def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
          // the append response should only contain the topics partition
          // 确保消息写入到指定位移主题分区，否则抛出异常
          if (responseStatus.size != 1 || !responseStatus.contains(groupMetadataPartition))
            throw new IllegalStateException("Append status %s should only have one partition %s"
              .format(responseStatus, groupMetadataPartition))

          // construct the error status in the propagated assignment response in the cache
          // 更新已提交位移数指标
          val status = responseStatus(groupMetadataPartition)

          // 写入结果没有错误
          val responseError = if (status.error == Errors.NONE) {
            Errors.NONE
          } else {
            debug(s"Metadata from group ${group.groupId} with generation $generationId failed when appending to log " +
              s"due to ${status.error.exceptionName}")

            // transform the log append error code to the corresponding the commit status error code
            status.error match {
              case Errors.UNKNOWN_TOPIC_OR_PARTITION
                   | Errors.NOT_ENOUGH_REPLICAS
                   | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                Errors.COORDINATOR_NOT_AVAILABLE

              case Errors.NOT_LEADER_OR_FOLLOWER
                   | Errors.KAFKA_STORAGE_ERROR =>
                Errors.NOT_COORDINATOR

              case Errors.REQUEST_TIMED_OUT =>
                Errors.REBALANCE_IN_PROGRESS

              case Errors.MESSAGE_TOO_LARGE
                   | Errors.RECORD_LIST_TOO_LARGE
                   | Errors.INVALID_FETCH_SIZE =>

                error(s"Appending metadata message for group ${group.groupId} generation $generationId failed due to " +
                  s"${status.error.exceptionName}, returning UNKNOWN error code to the client")

                Errors.UNKNOWN_SERVER_ERROR

              case other =>
                error(s"Appending metadata message for group ${group.groupId} generation $generationId failed " +
                  s"due to unexpected error: ${status.error.exceptionName}")

                other
            }
          }

          responseCallback(responseError)
        }
        // 向位移主题写入消息
        appendForGroup(group, groupMetadataRecords, requestLocal, putCacheCallback)

      // 如果当前Broker不是Coordinator
      case None =>
        // 返回NOT_COORDINATOR异常
        responseCallback(Errors.NOT_COORDINATOR)
    }
  }

  private def appendForGroup(group: GroupMetadata,
                             records: Map[TopicPartition, MemoryRecords],
                             requestLocal: RequestLocal,
                             callback: Map[TopicPartition, PartitionResponse] => Unit): Unit = {
    // call replica manager to append the group message
    // 将消息写入到位移主题中
    replicaManager.appendRecords(
      timeout = config.offsetCommitTimeoutMs.toLong,
      requiredAcks = config.offsetCommitRequiredAcks,
      internalTopicsAllowed = true,
      origin = AppendOrigin.Coordinator,
      entriesPerPartition = records,
      delayedProduceLock = Some(group.lock),
      responseCallback = callback,
      requestLocal = requestLocal)
  }

  /**
   * Store offsets by appending it to the replicated log and then inserting to cache
   */
  // group：消费者组元数据
  // consumerId：消费者组成员ID
  // offsetMetadata：待保存的位移值，按照分区分组
  // responseCallback：处理完成后的回调函数
  // producerId：事务型Producer ID
  // producerEpoch：事务型Producer Epoch值
  def storeOffsets(group: GroupMetadata,
                   consumerId: String,
                   offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                   responseCallback: immutable.Map[TopicPartition, Errors] => Unit,
                   producerId: Long = RecordBatch.NO_PRODUCER_ID,
                   producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH,
                   requestLocal: RequestLocal = RequestLocal.NoCaching): Unit = {
    // first filter out partitions with offset metadata size exceeding limit
    // 过滤出满足特定条件的待保存位移数据
    val filteredOffsetMetadata = offsetMetadata.filter { case (_, offsetAndMetadata) =>
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }

    group.inLock {
      if (!group.hasReceivedConsistentOffsetCommits)
        warn(s"group: ${group.groupId} with leader: ${group.leaderOrNull} has received offset commits from consumers as well " +
          s"as transactional producers. Mixing both types of offset commits will generally result in surprises and " +
          s"should be avoided.")
    }

    // construct the message set to append
    val isTxnOffsetCommit = producerId != RecordBatch.NO_PRODUCER_ID
    // 如果没有任何分区的待保存位移满足特定条件
    if (filteredOffsetMetadata.isEmpty) {
      // compute the final error codes for the commit response
      // 构造OFFSET_METADATA_TOO_LARGE异常并调用responseCallback返回
      val commitStatus = offsetMetadata.map { case (k, _) => k -> Errors.OFFSET_METADATA_TOO_LARGE }
      responseCallback(commitStatus)
    } else {
      // 查看当前Broker是否为给定消费者组的Coordinator
      getMagic(partitionFor(group.groupId)) match {
        // 如果是Coordinator
        case Some(magicValue) =>
          // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
          val timestampType = TimestampType.CREATE_TIME
          val timestamp = time.milliseconds()

          // 构造位移主题的位移提交消息
          val records = filteredOffsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
            // (groupId、topic名称、partition_id三部分组成)
            val key = GroupMetadataManager.offsetCommitKey(group.groupId, topicPartition)
            val value = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, interBrokerProtocolVersion)
            new SimpleRecord(timestamp, key, value)
          }
          val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
          // 为写入消息创建内存Buffer
          val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compressionType, records.asJava))

          if (isTxnOffsetCommit && magicValue < RecordBatch.MAGIC_VALUE_V2)
            throw Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.exception("Attempting to make a transaction offset commit with an invalid magic: " + magicValue)

          val builder = MemoryRecords.builder(buffer, magicValue, compressionType, timestampType, 0L, time.milliseconds(),
            producerId, producerEpoch, 0, isTxnOffsetCommit, RecordBatch.NO_PARTITION_LEADER_EPOCH)

          records.foreach(builder.append)
          val entries = Map(offsetTopicPartition -> builder.build())

          // set the callback function to insert offsets into cache after log append completed
          def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
            // the append response should only contain the topics partition
            // 确保消息写入到指定位移主题分区，否则抛出异常
            if (responseStatus.size != 1 || !responseStatus.contains(offsetTopicPartition))
              throw new IllegalStateException("Append status %s should only have one partition %s"
                .format(responseStatus, offsetTopicPartition))

            // construct the commit response status and insert
            // the offset and metadata to cache if the append status has no error
            // 更新已提交位移数指标
            val status = responseStatus(offsetTopicPartition)

            val responseError = group.inLock {
              // 写入结果没有错误
              if (status.error == Errors.NONE) {
                // 如果不是Dead状态
                if (!group.is(Dead)) {
                  filteredOffsetMetadata.forKeyValue { (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      group.onTxnOffsetCommitAppend(producerId, topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                    else
                    // 调用GroupMetadata的onOffsetCommitAppend方法填充元数据
                      group.onOffsetCommitAppend(topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                  }
                }

                // Record the number of offsets committed to the log
                offsetCommitsSensor.record(records.size)

                Errors.NONE
              } else {
                if (!group.is(Dead)) {
                  if (!group.hasPendingOffsetCommitsFromProducer(producerId))
                    removeProducerGroup(producerId, group.groupId)
                  filteredOffsetMetadata.forKeyValue { (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      group.failPendingTxnOffsetCommit(producerId, topicPartition)
                    else
                      group.failPendingOffsetWrite(topicPartition, offsetAndMetadata)
                  }
                }

                debug(s"Offset commit $filteredOffsetMetadata from group ${group.groupId}, consumer $consumerId " +
                  s"with generation ${group.generationId} failed when appending to log due to ${status.error.exceptionName}")

                // transform the log append error code to the corresponding the commit status error code
                // 确认异常类型
                status.error match {
                  case Errors.UNKNOWN_TOPIC_OR_PARTITION
                       | Errors.NOT_ENOUGH_REPLICAS
                       | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                    Errors.COORDINATOR_NOT_AVAILABLE

                  case Errors.NOT_LEADER_OR_FOLLOWER
                       | Errors.KAFKA_STORAGE_ERROR =>
                    Errors.NOT_COORDINATOR

                  case Errors.MESSAGE_TOO_LARGE
                       | Errors.RECORD_LIST_TOO_LARGE
                       | Errors.INVALID_FETCH_SIZE =>
                    Errors.INVALID_COMMIT_OFFSET_SIZE

                  case other => other
                }
              }
            }

            // compute the final error codes for the commit response
            // 利用异常类型构建提交返回状态
            val commitStatus = offsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
              if (validateOffsetMetadataLength(offsetAndMetadata.metadata))
                (topicPartition, responseError)
              else
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE)
            }

            // finally trigger the callback logic passed from the API layer
            // 调用回调函数
            responseCallback(commitStatus)
          }

          if (isTxnOffsetCommit) {
            group.inLock {
              addProducerGroup(producerId, group.groupId)
              group.prepareTxnOffsetCommit(producerId, offsetMetadata)
            }
          } else {
            group.inLock {
              group.prepareOffsetCommit(offsetMetadata)
            }
          }

          // 写入消息到位移主题，同时调用putCacheCallback方法更新消费者元数据
          appendForGroup(group, entries, requestLocal, putCacheCallback)

        // 如果是Coordinator
        case None =>
          // 构造NOT_COORDINATOR异常并提交给responseCallback方法
          val commitStatus = offsetMetadata.map { case (topicPartition, _) =>
            (topicPartition, Errors.NOT_COORDINATOR)
          }
          responseCallback(commitStatus)
      }
    }
  }

  /**
   * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
   * returns the current offset or it begins to sync the cache from the log (and returns an error code).
   */
  def getOffsets(groupId: String, requireStable: Boolean, topicPartitionsOpt: Option[Seq[TopicPartition]]): Map[TopicPartition, PartitionData] = {
    trace("Getting offsets of %s for group %s.".format(topicPartitionsOpt.getOrElse("all partitions"), groupId))
    // 从groupMetadataCache字段中获取指定消费者组的元数据
    val group = groupMetadataCache.get(groupId)
    // 如果没有组数据，返回空数据
    if (group == null) {
      topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
        val partitionData = new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
          Optional.empty(), "", Errors.NONE)
        topicPartition -> partitionData
      }.toMap
    } else {
      // 如果存在组数据
      group.inLock {
        // 如果组处于Dead状态，则返回空数据
        if (group.is(Dead)) {
          topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
            val partitionData = new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
              Optional.empty(), "", Errors.NONE)
            topicPartition -> partitionData
          }.toMap
        } else {
          // 查找指定分区集合的最近提交offset
          val topicPartitions = topicPartitionsOpt.getOrElse(group.allOffsets.keySet)

          topicPartitions.map { topicPartition =>
            if (requireStable && group.hasPendingOffsetCommitsForTopicPartition(topicPartition)) {
              topicPartition -> new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                Optional.empty(), "", Errors.UNSTABLE_OFFSET_COMMIT)
            } else {
              val partitionData = group.offset(topicPartition) match {
                // 如果没有该分区位移数据，返回空数据
                case None =>
                  new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                    Optional.empty(), "", Errors.NONE)
                // 从消费者组元数据中返回指定分区的位移数据
                case Some(offsetAndMetadata) =>
                  new PartitionData(offsetAndMetadata.offset,
                    offsetAndMetadata.leaderEpoch, offsetAndMetadata.metadata, Errors.NONE)
              }
              topicPartition -> partitionData
            }
          }.toMap
        }
      }
    }
  }

  /**
   * Asynchronously read the partition from the offsets topic and populate the cache
   * 会创建一个异步任务，来读取位移主题消息，并填充缓存。
   */
  def scheduleLoadGroupAndOffsets(offsetsPartition: Int, coordinatorEpoch: Int, onGroupLoaded: GroupMetadata => Unit): Unit = {
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    info(s"Scheduling loading of offsets and group metadata from $topicPartition for epoch $coordinatorEpoch")
    val startTimeMs = time.milliseconds()
    scheduler.schedule(topicPartition.toString, () => loadGroupsAndOffsets(topicPartition, coordinatorEpoch, onGroupLoaded, startTimeMs))
  }

  /**
   * 当Broker成为Offsets Topic分区的Leader副本时逻辑
   *
   * @param topicPartition
   * @param coordinatorEpoch
   * @param onGroupLoaded
   * @param startTimeMs
   */
  private[group] def loadGroupsAndOffsets(
    topicPartition: TopicPartition,
    coordinatorEpoch: Int,
    onGroupLoaded: GroupMetadata => Unit,
    startTimeMs: java.lang.Long
  ): Unit = {
    if (!maybeUpdateCoordinatorEpoch(topicPartition.partition, Some(coordinatorEpoch))) {
      info(s"Not loading offsets and group metadata for $topicPartition " +
        s"in epoch $coordinatorEpoch since current epoch is ${epochForPartitionId.get(topicPartition.partition)}")
    } else if (!addLoadingPartition(topicPartition.partition)) {
      info(s"Already loading offsets and group metadata from $topicPartition")
    } else {
      try {
        val schedulerTimeMs = time.milliseconds() - startTimeMs
        debug(s"Started loading offsets and group metadata from $topicPartition for epoch $coordinatorEpoch")
        doLoadGroupsAndOffsets(topicPartition, onGroupLoaded)
        val endTimeMs = time.milliseconds()
        val totalLoadingTimeMs = endTimeMs - startTimeMs
        partitionLoadSensor.record(totalLoadingTimeMs.toDouble, endTimeMs, false)
        info(s"Finished loading offsets and group metadata from $topicPartition "
          + s"in $totalLoadingTimeMs milliseconds for epoch $coordinatorEpoch, of which " +
          s"$schedulerTimeMs milliseconds was spent in the scheduler.")
      } catch {
        case t: Throwable => error(s"Error loading offsets from $topicPartition", t)
      } finally {
        // 将当前offset topic分区从loadingParatitions移入ownedPartitions集合中
        inLock(partitionLock) {
          ownedPartitions.add(topicPartition.partition)
          loadingPartitions.remove(topicPartition.partition)
        }
      }
    }
  }

  // 加载消费者组；加载消费者组的位移；(加载，就是指读取位移主题下的消息，并将这些信息填充到缓存中)
  /**
   *
   * @param topicPartition 位移主题目标分区
   * @param onGroupLoaded 加载完成后要执行的逻辑
   */
  private def doLoadGroupsAndOffsets(topicPartition: TopicPartition, onGroupLoaded: GroupMetadata => Unit): Unit = {
    // 获取位移主题指定分区的LEO值
    // 如果当前Broker不是该分区的Leader副本，则返回-1
    //  Kafka 依靠它来判断分区的 Leader 副本是否发生变更。一旦发生变更，那么，在当前 Broker 执行 logEndOffset 方法的返回值，就是 -1，此时，Broker 就不再是 Leader 副本了。
    def logEndOffset: Long = replicaManager.getLogEndOffset(topicPartition).getOrElse(-1L)

    // 获取partition对象对应的log实例
    replicaManager.getLog(topicPartition) match {
      // 如果无法获取到日志对象
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        // 已完成位移值加载的分区列表
        val loadedOffsets = mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]()
        // 处于位移加载中的分区列表，只用于Kafka事务
        val pendingOffsets = mutable.Map[Long, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]]()
        // 已完成组信息加载的消费者组列表
        val loadedGroups = mutable.Map[String, GroupMetadata]()
        // 待移除的消费者组列表
        val removedGroups = mutable.Set[String]()

        // buffer may not be needed if records are read from memory
        // 保存消息集合的ByteBuffer缓冲区
        var buffer = ByteBuffer.allocate(0)

        // loop breaks if leader changes at any time during the load, since logEndOffset is -1
        // 位移主题目标分区日志起始位移值
        var currOffset = log.logStartOffset

        // loop breaks if no records have been read, since the end of the log has been reached
        // 至少要求读取一条消息
        var readAtLeastOneRecord = true

        // 当前读取位移<LEO，且至少要求读取一条消息，且GroupMetadataManager未关闭
        while (currOffset < logEndOffset && readAtLeastOneRecord && !shuttingDown.get()) {
          // 读取位移主题指定分区
          val fetchDataInfo = log.read(currOffset,
            maxLength = config.loadBufferSize,
            isolation = FetchLogEnd,
            minOneMessage = true)

          // 如果无消息可读，则不再要求至少读取一条消息
          readAtLeastOneRecord = fetchDataInfo.records.sizeInBytes > 0

          // 创建消息集合
          val memRecords = (fetchDataInfo.records: @unchecked) match {
            case records: MemoryRecords => records
            case fileRecords: FileRecords =>
              val sizeInBytes = fileRecords.sizeInBytes
              val bytesNeeded = Math.max(config.loadBufferSize, sizeInBytes)

              // minOneMessage = true in the above log.read means that the buffer may need to be grown to ensure progress can be made
              if (buffer.capacity < bytesNeeded) {
                if (config.loadBufferSize < bytesNeeded)
                  warn(s"Loaded offsets and group metadata from $topicPartition with buffer larger ($bytesNeeded bytes) than " +
                    s"configured offsets.load.buffer.size (${config.loadBufferSize} bytes)")

                buffer = ByteBuffer.allocate(bytesNeeded)
              } else {
                buffer.clear()
              }

              fileRecords.readInto(buffer, 0)
              MemoryRecords.readableRecords(buffer)
          }

          // 遍历消息集合的每个消息批次(RecordBatch)
          memRecords.batches.forEach { batch =>
            val isTxnOffsetCommit = batch.isTransactional
            // 如果是控制类消息批次
            // 控制类消息批次属于Kafka事务范畴
            if (batch.isControlBatch) {
              val recordIterator = batch.iterator
              if (recordIterator.hasNext) {
                val record = recordIterator.next()
                val controlRecord = ControlRecordType.parse(record.key)
                if (controlRecord == ControlRecordType.COMMIT) {
                  pendingOffsets.getOrElse(batch.producerId, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]())
                    .foreach {
                      case (groupTopicPartition, commitRecordMetadataAndOffset) =>
                        if (!loadedOffsets.contains(groupTopicPartition) || loadedOffsets(groupTopicPartition).olderThan(commitRecordMetadataAndOffset))
                          loadedOffsets.put(groupTopicPartition, commitRecordMetadataAndOffset)
                    }
                }
                pendingOffsets.remove(batch.producerId)
              }
            } else {
              // 保存消息批次第一条消息的位移值
              var batchBaseOffset: Option[Long] = None
              // 遍历消息批次下的所有消息
              for (record <- batch.asScala) {
                // 确保消息必须有Key，否则抛出异常
                require(record.hasKey, "Group metadata/offset entry key should not be null")
                // 记录消息批次第一条消息的位移值
                if (batchBaseOffset.isEmpty)
                  batchBaseOffset = Some(record.offset)
                // 读取消息Key
                GroupMetadataManager.readMessageKey(record.key) match {

                  // 如果是OffsetKey，说明是提交位移消息
                  case offsetKey: OffsetKey =>
                    if (isTxnOffsetCommit && !pendingOffsets.contains(batch.producerId))
                      pendingOffsets.put(batch.producerId, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]())

                    // load offset
                    val groupTopicPartition = offsetKey.key
                    // 如果该消息没有Value(标记删除的消息)
                    if (!record.hasValue) {
                      if (isTxnOffsetCommit)
                        pendingOffsets(batch.producerId).remove(groupTopicPartition)
                      else
                      // 将目标分区从已完成位移值加载的分区列表中移除
                        loadedOffsets.remove(groupTopicPartition)
                    } else {
                      val offsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(record.value)
                      if (isTxnOffsetCommit)
                        pendingOffsets(batch.producerId).put(groupTopicPartition, CommitRecordMetadataAndOffset(batchBaseOffset, offsetAndMetadata))
                      else
                      // 将目标分区加入到已完成位移值加载的分区列表
                        loadedOffsets.put(groupTopicPartition, CommitRecordMetadataAndOffset(batchBaseOffset, offsetAndMetadata))
                    }

                  // 如果是GroupMetadataKey，说明是注册消息
                  case groupMetadataKey: GroupMetadataKey =>
                    // load group metadata
                    val groupId = groupMetadataKey.key
                    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value, time)
                    // 如果消息Value不为空
                    if (groupMetadata != null) {
                      // 把该消费者组从待移除消费者组列表中移除
                      removedGroups.remove(groupId)
                      // 将消费者组加入到已完成加载的消费组列表
                      loadedGroups.put(groupId, groupMetadata)
                    } else {
                      // 如果消息Value为空，说明是Tombstone消息
                      loadedGroups.remove(groupId)
                      // 将消费者组加入到待移除消费组列表
                      removedGroups.add(groupId)
                    }

                  // 如果是未知类型的Key，抛出异常
                  case unknownKey =>
                    throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
                }
              }
            }
            // 更新读取位置到消息批次最后一条消息的位移值+1，等待下次while循环
            currOffset = batch.nextOffset
          }
        }

        // 处理loadedOffsets
        val (groupOffsets, emptyGroupOffsets) = loadedOffsets
          .groupBy(_._1.group)
          .map { case (k, v) =>
            // 提取出<组名，主题名，分区号>与位移值对
            k -> v.map { case (groupTopicPartition, offset) => (groupTopicPartition.topicPartition, offset) }
          }.partition { case (group, _) => loadedGroups.contains(group) }

        val pendingOffsetsByGroup = mutable.Map[String, mutable.Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]]()
        pendingOffsets.forKeyValue { (producerId, producerOffsets) =>
          producerOffsets.keySet.map(_.group).foreach(addProducerGroup(producerId, _))
          producerOffsets
            .groupBy(_._1.group)
            .forKeyValue { (group, offsets) =>
              val groupPendingOffsets = pendingOffsetsByGroup.getOrElseUpdate(group, mutable.Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
              val groupProducerOffsets = groupPendingOffsets.getOrElseUpdate(producerId, mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
              groupProducerOffsets ++= offsets.map { case (groupTopicPartition, offset) =>
                (groupTopicPartition.topicPartition, offset)
              }
            }
        }

        val (pendingGroupOffsets, pendingEmptyGroupOffsets) = pendingOffsetsByGroup
          .partition { case (group, _) => loadedGroups.contains(group)}

        // 处理loadedGroups
        // 将需要加载的GroupMetadata信息加载到groupCache集合中
        loadedGroups.values.foreach { group =>
          // 提取消费者组的已提交位移
          val offsets = groupOffsets.getOrElse(group.groupId, Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
          val pendingOffsets = pendingGroupOffsets.getOrElse(group.groupId, Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
          debug(s"Loaded group metadata $group with offsets $offsets and pending offsets $pendingOffsets")
          // 为已完成加载的组执行加载组操作
          loadGroup(group, offsets, pendingOffsets)
          // 为已完成加载的组执行加载组操作之后的逻辑
          onGroupLoaded(group)
        }

        // load groups which store offsets in kafka, but which have no active members and thus no group
        // metadata stored in the log
        (emptyGroupOffsets.keySet ++ pendingEmptyGroupOffsets.keySet).foreach { groupId =>
          val group = new GroupMetadata(groupId, Empty, time)
          val offsets = emptyGroupOffsets.getOrElse(groupId, Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
          val pendingOffsets = pendingEmptyGroupOffsets.getOrElse(groupId, Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
          debug(s"Loaded group metadata $group with offsets $offsets and pending offsets $pendingOffsets")
          // 为空的消费者组执行加载组操作
          loadGroup(group, offsets, pendingOffsets)
          // 为空的消费者执行加载组操作之后的逻辑
          onGroupLoaded(group)
        }

        // 处理removedGroups
        // 检测需要删除的GroupMetadata信息是否还在groupsCache集合中
        removedGroups.foreach { groupId =>
          // if the cache already contains a group which should be removed, raise an error. Note that it
          // is possible (however unlikely) for a consumer group to be removed, and then to be used only for
          // offset storage (i.e. by "simple" consumers)
          if (groupMetadataCache.contains(groupId) && !emptyGroupOffsets.contains(groupId))
            throw new IllegalStateException(s"Unexpected unload of active group $groupId while " +
              s"loading partition $topicPartition")
        }
    }
  }

  private def loadGroup(group: GroupMetadata, offsets: Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTransactionalOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]): Unit = {
    // offsets are initialized prior to loading the group into the cache to ensure that clients see a consistent
    // view of the group's offsets
    trace(s"Initialized offsets $offsets for group ${group.groupId}")
    // 初始化消费者组的位移信息
    group.initializeOffsets(offsets, pendingTransactionalOffsets.toMap)

    // 调用addGroup方法添加消费者组
    val currentGroup = addGroup(group)
    if (group != currentGroup)
      debug(s"Attempt to load group ${group.groupId} from log with generation ${group.generationId} failed " +
        s"because there is already a cached group with generation ${currentGroup.generationId}")
  }

  /**
   * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
   * that partition.
   *
   * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
   */
  def removeGroupsForPartition(offsetsPartition: Int,
                               coordinatorEpoch: Option[Int],
                               onGroupUnloaded: GroupMetadata => Unit): Unit = {
    // 位移主题分区
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    info(s"Scheduling unloading of offsets and group metadata from $topicPartition")
    // 创建异步任务，移除组信息和位移信息
    scheduler.schedule(topicPartition.toString, () => removeGroupsAndOffsets(topicPartition, coordinatorEpoch, onGroupUnloaded))
  }

  // 内部方法，用于移除组信息和位移信息
  private [group] def removeGroupsAndOffsets(topicPartition: TopicPartition,
                                             coordinatorEpoch: Option[Int],
                                             onGroupUnloaded: GroupMetadata => Unit): Unit = {
    val offsetsPartition = topicPartition.partition
    if (maybeUpdateCoordinatorEpoch(offsetsPartition, coordinatorEpoch)) {
      var numOffsetsRemoved = 0
      var numGroupsRemoved = 0

      debug(s"Started unloading offsets and group metadata for $topicPartition for " +
        s"coordinator epoch $coordinatorEpoch")
      inLock(partitionLock) {
        // we need to guard the group removal in cache in the loading partition lock
        // to prevent coordinator's check-and-get-group race condition
        // 移除ownedPartitions中特定位移主题分区记录
        ownedPartitions.remove(offsetsPartition) // 标识当前GroupCoordinator不再管理其对应的consumer group
        loadingPartitions.remove(offsetsPartition)

        // 遍历所有消费者组信息
        for (group <- groupMetadataCache.values) {
          // 如果该组信息保存在特定位移主题分区中
          if (partitionFor(group.groupId) == offsetsPartition) {
            // 执行组卸载逻辑
            onGroupUnloaded(group)
            // 关键步骤！将组信息从groupMetadataCache中移除
            groupMetadataCache.remove(group.groupId, group)
            // 把消费者组从producer对应的组集合中移除
            removeGroupFromAllProducers(group.groupId)
            // 更新已移除组计数器
            numGroupsRemoved += 1
            // 更新已移除位移值计数器
            numOffsetsRemoved += group.numOffsets
          }
        }
      }
      info(s"Finished unloading $topicPartition for coordinator epoch $coordinatorEpoch. " +
        s"Removed $numOffsetsRemoved cached offsets and $numGroupsRemoved cached groups.")
    } else {
      info(s"Not removing offsets and group metadata for $topicPartition " +
        s"in epoch $coordinatorEpoch since current epoch is ${epochForPartitionId.get(topicPartition.partition)}")
    }
  }

  /**
   * Update the cached coordinator epoch if the new value is larger than the old value.
   * @return true if `epochOpt` is either empty or contains a value greater than or equal to the current epoch
   */
  private def maybeUpdateCoordinatorEpoch(
    partitionId: Int,
    epochOpt: Option[Int]
  ): Boolean = {
    val updatedEpoch = epochForPartitionId.compute(partitionId, (_, currentEpoch) => {
      if (currentEpoch == null) {
        epochOpt.map(Int.box).orNull
      } else {
        epochOpt match {
          case Some(epoch) if epoch > currentEpoch => epoch
          case _ => currentEpoch
        }
      }
    })
    epochOpt.forall(_ == updatedEpoch)
  }

  // visible for testing
  private[group] def cleanupGroupMetadata(): Unit = {
    val currentTimestamp = time.milliseconds()
    val numOffsetsRemoved = cleanupGroupMetadata(groupMetadataCache.values, RequestLocal.NoCaching,
      _.removeExpiredOffsets(currentTimestamp, config.offsetsRetentionMs))
    offsetExpiredSensor.record(numOffsetsRemoved)
    if (numOffsetsRemoved > 0)
      info(s"Removed $numOffsetsRemoved expired offsets in ${time.milliseconds() - currentTimestamp} milliseconds.")
  }

  /**
    * This function is used to clean up group offsets given the groups and also a function that performs the offset deletion.
    * @param groups Groups whose metadata are to be cleaned up
    * @param selector A function that implements deletion of (all or part of) group offsets. This function is called while
    *                 a group lock is held, therefore there is no need for the caller to also obtain a group lock.
    * @return The cumulative number of offsets removed
    */
  def cleanupGroupMetadata(groups: Iterable[GroupMetadata], requestLocal: RequestLocal,
                           selector: GroupMetadata => Map[TopicPartition, OffsetAndMetadata]): Int = {
    var offsetsRemoved = 0

    groups.foreach { group =>
      val groupId = group.groupId
      val (removedOffsets, groupIsDead, generation) = group.inLock {
        val removedOffsets = selector(group)
        if (group.is(Empty) && !group.hasOffsets) {
          info(s"Group $groupId transitioned to Dead in generation ${group.generationId}")
          group.transitionTo(Dead)
        }
        (removedOffsets, group.is(Dead), group.generationId)
      }

    val offsetsPartition = partitionFor(groupId)
    val appendPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    getMagic(offsetsPartition) match {
      case Some(magicValue) =>
        // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
        val timestampType = TimestampType.CREATE_TIME
        val timestamp = time.milliseconds()

          replicaManager.onlinePartition(appendPartition).foreach { partition =>
            val tombstones = ArrayBuffer.empty[SimpleRecord]
            removedOffsets.forKeyValue { (topicPartition, offsetAndMetadata) =>
              trace(s"Removing expired/deleted offset and metadata for $groupId, $topicPartition: $offsetAndMetadata")
              val commitKey = GroupMetadataManager.offsetCommitKey(groupId, topicPartition)
              // 提交位移消息对应的Tombstone消息
              tombstones += new SimpleRecord(timestamp, commitKey, null)
            }
            trace(s"Marked ${removedOffsets.size} offsets in $appendPartition for deletion.")

            // We avoid writing the tombstone when the generationId is 0, since this group is only using
            // Kafka for offset storage.
            if (groupIsDead && groupMetadataCache.remove(groupId, group) && generation > 0) {
              // Append the tombstone messages to the partition. It is okay if the replicas don't receive these (say,
              // if we crash or leaders move) since the new leaders will still expire the consumers with heartbeat and
              // retry removing this group.
              val groupMetadataKey = GroupMetadataManager.groupMetadataKey(group.groupId)
              // 注册消息对应的Tombstone消息
              tombstones += new SimpleRecord(timestamp, groupMetadataKey, null)
              trace(s"Group $groupId removed from the metadata cache and marked for deletion in $appendPartition.")
            }

            if (tombstones.nonEmpty) {
              try {
                // do not need to require acks since even if the tombstone is lost,
                // it will be appended again in the next purge cycle
                val records = MemoryRecords.withRecords(magicValue, 0L, compressionType, timestampType, tombstones.toArray: _*)
                partition.appendRecordsToLeader(records, origin = AppendOrigin.Coordinator, requiredAcks = 0,
                  requestLocal = requestLocal)

                offsetsRemoved += removedOffsets.size
                trace(s"Successfully appended ${tombstones.size} tombstones to $appendPartition for expired/deleted " +
                  s"offsets and/or metadata for group $groupId")
              } catch {
                case t: Throwable =>
                  error(s"Failed to append ${tombstones.size} tombstones to $appendPartition for expired/deleted " +
                    s"offsets and/or metadata for group $groupId.", t)
                // ignore and continue
              }
            }
          }

        case None =>
          info(s"BrokerId $brokerId is no longer a coordinator for the group $groupId. Proceeding cleanup for other alive groups")
      }
    }

    offsetsRemoved
  }

  /**
   * Complete pending transactional offset commits of the groups of `producerId` from the provided
   * `completedPartitions`. This method is invoked when a commit or abort marker is fully written
   * to the log. It may be invoked when a group lock is held by the caller, for instance when delayed
   * operations are completed while appending offsets for a group. Since we need to acquire one or
   * more group metadata locks to handle transaction completion, this operation is scheduled on
   * the scheduler thread to avoid deadlocks.
   */
  def scheduleHandleTxnCompletion(producerId: Long, completedPartitions: Set[Int], isCommit: Boolean): Unit = {
    scheduler.schedule(s"handleTxnCompletion-$producerId", () =>
      handleTxnCompletion(producerId, completedPartitions, isCommit))
  }

  private[group] def handleTxnCompletion(producerId: Long, completedPartitions: Set[Int], isCommit: Boolean): Unit = {
    val pendingGroups = groupsBelongingToPartitions(producerId, completedPartitions)
    pendingGroups.foreach { groupId =>
      getGroup(groupId) match {
        case Some(group) => group.inLock {
          if (!group.is(Dead)) {
            group.completePendingTxnOffsetCommit(producerId, isCommit)
            removeProducerGroup(producerId, groupId)
          }
        }
        case _ =>
          info(s"Group $groupId has moved away from $brokerId after transaction marker was written but before the " +
            s"cache was updated. The cache on the new group owner will be updated instead.")
      }
    }
  }

  private def addProducerGroup(producerId: Long, groupId: String) = openGroupsForProducer synchronized {
    openGroupsForProducer.getOrElseUpdate(producerId, mutable.Set.empty[String]).add(groupId)
  }

  private def removeProducerGroup(producerId: Long, groupId: String) = openGroupsForProducer synchronized {
    openGroupsForProducer.getOrElseUpdate(producerId, mutable.Set.empty[String]).remove(groupId)
    if (openGroupsForProducer(producerId).isEmpty)
      openGroupsForProducer.remove(producerId)
  }

  private def groupsBelongingToPartitions(producerId: Long, partitions: Set[Int]) = openGroupsForProducer synchronized {
    val (ownedGroups, _) = openGroupsForProducer.getOrElse(producerId, mutable.Set.empty[String])
      .partition(group => partitions.contains(partitionFor(group)))
    ownedGroups
  }

  private def removeGroupFromAllProducers(groupId: String): Unit = openGroupsForProducer synchronized {
    openGroupsForProducer.forKeyValue { (_, groups) =>
      groups.remove(groupId)
    }
  }

  /*
   * Check if the offset metadata length is valid
   */
  private def validateOffsetMetadataLength(metadata: String) : Boolean = {
    metadata == null || metadata.length() <= config.maxMetadataSize
  }


  def shutdown(): Unit = {
    shuttingDown.set(true)
    if (scheduler.isStarted)
      scheduler.shutdown()
    metrics.removeSensor(GroupMetadataManager.LoadTimeSensor)
    metrics.removeSensor(GroupMetadataManager.OffsetCommitsSensor)
    metrics.removeSensor(GroupMetadataManager.OffsetExpiredSensor)

    // TODO: clear the caches
  }

  /**
   * Check if the replica is local and return the message format version
   *
   * @param   partition  Partition of GroupMetadataTopic
   * @return  Some(MessageFormatVersion) if replica is local, None otherwise
   */
  private def getMagic(partition: Int): Option[Byte] =
    replicaManager.getMagic(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))

  /**
   * Add the partition into the owned list
   *
   * NOTE: this is for test only
   */
  private[group] def addPartitionOwnership(partition: Int): Unit = {
    inLock(partitionLock) {
      ownedPartitions.add(partition)
    }
  }

  /**
   * Add a partition to the loading partitions set. Return true if the partition was not
   * already loading.
   *
   * Visible for testing
   */
  private[group] def addLoadingPartition(partition: Int): Boolean = {
    inLock(partitionLock) {
      if (ownedPartitions.contains(partition)) {
        false
      } else {
        loadingPartitions.add(partition)
      }
    }
  }

}

/**
 * Messages stored for the group topic has versions for both the key and value fields. Key
 * version is used to indicate the type of the message (also to differentiate different types
 * of messages from being compacted together if they have the same field values); and value
 * version is used to evolve the messages within their data types:
 *
 * key version 0:       group consumption offset
 *    -> value version 0:       [offset, metadata, timestamp]
 *
 * key version 1:       group consumption offset
 *    -> value version 1:       [offset, metadata, commit_timestamp, expire_timestamp]
 *
 * key version 2:       group metadata
 *    -> value version 0:       [protocol_type, generation, protocol, leader, members]
 */
object GroupMetadataManager {
  // Metrics names
  val MetricsGroup: String = "group-coordinator-metrics"
  val LoadTimeSensor: String = "GroupPartitionLoadTime"
  val OffsetCommitsSensor: String = "OffsetCommits"
  val OffsetExpiredSensor: String = "OffsetExpired"

  /**
   * Generates the key for offset commit message for given (group, topic, partition)
   *
   * @param groupId the ID of the group to generate the key // 消费者组名
   * @param topicPartition the TopicPartition to generate the key // 主题 + 分区号
   * @return key for offset commit message
   */
  def offsetCommitKey(groupId: String, topicPartition: TopicPartition): Array[Byte] = {
    // 创建结构体，依次写入消费者组名、主题和分区号
    MessageUtil.toVersionPrefixedBytes(OffsetCommitKey.HIGHEST_SUPPORTED_VERSION,
      new OffsetCommitKey()
        .setGroup(groupId)
        .setTopic(topicPartition.topic)
        .setPartition(topicPartition.partition))
  }

  /**
   * Generates the key for group metadata message for given group
   *
   * @param groupId the ID of the group to generate the key
   * @return key bytes for group metadata message
   */
  def groupMetadataKey(groupId: String): Array[Byte] = {
    // 构造一个ByteBuffer对象，容纳version和key数据
    MessageUtil.toVersionPrefixedBytes(GroupMetadataKeyData.HIGHEST_SUPPORTED_VERSION,
      new GroupMetadataKeyData()
        .setGroup(groupId))
  }

  /**
   * Generates the payload for offset commit message from given offset and metadata
   *
   * @param offsetAndMetadata consumer's current offset and metadata
   * @param apiVersion the api version
   * @return payload for offset commit message
   */
  def offsetCommitValue(offsetAndMetadata: OffsetAndMetadata,
                        apiVersion: ApiVersion): Array[Byte] = {
    // 确定消息格式版本以及创建对应的结构体对象
    val version =
      if (apiVersion < KAFKA_2_1_IV0 || offsetAndMetadata.expireTimestamp.nonEmpty) 1.toShort
      else if (apiVersion < KAFKA_2_1_IV1) 2.toShort
      else 3.toShort
    // 依次写入位移值、Leader Epoch值、自定义元数据以及时间戳
    MessageUtil.toVersionPrefixedBytes(version, new OffsetCommitValue()
      .setOffset(offsetAndMetadata.offset)
      .setMetadata(offsetAndMetadata.metadata)
      .setCommitTimestamp(offsetAndMetadata.commitTimestamp)
      .setLeaderEpoch(offsetAndMetadata.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
      // version 1 has a non empty expireTimestamp field
      .setExpireTimestamp(offsetAndMetadata.expireTimestamp.getOrElse(OffsetCommitRequest.DEFAULT_TIMESTAMP))
    )
  }

  /**
   * Generates the payload for group metadata message from given offset and metadata
   * assuming the generation id, selected protocol, leader and member assignment are all available
   *
   * @param groupMetadata current group metadata
   * @param assignment the assignment for the rebalancing generation
   * @param apiVersion the api version
   * @return payload for offset commit message
   */
  def groupMetadataValue(groupMetadata: GroupMetadata, // 消费者组元数据对象
                         assignment: Map[String, Array[Byte]], // 分区消费分配方案
                         apiVersion: ApiVersion): Array[Byte] = {  // Kafka API版本号

    // 确定消息格式版本以及格式结构
    val version =
      if (apiVersion < KAFKA_0_10_1_IV0) 0.toShort
      else if (apiVersion < KAFKA_2_1_IV0) 1.toShort
      else if (apiVersion < KAFKA_2_3_IV0) 2.toShort
      else 3.toShort

    // 写入各个成员的元数据信息
    MessageUtil.toVersionPrefixedBytes(version, new GroupMetadataValue()
      // 包括协议类型、Generation ID、分区分配策略和Leader成员ID
      // 对于普通的消费者组而言，协议类型就是"consumer"字符串
      .setProtocolType(groupMetadata.protocolType.getOrElse(""))
      .setGeneration(groupMetadata.generationId)
      // 分区分配策略可能是"range""round-robin"等
      .setProtocol(groupMetadata.protocolName.orNull)
      .setLeader(groupMetadata.leaderOrNull)
      // 写入最近一次状态变更时间戳
      .setCurrentStateTimestamp(groupMetadata.currentStateTimestampOrDefault)
      .setMembers(groupMetadata.allMemberMetadata.map { memberMetadata =>
        // 依次写入消费者组主要的元数据信息
        // 包括成员ID、client.id、主机名以及会话超时时间
      new GroupMetadataValue.MemberMetadata()
          .setMemberId(memberMetadata.memberId)
          .setClientId(memberMetadata.clientId)
          .setClientHost(memberMetadata.clientHost)
          .setSessionTimeout(memberMetadata.sessionTimeoutMs)
          // 写入Rebalance超时时间
          .setRebalanceTimeout(memberMetadata.rebalanceTimeoutMs)
          // 写入用于静态消费者组管理的Group Instance ID
          .setGroupInstanceId(memberMetadata.groupInstanceId.orNull)
          // The group is non-empty, so the current protocol must be defined
          // 必须定义分区分配策略，否则抛出异常
          .setSubscription(groupMetadata.protocolName.map(memberMetadata.metadata)
            .getOrElse(throw new IllegalStateException("Attempted to write non-empty group metadata with no defined protocol.")))
          // 写入成员消费订阅信息
          // 写入成员消费分配信息
          .setAssignment(assignment.getOrElse(memberMetadata.memberId,
            throw new IllegalStateException(s"Attempted to write member ${memberMetadata.memberId} of group ${groupMetadata.groupId} with no assignment.")))
      }.asJava))
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an OffsetKey or GroupMetadataKey object from the message
   */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    if (version >= OffsetCommitKey.LOWEST_SUPPORTED_VERSION && version <= OffsetCommitKey.HIGHEST_SUPPORTED_VERSION) {
      // version 0 and 1 refer to offset
      val key = new OffsetCommitKey(new ByteBufferAccessor(buffer), version)
      OffsetKey(version, GroupTopicPartition(key.group, new TopicPartition(key.topic, key.partition)))
    } else if (version >= GroupMetadataKeyData.LOWEST_SUPPORTED_VERSION && version <= GroupMetadataKeyData.HIGHEST_SUPPORTED_VERSION) {
      // version 2 refers to group metadata
      val key = new GroupMetadataKeyData(new ByteBufferAccessor(buffer), version)
      GroupMetadataKey(version, key.group)
    } else throw new IllegalStateException(s"Unknown group metadata message version: $version")
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  def readOffsetMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    // tombstone
    if (buffer == null) null
    else {
      val version = buffer.getShort
      if (version >= OffsetCommitValue.LOWEST_SUPPORTED_VERSION && version <= OffsetCommitValue.HIGHEST_SUPPORTED_VERSION) {
        val value = new OffsetCommitValue(new ByteBufferAccessor(buffer), version)
        OffsetAndMetadata(
          offset = value.offset,
          leaderEpoch = if (value.leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) Optional.empty() else Optional.of(value.leaderEpoch),
          metadata = value.metadata,
          commitTimestamp = value.commitTimestamp,
          expireTimestamp = if (value.expireTimestamp == OffsetCommitRequest.DEFAULT_TIMESTAMP) None else Some(value.expireTimestamp))
      } else throw new IllegalStateException(s"Unknown offset message version: $version")
    }
  }

  /**
   * Decodes the group metadata messages' payload and retrieves its member metadata from it
   *
   * @param groupId The ID of the group to be read
   * @param buffer input byte-buffer
   * @param time the time instance to use
   * @return a group metadata object from the message
   */
  def readGroupMessageValue(groupId: String, buffer: ByteBuffer, time: Time): GroupMetadata = {
    // tombstone
    if (buffer == null) null
    else {
      val version = buffer.getShort
      if (version >= GroupMetadataValue.LOWEST_SUPPORTED_VERSION && version <= GroupMetadataValue.HIGHEST_SUPPORTED_VERSION) {
        val value = new GroupMetadataValue(new ByteBufferAccessor(buffer), version)
        val members = value.members.asScala.map { memberMetadata =>
          new MemberMetadata(
            memberId = memberMetadata.memberId,
            groupInstanceId = Option(memberMetadata.groupInstanceId),
            clientId = memberMetadata.clientId,
            clientHost = memberMetadata.clientHost,
            rebalanceTimeoutMs = if (version == 0) memberMetadata.sessionTimeout else memberMetadata.rebalanceTimeout,
            sessionTimeoutMs = memberMetadata.sessionTimeout,
            protocolType = value.protocolType,
            supportedProtocols = List((value.protocol, memberMetadata.subscription)),
            assignment = memberMetadata.assignment)
        }
        GroupMetadata.loadGroup(
          groupId = groupId,
          initialState = if (members.isEmpty) Empty else Stable,
          generationId = value.generation,
          protocolType = value.protocolType,
          protocolName = value.protocol,
          leaderId = value.leader,
          currentStateTimestamp = if (value.currentStateTimestamp == -1) None else Some(value.currentStateTimestamp),
          members = members,
          time = time)
      } else throw new IllegalStateException(s"Unknown group metadata message version: $version")
    }
  }

  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
  class OffsetsMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is an offset record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case offsetKey: OffsetKey =>
          val groupTopicPartition = offsetKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value)).toString
          output.write(groupTopicPartition.toString.getBytes(StandardCharsets.UTF_8))
          output.write("::".getBytes(StandardCharsets.UTF_8))
          output.write(formattedValue.getBytes(StandardCharsets.UTF_8))
          output.write("\n".getBytes(StandardCharsets.UTF_8))
        case _ => // no-op
      }
    }
  }

  // Formatter for use with tools to read group metadata history
  class GroupMetadataMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is a group metadata record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case groupMetadataKey: GroupMetadataKey =>
          val groupId = groupMetadataKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readGroupMessageValue(groupId, ByteBuffer.wrap(value), Time.SYSTEM).toString
          output.write(groupId.getBytes(StandardCharsets.UTF_8))
          output.write("::".getBytes(StandardCharsets.UTF_8))
          output.write(formattedValue.getBytes(StandardCharsets.UTF_8))
          output.write("\n".getBytes(StandardCharsets.UTF_8))
        case _ => // no-op
      }
    }
  }

  /**
   * Exposed for printing records using [[kafka.tools.DumpLogSegments]]
   */
  def formatRecordKeyAndValue(record: Record): (Option[String], Option[String]) = {
    if (!record.hasKey) {
      throw new KafkaException("Failed to decode message using offset topic decoder (message had a missing key)")
    } else {
      GroupMetadataManager.readMessageKey(record.key) match {
        case offsetKey: OffsetKey => parseOffsets(offsetKey, record.value)
        case groupMetadataKey: GroupMetadataKey => parseGroupMetadata(groupMetadataKey, record.value)
        case _ => throw new KafkaException("Failed to decode message using offset topic decoder (message had an invalid key)")
      }
    }
  }

  private def parseOffsets(offsetKey: OffsetKey, payload: ByteBuffer): (Option[String], Option[String]) = {
    val groupId = offsetKey.key.group
    val topicPartition = offsetKey.key.topicPartition
    val keyString = s"offset_commit::group=$groupId,partition=$topicPartition"

    val offset = GroupMetadataManager.readOffsetMessageValue(payload)
    val valueString = if (offset == null) {
      "<DELETE>"
    } else {
      if (offset.metadata.isEmpty)
        s"offset=${offset.offset}"
      else
        s"offset=${offset.offset},metadata=${offset.metadata}"
    }

    (Some(keyString), Some(valueString))
  }

  private def parseGroupMetadata(groupMetadataKey: GroupMetadataKey, payload: ByteBuffer): (Option[String], Option[String]) = {
    val groupId = groupMetadataKey.key
    val keyString = s"group_metadata::group=$groupId"

    val group = GroupMetadataManager.readGroupMessageValue(groupId, payload, Time.SYSTEM)
    val valueString = if (group == null)
      "<DELETE>"
    else {
      val protocolType = group.protocolType.getOrElse("")

      val assignment = group.allMemberMetadata.map { member =>
        if (protocolType == ConsumerProtocol.PROTOCOL_TYPE) {
          val partitionAssignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.assignment))
          val userData = Option(partitionAssignment.userData)
            .map(Utils.toArray)
            .map(hex)
            .getOrElse("")

          if (userData.isEmpty)
            s"${member.memberId}=${partitionAssignment.partitions}"
          else
            s"${member.memberId}=${partitionAssignment.partitions}:$userData"
        } else {
          s"${member.memberId}=${hex(member.assignment)}"
        }
      }.mkString("{", ",", "}")

      Json.encodeAsString(Map(
        "protocolType" -> protocolType,
        "protocol" -> group.protocolName.orNull,
        "generationId" -> group.generationId,
        "assignment" -> assignment
      ).asJava)
    }
    (Some(keyString), Some(valueString))
  }

  private def hex(bytes: Array[Byte]): String = {
    if (bytes.isEmpty)
      ""
    else
      "%X".format(BigInt(1, bytes))
  }

}

// 封装 < 消费者组名，主题，分区号 > 的三元组
case class GroupTopicPartition(group: String, topicPartition: TopicPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicPartition(topic, partition))

  override def toString: String =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)
}

trait BaseKey{
  def version: Short // 消息格式版本
  def key: Any       // 消息key
}

case class OffsetKey(version: Short, key: GroupTopicPartition) extends BaseKey {

  override def toString: String = key.toString
}

case class GroupMetadataKey(version: Short, key: String) extends BaseKey {

  override def toString: String = key
}

