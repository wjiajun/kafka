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

package kafka.server.metadata

import java.util
import java.util.Collections
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.admin.BrokerMetadata

import scala.collection.{Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import kafka.cluster.{Broker, EndPoint}
import kafka.api._
import kafka.controller.StateChangeLogger
import kafka.server.MetadataCache
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import kafka.utils.Implicits._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition, Uuid}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataResponse, UpdateMetadataRequest}
import org.apache.kafka.common.security.auth.SecurityProtocol

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
class ZkMetadataCache(brokerId: Int) extends MetadataCache with Logging {

  // 保护它写入的锁对象
  private val partitionMetadataLock = new ReentrantReadWriteLock()
  //this is the cache state. every MetadataSnapshot instance is immutable, and updates (performed under a lock)
  //replace the value with a completely new one. this means reads (which are not under any lock) need to grab
  //the value of this var (into a val) ONCE and retain that read copy for the duration of their operation.
  //multiple reads of this value risk getting different snapshots.
  /**
   * 保存了实际的元数据信息
   */
  @volatile private var metadataSnapshot: MetadataSnapshot = MetadataSnapshot(partitionStates = mutable.AnyRefMap.empty,
    topicIds = Map.empty, controllerId = None, aliveBrokers = mutable.LongMap.empty, aliveNodes = mutable.LongMap.empty)

  /**
   * 日志输出
   */
  this.logIdent = s"[MetadataCache brokerId=$brokerId] "
  private val stateChangeLogger = new StateChangeLogger(brokerId, inControllerContext = false, None)

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here. Relatedly, `brokers` is
  // `List[Integer]` instead of `List[Int]` to avoid a collection copy.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def maybeFilterAliveReplicas(snapshot: MetadataSnapshot,
                                       brokers: java.util.List[Integer],
                                       listenerName: ListenerName,
                                       filterUnavailableEndpoints: Boolean): java.util.List[Integer] = {
    if (!filterUnavailableEndpoints) {
      brokers
    } else {
      val res = new util.ArrayList[Integer](math.min(snapshot.aliveBrokers.size, brokers.size))
      for (brokerId <- brokers.asScala) {
        if (hasAliveEndpoint(snapshot, brokerId, listenerName))
          res.add(brokerId)
      }
      res
    }
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // If errorUnavailableListeners=true, return LISTENER_NOT_FOUND if listener is missing on the broker.
  // Otherwise, return LEADER_NOT_AVAILABLE for broker unavailable and missing listener (Metadata response v5 and below).
  private def getPartitionMetadata(snapshot: MetadataSnapshot, topic: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean,
                                   errorUnavailableListeners: Boolean): Option[Iterable[MetadataResponsePartition]] = {
    // 获取每个topic的分区集合
    snapshot.partitionStates.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        val topicPartition = new TopicPartition(topic, partitionId.toInt)
        val leaderBrokerId = partitionState.leader
        val leaderEpoch = partitionState.leaderEpoch
        // 获取leader所在的node
        val maybeLeader = getAliveEndpoint(snapshot, leaderBrokerId, listenerName)

        // 获取分区的ar集合
        val replicas = partitionState.replicas
        // 获取ar对应的可用副本
        val filteredReplicas = maybeFilterAliveReplicas(snapshot, replicas, listenerName, errorUnavailableEndpoints)

        // 获取分区的isr集合
        val isr = partitionState.isr
        // 过滤Isr集合中的可用副本
        val filteredIsr = maybeFilterAliveReplicas(snapshot, isr, listenerName, errorUnavailableEndpoints)

        val offlineReplicas = partitionState.offlineReplicas

        maybeLeader match {
          case None =>
            val error = if (!snapshot.aliveBrokers.contains(leaderBrokerId)) { // we are already holding the read lock
              debug(s"Error while fetching metadata for $topicPartition: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for $topicPartition: listener $listenerName " +
                s"not found on leader $leaderBrokerId")
              if (errorUnavailableListeners) Errors.LISTENER_NOT_FOUND else Errors.LEADER_NOT_AVAILABLE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId.toInt)
              .setLeaderId(MetadataResponse.NO_LEADER_ID)
              .setLeaderEpoch(leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)

          case Some(_) =>
            // 检查AR副本是否都是可用的
            val error = if (filteredReplicas.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.asScala.filterNot(filteredReplicas.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
              // 检查ISR副本是否都是可用的
            } else if (filteredIsr.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.asScala.filterNot(filteredIsr.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else {
              Errors.NONE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId.toInt)
              .setLeaderId(maybeLeader.map(_.id()).getOrElse(MetadataResponse.NO_LEADER_ID))
              .setLeaderEpoch(leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)
        }
      }
    }
  }

  /**
   * Check whether a broker is alive and has a registered listener matching the provided name.
   * This method was added to avoid unnecessary allocations in [[maybeFilterAliveReplicas]], which is
   * a hotspot in metadata handling.
   */
  private def hasAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Boolean = {
    snapshot.aliveNodes.get(brokerId).exists(_.contains(listenerName))
  }

  /**
   * Get the endpoint matching the provided listener if the broker is alive. Note that listeners can
   * be added dynamically, so a broker with a missing listener could be a transient error.
   *
   * @return None if broker is not alive or if the broker does not have a listener named `listenerName`.
   */
  private def getAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Option[Node] = {
    snapshot.aliveNodes.get(brokerId).flatMap(_.get(listenerName))
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String],
                       listenerName: ListenerName,
                       errorUnavailableEndpoints: Boolean = false,
                       errorUnavailableListeners: Boolean = false): Seq[MetadataResponseTopic] = {
    val snapshot = metadataSnapshot
    topics.toSeq.flatMap { topic =>
      getPartitionMetadata(snapshot, topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners).map { partitionMetadata =>
        new MetadataResponseTopic()
          .setErrorCode(Errors.NONE.code)
          .setName(topic)
          .setTopicId(snapshot.topicIds.getOrElse(topic, Uuid.ZERO_UUID))
          .setIsInternal(Topic.isInternal(topic))
          .setPartitions(partitionMetadata.toBuffer.asJava)
      }
    }
  }

  def topicNamesToIds(): util.Map[String, Uuid] = {
    Collections.unmodifiableMap(metadataSnapshot.topicIds.asJava)
  }

  def topicIdsToNames(): util.Map[Uuid, String] = {
    Collections.unmodifiableMap(metadataSnapshot.topicNames.asJava)
  }

  /**
   * This method returns a map from topic names to IDs and a map from topic IDs to names
   */
  def topicIdInfo(): (util.Map[String, Uuid], util.Map[Uuid, String]) = {
    val snapshot = metadataSnapshot
    (Collections.unmodifiableMap(snapshot.topicIds.asJava), Collections.unmodifiableMap(snapshot.topicNames.asJava))
  }

  override def getAllTopics(): Set[String] = {
    getAllTopics(metadataSnapshot)
  }

  override def getTopicPartitions(topicName: String): Set[TopicPartition] = {
    metadataSnapshot.partitionStates.getOrElse(topicName, Map.empty).values.
      map(p => new TopicPartition(topicName, p.partitionIndex())).toSet
  }

  // 返回当前集群元数据缓存中的所有主题
  private def getAllTopics(snapshot: MetadataSnapshot): Set[String] = {
    snapshot.partitionStates.keySet
  }

  private def getAllPartitions(snapshot: MetadataSnapshot): Map[TopicPartition, UpdateMetadataPartitionState] = {
    snapshot.partitionStates.flatMap { case (topic, partitionStates) =>
      partitionStates.map { case (partition, state) => (new TopicPartition(topic, partition.toInt), state) }
    }.toMap
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    topics.diff(metadataSnapshot.partitionStates.keySet)
  }

  override def hasAliveBroker(brokerId: Int): Boolean = metadataSnapshot.aliveBrokers.contains(brokerId)

  override def getAliveBrokers(): Iterable[BrokerMetadata] = {
    metadataSnapshot.aliveBrokers.values.map(b => new BrokerMetadata(b.id, b.rack))
  }

  override def getAliveBrokerNode(brokerId: Int, listenerName: ListenerName): Option[Node] = {
    metadataSnapshot.aliveBrokers.get(brokerId).flatMap(_.getNode(listenerName))
  }

  override def getAliveBrokerNodes(listenerName: ListenerName): Iterable[Node] = {
    metadataSnapshot.aliveBrokers.values.flatMap(_.getNode(listenerName))
  }

  def getTopicId(topicName: String): Uuid = {
    metadataSnapshot.topicIds.getOrElse(topicName, Uuid.ZERO_UUID)
  }

  def getTopicName(topicId: Uuid): Option[String] = {
    metadataSnapshot.topicNames.get(topicId)
  }

  private def addOrUpdatePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                                       topic: String,
                                       partitionId: Int,
                                       stateInfo: UpdateMetadataPartitionState): Unit = {
    val infos = partitionStates.getOrElseUpdate(topic, mutable.LongMap.empty)
    infos(partitionId) = stateInfo
  }

  // 获取给定主题分区的详细数据信息。如果没有找到对应记录，返回None
  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataPartitionState] = {
    metadataSnapshot.partitionStates.get(topic).flatMap(_.get(partitionId))
  }

  def numPartitions(topic: String): Option[Int] = {
    metadataSnapshot.partitionStates.get(topic).map(_.size)
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    val snapshot = metadataSnapshot
    snapshot.partitionStates.get(topic).flatMap(_.get(partitionId)) map { partitionInfo =>
      val leaderId = partitionInfo.leader

      snapshot.aliveNodes.get(leaderId) match {
        case Some(nodeMap) =>
          nodeMap.getOrElse(listenerName, Node.noNode)
        case None =>
          Node.noNode
      }
    }
  }

  def getPartitionReplicaEndpoints(tp: TopicPartition, listenerName: ListenerName): Map[Int, Node] = {
    // 使用局部变量获取当前元数据缓存
    val snapshot = metadataSnapshot
    // 获取给定主题分区的数据
    snapshot.partitionStates.get(tp.topic).flatMap(_.get(tp.partition)).map { partitionInfo =>
      // 拿到副本Id列表
      val replicaIds = partitionInfo.replicas
      replicaIds.asScala
        .map(replicaId => replicaId.intValue() -> {
          // 获取副本所在的Broker Id
          snapshot.aliveBrokers.get(replicaId.longValue()) match {
            case Some(broker) =>
              // 根据Broker Id去获取对应的Broker节点对象
              broker.getNode(listenerName).getOrElse(Node.noNode())
            case None =>
              // 如果找不到节点
              Node.noNode()
          }
        }).toMap
        .filter(pair => pair match {
          case (_, node) => !node.isEmpty
        })
    }.getOrElse(Map.empty[Int, Node])
  }

  def getControllerId: Option[Int] = metadataSnapshot.controllerId

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val snapshot = metadataSnapshot
    val nodes = snapshot.aliveNodes.flatMap { case (id, nodesByListener) =>
      nodesByListener.get(listenerName).map { node =>
        id -> node
      }
    }

    def node(id: Integer): Node = {
      nodes.getOrElse(id.toLong, new Node(id, "", -1))
    }

    val partitions = getAllPartitions(snapshot)
      .filter { case (_, state) => state.leader != LeaderAndIsr.LeaderDuringDelete }
      .map { case (tp, state) =>
        new PartitionInfo(tp.topic, tp.partition, node(state.leader),
          state.replicas.asScala.map(node).toArray,
          state.isr.asScala.map(node).toArray,
          state.offlineReplicas.asScala.map(node).toArray)
      }
    val unauthorizedTopics = Collections.emptySet[String]
    val internalTopics = getAllTopics(snapshot).filter(Topic.isInternal).asJava
    new Cluster(clusterId, nodes.values.toBuffer.asJava,
      partitions.toBuffer.asJava,
      unauthorizedTopics, internalTopics,
      snapshot.controllerId.map(id => node(id)).orNull)
  }

  /**
   * 读取 UpdateMetadataRequest 请求中的分区数据，然后更新本地元数据缓存
   *
   * @param correlationId
   * @param updateMetadataRequest
   * @return
   */
  // This method returns the deleted TopicPartitions received from UpdateMetadataRequest
  def updateMetadata(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest): Seq[TopicPartition] = {
    inWriteLock(partitionMetadataLock) {

      // 保存存活Broker对象。Key是Broker ID，Value是Broker对象
      val aliveBrokers = new mutable.LongMap[Broker](metadataSnapshot.aliveBrokers.size)
      // 保存存活节点对象。Key是Broker ID，Value是监听器->节点对象
      val aliveNodes = new mutable.LongMap[collection.Map[ListenerName, Node]](metadataSnapshot.aliveNodes.size)

      // 从UpdateMetadataRequest中获取Controller所在的Broker ID
      // 如果当前没有Controller，赋值为None
      val controllerIdOpt = updateMetadataRequest.controllerId match {
        case id if id < 0 => None
        case id => Some(id)
      }

      // 遍历UpdateMetadataRequest请求中的所有存活Broker对象
      updateMetadataRequest.liveBrokers.forEach { broker =>
        // `aliveNodes` is a hot path for metadata requests for large clusters, so we use java.util.HashMap which
        // is a bit faster than scala.collection.mutable.HashMap. When we drop support for Scala 2.10, we could
        // move to `AnyRefMap`, which has comparable performance.
        val nodes = new java.util.HashMap[ListenerName, Node]
        val endPoints = new mutable.ArrayBuffer[EndPoint]
        // 遍历它的所有EndPoint类型，也就是为Broker配置的监听器
        broker.endpoints.forEach { ep =>
          val listenerName = new ListenerName(ep.listener)
          endPoints += new EndPoint(ep.host, ep.port, listenerName, SecurityProtocol.forId(ep.securityProtocol))
          // 将<监听器，Broker节点对象>对保存起来
          nodes.put(listenerName, new Node(broker.id, ep.host, ep.port))
        }
        // 将Broker加入到存活Broker对象集合
        aliveBrokers(broker.id) = Broker(broker.id, endPoints, Option(broker.rack))
        // 将Broker节点加入到存活节点对象集合
        aliveNodes(broker.id) = nodes.asScala
      }

      // 使用上一部分中的存活Broker节点对象，
      // 获取当前Broker所有的<监听器,节点>对
      aliveNodes.get(brokerId).foreach { listenerMap =>
        val listeners = listenerMap.keySet
        // 如果发现当前Broker配置的监听器与其他Broker有不同之处，记录错误日志
        if (!aliveNodes.values.forall(_.keySet == listeners))
          error(s"Listeners are not identical across brokers: $aliveNodes")
      }

      val topicIds = mutable.Map.empty[String, Uuid]
      topicIds ++= metadataSnapshot.topicIds
      val (newTopicIds, newZeroIds) = updateMetadataRequest.topicStates().asScala
        .map(topicState => (topicState.topicName(), topicState.topicId()))
        .partition { case (_, topicId) => topicId != Uuid.ZERO_UUID }
      newZeroIds.foreach { case (zeroIdTopic, _) => topicIds.remove(zeroIdTopic) }
      topicIds ++= newTopicIds.toMap

      // 构造已删除分区数组，将其作为方法返回结果
      val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
      // UpdateMetadataRequest请求没有携带任何分区信息
      if (!updateMetadataRequest.partitionStates.iterator.hasNext) {
        // 构造新的MetadataSnapshot对象，使用之前的分区信息和新的Broker列表信息
        metadataSnapshot = MetadataSnapshot(metadataSnapshot.partitionStates, topicIds.toMap, controllerIdOpt, aliveBrokers, aliveNodes)
      } else {
        //since kafka may do partial metadata updates, we start by copying the previous state
        val partitionStates = new mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]](metadataSnapshot.partitionStates.size)
        // 备份现有元数据缓存中的分区数据
        metadataSnapshot.partitionStates.forKeyValue { (topic, oldPartitionStates) =>
          val copy = new mutable.LongMap[UpdateMetadataPartitionState](oldPartitionStates.size)
          copy ++= oldPartitionStates
          partitionStates(topic) = copy
        }

        val traceEnabled = stateChangeLogger.isTraceEnabled
        val controllerId = updateMetadataRequest.controllerId
        val controllerEpoch = updateMetadataRequest.controllerEpoch
        // 获取UpdateMetadataRequest请求中携带的所有分区数据
        val newStates = updateMetadataRequest.partitionStates.asScala
        // 遍历分区数据
        newStates.foreach { state =>
          // per-partition logging here can be very expensive due going through all partitions in the cluster
          val tp = new TopicPartition(state.topicName, state.partitionIndex)
          // 如果分区处于被删除过程中
          if (state.leader == LeaderAndIsr.LeaderDuringDelete) {
            // 将分区从元数据缓存中移除
            removePartitionInfo(partitionStates, topicIds, tp.topic, tp.partition)
            if (traceEnabled)
              stateChangeLogger.trace(s"Deleted partition $tp from metadata cache in response to UpdateMetadata " +
                s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
            // 将分区加入到返回结果数据
            deletedPartitions += tp
          } else {
            // 将分区加入到元数据缓存
            addOrUpdatePartitionInfo(partitionStates, tp.topic, tp.partition, state)
            if (traceEnabled)
              stateChangeLogger.trace(s"Cached leader info $state for partition $tp in response to " +
                s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
          }
        }
        val cachedPartitionsCount = newStates.size - deletedPartitions.size
        stateChangeLogger.info(s"Add $cachedPartitionsCount partitions and deleted ${deletedPartitions.size} partitions from metadata cache " +
          s"in response to UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")

        // 使用更新过的分区元数据，和第一部分计算的存活Broker列表及节点列表，构建最新的元数据缓存
        metadataSnapshot = MetadataSnapshot(partitionStates, topicIds.toMap, controllerIdOpt, aliveBrokers, aliveNodes)
      }
      // 返回已删除分区列表数组
      deletedPartitions
    }
  }

  // 判断给定主题是否包含在元数据缓存中
  def contains(topic: String): Boolean = {
    metadataSnapshot.partitionStates.contains(topic)
  }

  // 判断给定主题分区是否包含在元数据缓存中
  def contains(tp: TopicPartition): Boolean = getPartitionInfo(tp.topic, tp.partition).isDefined

  private def removePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                                  topicIds: mutable.Map[String, Uuid], topic: String, partitionId: Int): Boolean = {
    partitionStates.get(topic).exists { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) {
        partitionStates.remove(topic)
        topicIds.remove(topic)
      }
      true
    }
  }

  // partitionStates：这是一个 Map 类型。Key 是主题名称，Value 又是一个 Map 类型，其 Key 是分区号，Value 是一个 UpdateMetadataPartitionState 类型的字段。
  //                  UpdateMetadataPartitionState 类型是 UpdateMetadataRequest 请求内部所需的数据结构。
  // controllerId：Controller 所在 Broker 的 ID。
  // aliveBrokers：当前集群中所有存活着的 Broker 对象列表。
  // aliveNodes：这也是一个 Map 的 Map 类型。其 Key 是 Broker ID 序号，Value 是Map 类型，其 Key 是 ListenerName，即 Broker 监听器类型，而 Value 是 Broker 节点对象。
  case class MetadataSnapshot(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                              topicIds: Map[String, Uuid],
                              controllerId: Option[Int],// Controller 所在 Broker 的 ID
                              aliveBrokers: mutable.LongMap[Broker], // 当前集群中所有存活着的 Broker 对象列表
                              aliveNodes: mutable.LongMap[collection.Map[ListenerName, Node]]) { // 其 Key 是 Broker ID 序号，Value 是Map 类型，其 Key 是 ListenerName，即 Broker 监听器类型，而 Value 是 Broker 节点对象。
    val topicNames: Map[Uuid, String] = topicIds.map { case (topicName, topicId) => (topicId, topicName) }
  }
}
