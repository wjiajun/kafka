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

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import kafka.common.OffsetAndMetadata
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import kafka.utils.Implicits._
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.Time

import scala.collection.{Seq, immutable, mutable}
import scala.jdk.CollectionConverters._

private[group] sealed trait GroupState {
  val validPreviousStates: Set[GroupState]
}

/**
 * Group is preparing to rebalance
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to sync group with REBALANCE_IN_PROGRESS
 *         remove member on leave group request
 *         park join group requests from new or existing members until all expected members have joined
 *         allow offset commits from previous generation
 *         allow offset fetch requests
 * transition: some members have joined by the timeout => CompletingRebalance
 *             all members have left the group => Empty
 *             group is removed by partition emigration => Dead
 */
private[group] case object PreparingRebalance extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(Stable, CompletingRebalance, Empty)
}

/**
 * Group is awaiting state assignment from the leader
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to offset commits with REBALANCE_IN_PROGRESS
 *         park sync group requests from followers until transition to Stable
 *         allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 *             join group from new member or existing member with updated metadata => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             member failure detected => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object CompletingRebalance extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(PreparingRebalance)
}

/**
 * Group is stable
 *
 * action: respond to member heartbeats normally
 *         respond to sync group from any member with current assignment
 *         respond to join group from followers with matching metadata with current group metadata
 *         allow offset commits from member of current generation
 *         allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             leader join-group received => PreparingRebalance
 *             follower join-group with new metadata => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object Stable extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(CompletingRebalance)
}

/**
 * Group has no more members and its metadata is being removed
 *
 * action: respond to join group with UNKNOWN_MEMBER_ID
 *         respond to sync group with UNKNOWN_MEMBER_ID
 *         respond to heartbeat with UNKNOWN_MEMBER_ID
 *         respond to leave group with UNKNOWN_MEMBER_ID
 *         respond to offset commit with UNKNOWN_MEMBER_ID
 *         allow offset fetch requests
 * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
 */
private[group] case object Dead extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead)
}

/**
  * Group has no more members, but lingers until all offsets have expired. This state
  * also represents groups which use Kafka only for offset commits and have no members.
  *
  * action: respond normally to join group from new members
  *         respond to sync group with UNKNOWN_MEMBER_ID
  *         respond to heartbeat with UNKNOWN_MEMBER_ID
  *         respond to leave group with UNKNOWN_MEMBER_ID
  *         respond to offset commit with UNKNOWN_MEMBER_ID
  *         allow offset fetch requests
  * transition: last offsets removed in periodic expiration task => Dead
  *             join group from a new member => PreparingRebalance
  *             group is removed by partition emigration => Dead
  *             group is removed by expiration => Dead
  */
private[group] case object Empty extends GroupState {
  // 合法前置状态
  val validPreviousStates: Set[GroupState] = Set(PreparingRebalance)
}


private object GroupMetadata extends Logging {

  def loadGroup(groupId: String,
                initialState: GroupState,
                generationId: Int,
                protocolType: String,
                protocolName: String,
                leaderId: String,
                currentStateTimestamp: Option[Long],
                members: Iterable[MemberMetadata],
                time: Time): GroupMetadata = {
    val group = new GroupMetadata(groupId, initialState, time)
    group.generationId = generationId
    group.protocolType = if (protocolType == null || protocolType.isEmpty) None else Some(protocolType)
    group.protocolName = Option(protocolName)
    group.leaderId = Option(leaderId)
    group.currentStateTimestamp = currentStateTimestamp
    members.foreach { member =>
      group.add(member, null)
      info(s"Loaded member $member in group $groupId with generation ${group.generationId}.")
    }
    group.subscribedTopics = group.computeSubscribedTopics()
    group
  }

  private val MemberIdDelimiter = "-"
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(groupId: String,// 组ID信息，即group.id参数值
                         protocolType: String,// 消费者组的协议类型
                         state: String)// 消费者组的状态

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(state: String, // 消费者组状态
                        protocolType: String, // 协议类型
                        protocol: String, // 消费者组选定的分区分配策略
                        members: List[MemberSummary]) // 成员元数据

/**
  * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
  * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
  * information of the commit record offset, compaction of the offsets topic itself may result in the wrong offset commit
  * being materialized.
  *
  * 保存写入到位移主题中的消息的位移值，以及其他元数据信息。这个类的主要职责就是保存位移值。
  * appendedBatchOffset：保存的是位移主题消息自己的位移值；
  * offsetAndMetadata：保存的是位移提交消息中保存的消费者组的位移值。
  */
case class CommitRecordMetadataAndOffset(appendedBatchOffset: Option[Long], offsetAndMetadata: OffsetAndMetadata) {
  def olderThan(that: CommitRecordMetadataAndOffset): Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get
}

/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 *
 *  Empty、PreparingRebalance、CompletingRebalance、Stable 和 Dead。
 *    Empty
 *      表示当前无成员的消费者组；
 *    PreparingRebalance
 *      表示正在执行加入组操作的消费者组；
 *    CompletingRebalance
 *      表示等待 Leader 成员制定分配方案的消费者组；
 *    Stable
 *      表示已完成 Rebalance 操作可正常工作的消费者组；
 *    Dead
 *      表示当前无成员且元数据信息被删除的消费者组。
 *
 *    currentStateTimestamp：
 *      记录最近一次状态变更的时间戳，用于确定位移主题中的过期消息。位移主题中的消息也要遵循 Kafka 的留存策略，所有当前时间与该字段的差值超过了留存阈值的消息都被视为“已过期”（Expired）。
 *    generationId：
 *      消费组 Generation 号。Generation 等同于消费者组执行过Rebalance 操作的次数，每次执行 Rebalance 时，Generation 数都要加 1。
 *    leaderId：
 *      消费者组中 Leader 成员的 Member ID 信息。当消费者组执行 Rebalance过程时，需要选举一个成员作为 Leader，负责为所有成员制定分区分配方案。在Rebalance 早期阶段，这个 Leader 可能尚未被选举出来。这就是，leaderId 字段是Option 类型的原因。
 *    members：
 *      保存消费者组下所有成员的元数据信息。组元数据是由 MemberMetadata类建模的，因此，members 字段是按照 Member ID 分组的 MemberMetadata 类。
 *    offsets：
 *      保存按照主题分区分组的位移主题消息位移值的 HashMap。Key 是主题分区，Value 是前面讲过的 CommitRecordMetadataAndOffset 类型。当消费者组成员向 Kafka 提交位移时，源码都会向这个字段插入对应的记录。
 *    subscribedTopics：
 *      保存消费者组订阅的主题列表，用于帮助从 offsets 字段中过滤订阅主题分区的位移值。
 *    supportedProtocols：
 *      保存分区分配策略的支持票数。它是一个 HashMap 类型，其中，Key 是分配策略的名称，Value 是支持的票数。前面我们说过，每个成员可以选择多个分区分配策略，因此，假设成员 A 选择[“range”，“round-robin”]、B 选择[“range”]、C 选择[“round-robin”，“sticky”]，那么这个字段就有 3 项，分别是：<“range”，2>、<“round-robin”，2> 和 <“sticky”，1>。
 */
@nonthreadsafe
private[group] class GroupMetadata(val groupId: String, initialState: GroupState, time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit

  private[group] val lock = new ReentrantLock

  // 组状态
  private var state: GroupState = initialState
  // 记录状态最近一次变更的时间戳
  var currentStateTimestamp: Option[Long] = Some(time.milliseconds())
  var protocolType: Option[String] = None
  var protocolName: Option[String] = None
  // 标识当前Consumer Group的年代信息，避免受到过期请求的影响
  var generationId = 0
  // 记录消费者组的Leader成员，可能不存在
  private var leaderId: Option[String] = None

  // 成员元数据列表信息
  private val members = new mutable.HashMap[String, MemberMetadata]
  // Static membership mapping [key: group.instance.id, value: member.id]
  // 静态成员Id列表
  private val staticMembers = new mutable.HashMap[String, String]
  private val pendingMembers = new mutable.HashSet[String]
  private var numMembersAwaitingJoin = 0
  // 分区分配策略支持票数
  private val supportedProtocols = new mutable.HashMap[String, Integer]().withDefaultValue(0)
  // 保存消费者组订阅分区的提交位移值
  // Key 是 TopicPartition 类型，表示一个主题分区， Value 是CommitRecordMetadataAndOffset 类型。该类封装了位移提交消息的位移值。
  private val offsets = new mutable.HashMap[TopicPartition, CommitRecordMetadataAndOffset]
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]()
  private var receivedTransactionalOffsetCommits = false
  private var receivedConsumerOffsetCommits = false
  private val pendingSyncMembers = new mutable.HashSet[String]

  // When protocolType == `consumer`, a set of subscribed topics is maintained. The set is
  // computed when a new generation is created or when the group is restored from the log.
  // 消费者组订阅的主题列表
  private var subscribedTopics: Option[Set[String]] = None

  var newMemberAdded: Boolean = false

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  // 判断消费者组状态是指定状态
  def is(groupState: GroupState): Boolean = state == groupState
  def has(memberId: String): Boolean = members.contains(memberId)
  def get(memberId: String): MemberMetadata = members(memberId)
  def size: Int = members.size

  def isLeader(memberId: String): Boolean = leaderId.contains(memberId)
  def leaderOrNull: String = leaderId.orNull
  def currentStateTimestampOrDefault: Long = currentStateTimestamp.getOrElse(-1)

  def isConsumerGroup: Boolean = protocolType.contains(ConsumerProtocol.PROTOCOL_TYPE)

  def add(member: MemberMetadata, callback: JoinCallback = null): Unit = {
    member.groupInstanceId.foreach { instanceId =>
      if (staticMembers.contains(instanceId))
        throw new IllegalStateException(s"Static member with groupInstanceId=$instanceId " +
          s"cannot be added to group $groupId since it is already a member")
      staticMembers.put(instanceId, member.memberId)
    }

    // 如果是要添加的第一个消费者组成员
    if (members.isEmpty)
    // 就把该成员的procotolType设置为消费者组的protocolType
      this.protocolType = Some(member.protocolType)

    // 确保成员元数据中的groupId和组Id相同
    assert(this.protocolType.orNull == member.protocolType)
    // 确保成员元数据中的protoclType和组protocolType相同
    // 确保该成员选定的分区分配策略与组选定的分区分配策略相匹配
    assert(supportsProtocols(member.protocolType, MemberMetadata.plainProtocolSet(member.supportedProtocols)))

    // 如果尚未选出Leader成员
    // 这里的 Leader 和副本管理器时的 Leader 副本是不同的概念。这里的 Leader 成员，是指消费者组下的一个成员。
    // 该成员负责为所有成员制定分区分配方案，制定方法的依据，就是消费者组选定的分区分配策略。
    if (leaderId.isEmpty)
    // 把该成员设定为Leader成员
      leaderId = Some(member.memberId)

    // 将该成员添加进members
    members.put(member.memberId, member)
    // 更新分区分配策略支持票数
    incSupportedProtocols(member)
    // 设置成员加入组后的回调逻辑
    member.awaitingJoinCallback = callback

    // 更新已加入组的成员数
    if (member.isAwaitingJoin)
      numMembersAwaitingJoin += 1

    pendingMembers.remove(member.memberId)
  }

  def remove(memberId: String): Unit = {
    // 从members中移除给定成员
    members.remove(memberId).foreach { member =>
      // 更新分区分配策略支持票数
      decSupportedProtocols(member)
      // 更新已加入组的成员数
      if (member.isAwaitingJoin)
        numMembersAwaitingJoin -= 1

      member.groupInstanceId.foreach(staticMembers.remove)
    }

    // 如果该成员是Leader，选择剩下成员列表中的第一个作为新的Leader成员
    if (isLeader(memberId))
      leaderId = members.keys.headOption

    pendingMembers.remove(memberId)
    pendingSyncMembers.remove(memberId)
  }

  /**
    * Check whether current leader is rejoined. If not, try to find another joined member to be
    * new leader. Return false if
    *   1. the group is currently empty (has no designated leader)
    *   2. no member rejoined
    */
  def maybeElectNewJoinedLeader(): Boolean = {
    leaderId.exists { currentLeaderId =>
      val currentLeader = get(currentLeaderId)
      if (!currentLeader.isAwaitingJoin) {
        members.find(_._2.isAwaitingJoin) match {
          case Some((anyJoinedMemberId, anyJoinedMember)) =>
            leaderId = Option(anyJoinedMemberId)
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, while new leader $anyJoinedMember was elected.")
            true

          case None =>
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, and the group couldn't proceed to next generation" +
              s"because no member joined.")
            false
        }
      } else {
        true
      }
    }
  }

  /**
    * [For static members only]: Replace the old member id with the new one,
    * keep everything else unchanged and return the updated member.
    */
  def replaceStaticMember(
    groupInstanceId: String,
    oldMemberId: String,
    newMemberId: String
  ): MemberMetadata = {
    val memberMetadata = members.remove(oldMemberId)
      .getOrElse(throw new IllegalArgumentException(s"Cannot replace non-existing member id $oldMemberId"))

    // Fence potential duplicate member immediately if someone awaits join/sync callback.
    maybeInvokeJoinCallback(memberMetadata, JoinGroupResult(oldMemberId, Errors.FENCED_INSTANCE_ID))
    maybeInvokeSyncCallback(memberMetadata, SyncGroupResult(Errors.FENCED_INSTANCE_ID))

    memberMetadata.memberId = newMemberId
    members.put(newMemberId, memberMetadata)

    if (isLeader(oldMemberId)) {
      leaderId = Some(newMemberId)
    }

    staticMembers.put(groupInstanceId, newMemberId)
    memberMetadata
  }

  def isPendingMember(memberId: String): Boolean = pendingMembers.contains(memberId)

  def addPendingMember(memberId: String): Boolean = {
    if (has(memberId)) {
      throw new IllegalStateException(s"Attempt to add pending member $memberId which is already " +
        s"a stable member of the group")
    }
    pendingMembers.add(memberId)
  }

  def addPendingSyncMember(memberId: String): Boolean = {
    if (!has(memberId)) {
      throw new IllegalStateException(s"Attempt to add a pending sync for member $memberId which " +
        "is not a member of the group")
    }
    pendingSyncMembers.add(memberId)
  }

  def removePendingSyncMember(memberId: String): Boolean = {
    if (!has(memberId)) {
      throw new IllegalStateException(s"Attempt to remove a pending sync for member $memberId which " +
        "is not a member of the group")
    }
    pendingSyncMembers.remove(memberId)
  }

  def hasReceivedSyncFromAllMembers: Boolean = {
    pendingSyncMembers.isEmpty
  }

  def allPendingSyncMembers: Set[String] = {
    pendingSyncMembers.toSet
  }

  def clearPendingSyncMembers(): Unit = {
    pendingSyncMembers.clear()
  }

  def hasStaticMember(groupInstanceId: String): Boolean = {
    staticMembers.contains(groupInstanceId)
  }

  def currentStaticMemberId(groupInstanceId: String): Option[String] = {
    staticMembers.get(groupInstanceId)
  }

  def currentState: GroupState = state

  def notYetRejoinedMembers: Map[String, MemberMetadata] = members.filter(!_._2.isAwaitingJoin).toMap

  def hasAllMembersJoined: Boolean = members.size == numMembersAwaitingJoin && pendingMembers.isEmpty

  def allMembers: collection.Set[String] = members.keySet

  def allStaticMembers: collection.Set[String] = staticMembers.keySet

  // For testing only.
  private[group] def allDynamicMembers: Set[String] = {
    val dynamicMemberSet = new mutable.HashSet[String]
    allMembers.foreach(memberId => dynamicMemberSet.add(memberId))
    staticMembers.values.foreach(memberId => dynamicMemberSet.remove(memberId))
    dynamicMemberSet.toSet
  }

  def numPending: Int = pendingMembers.size

  def numAwaiting: Int = numMembersAwaitingJoin

  def allMemberMetadata: List[MemberMetadata] = members.values.toList

  def rebalanceTimeoutMs: Int = members.values.foldLeft(0) { (timeout, member) =>
    timeout.max(member.rebalanceTimeoutMs)
  }

  def generateMemberId(clientId: String,
                       groupInstanceId: Option[String]): String = {
    groupInstanceId match {
      case None =>
        clientId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
      case Some(instanceId) =>
        instanceId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
    }
  }

  /**
    * Verify the member.id is up to date for static members. Return true if both conditions met:
    *   1. given member is a known static member to group
    *   2. group stored member.id doesn't match with given member.id
    */
  def isStaticMemberFenced(
    groupInstanceId: String,
    memberId: String
  ): Boolean = {
    currentStaticMemberId(groupInstanceId).exists(_ != memberId)
  }

  // 消费者组能否Rebalance的条件是当前状态是PreparingRebalance状态的合法前置状态
  def canRebalance: Boolean = PreparingRebalance.validPreviousStates.contains(state)

  // 设置/更新状态
  def transitionTo(groupState: GroupState): Unit = {
    assertValidTransition(groupState)// 确保是合法的状态转换
    state = groupState// 设置状态到给定状态
    currentStateTimestamp = Some(time.milliseconds())// 更新状态变更时间戳
  }

  def selectProtocol: String = {
    // 如果没有任何成员，自然无法确定选用哪个策略
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members
    // 获取所有成员都支持的策略集合
    val candidates = candidateProtocols

    // let each member vote for one of the protocols and choose the one with the most votes
    // 让每个成员投票，票数最多的那个策略当选
    // 每个成员都会通过vote为其support protocols集合中第一个候选协议投一票，最终将选择投票最多的protocol
    val (protocol, _) = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .maxBy { case (_, votes) => votes.size }

    protocol
  }

  private def incSupportedProtocols(member: MemberMetadata): Unit = {
    member.supportedProtocols.foreach { case (protocol, _) => supportedProtocols(protocol) += 1 }
  }

  private def decSupportedProtocols(member: MemberMetadata): Unit = {
    member.supportedProtocols.foreach { case (protocol, _) => supportedProtocols(protocol) -= 1 }
  }

  // 是找出组内所有成员都支持的分区分配策略集
  private def candidateProtocols: Set[String] = {
    // get the set of protocols that are commonly supported by all members
    val numMembers = members.size // 获取组内成员数
    supportedProtocols.filter(_._2 == numMembers).keys.toSet// 找出支持票数=总成员数的策略，返回它们的名称(支持票数等于总成员数的意思，等同于所有成员都支持该策略。)
  }

  def supportsProtocols(memberProtocolType: String, memberProtocols: Set[String]): Boolean = {
    if (is(Empty))
      memberProtocolType.nonEmpty && memberProtocols.nonEmpty
    else
      protocolType.contains(memberProtocolType) && memberProtocols.exists(supportedProtocols(_) == members.size)
  }

  def getSubscribedTopics: Option[Set[String]] = subscribedTopics

  /**
   * Returns true if the consumer group is actively subscribed to the topic. When the consumer
   * group does not know, because the information is not available yet or because the it has
   * failed to parse the Consumer Protocol, it returns true to be safe.
   */
  def isSubscribedToTopic(topic: String): Boolean = subscribedTopics match {
    case Some(topics) => topics.contains(topic)
    case None => true
  }

  /**
   * Collects the set of topics that the members are subscribed to when the Protocol Type is equal
   * to 'consumer'. None is returned if
   * - the protocol type is not equal to 'consumer';
   * - the protocol is not defined yet; or
   * - the protocol metadata does not comply with the schema.
   */
  private[group] def computeSubscribedTopics(): Option[Set[String]] = {
    protocolType match {
      case Some(ConsumerProtocol.PROTOCOL_TYPE) if members.nonEmpty && protocolName.isDefined =>
        try {
          Some(
            members.map { case (_, member) =>
              // The consumer protocol is parsed with V0 which is the based prefix of all versions.
              // This way the consumer group manager does not depend on any specific existing or
              // future versions of the consumer protocol. VO must prefix all new versions.
              val buffer = ByteBuffer.wrap(member.metadata(protocolName.get))
              ConsumerProtocol.deserializeVersion(buffer)
              ConsumerProtocol.deserializeSubscription(buffer, 0).topics.asScala.toSet
            }.reduceLeft(_ ++ _)
          )
        } catch {
          case e: SchemaException =>
            warn(s"Failed to parse Consumer Protocol ${ConsumerProtocol.PROTOCOL_TYPE}:${protocolName.get} " +
              s"of group $groupId. Consumer group coordinator is not aware of the subscribed topics.", e)
            None
        }

      case Some(ConsumerProtocol.PROTOCOL_TYPE) if members.isEmpty =>
        Option(Set.empty)

      case _ => None
    }
  }

  def updateMember(member: MemberMetadata,
                   protocols: List[(String, Array[Byte])],
                   callback: JoinCallback): Unit = {
    decSupportedProtocols(member)
    member.supportedProtocols = protocols
    incSupportedProtocols(member)

    if (callback != null && !member.isAwaitingJoin) {
      numMembersAwaitingJoin += 1
    } else if (callback == null && member.isAwaitingJoin) {
      numMembersAwaitingJoin -= 1
    }
    member.awaitingJoinCallback = callback
  }

  def maybeInvokeJoinCallback(member: MemberMetadata,
                              joinGroupResult: JoinGroupResult): Unit = {
    if (member.isAwaitingJoin) {
      member.awaitingJoinCallback(joinGroupResult)
      member.awaitingJoinCallback = null
      numMembersAwaitingJoin -= 1
    }
  }

  /**
    * @return true if a sync callback actually performs.
    */
  def maybeInvokeSyncCallback(member: MemberMetadata,
                              syncGroupResult: SyncGroupResult): Boolean = {
    if (member.isAwaitingSync) {
      member.awaitingSyncCallback(syncGroupResult)
      member.awaitingSyncCallback = null
      true
    } else {
      false
    }
  }

  def initNextGeneration(): Unit = {
    if (members.nonEmpty) {
      generationId += 1
      protocolName = Some(selectProtocol)
      subscribedTopics = computeSubscribedTopics()
      transitionTo(CompletingRebalance)
    } else {
      generationId += 1
      protocolName = None
      subscribedTopics = computeSubscribedTopics()
      transitionTo(Empty)
    }
    receivedConsumerOffsetCommits = false
    receivedTransactionalOffsetCommits = false
    clearPendingSyncMembers()
  }

  def currentMemberMetadata: List[JoinGroupResponseMember] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map{ case (memberId, memberMetadata) => new JoinGroupResponseMember()
        .setMemberId(memberId)
        .setGroupInstanceId(memberMetadata.groupInstanceId.orNull)
        .setMetadata(memberMetadata.metadata(protocolName.get))
    }.toList
  }

  def summary: GroupSummary = {
    if (is(Stable)) {
      val protocol = protocolName.orNull
      if (protocol == null)
        throw new IllegalStateException("Invalid null group protocol for stable group")

      val members = this.members.values.map { member => member.summary(protocol) }
      GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members.toList)
    } else {
      val members = this.members.values.map{ member => member.summaryNoMetadata() }
      GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members.toList)
    }
  }

  def overview: GroupOverview = {
    GroupOverview(groupId, protocolType.getOrElse(""), state.toString)
  }

  def initializeOffsets(offsets: collection.Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTxnOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]): Unit = {
    this.offsets ++= offsets
    this.pendingTransactionalOffsetCommits ++= pendingTxnOffsets
  }

  def onOffsetCommitAppend(topicPartition: TopicPartition, offsetWithCommitRecordMetadata: CommitRecordMetadataAndOffset): Unit = {
    if (pendingOffsetCommits.contains(topicPartition)) {
      if (offsetWithCommitRecordMetadata.appendedBatchOffset.isEmpty)
        throw new IllegalStateException("Cannot complete offset commit write without providing the metadata of the record " +
          "in the log.")
      // offsets字段中没有该分区位移提交数据，或者
      // offsets字段中该分区对应的提交位移消息在位移主题中的位移值小于待写入的位移值
      if (!offsets.contains(topicPartition) || offsets(topicPartition).olderThan(offsetWithCommitRecordMetadata))
        // 将该分区对应的提交位移消息添加到offsets中
        offsets.put(topicPartition, offsetWithCommitRecordMetadata)
    }

    pendingOffsetCommits.get(topicPartition) match {
      case Some(stagedOffset) if offsetWithCommitRecordMetadata.offsetAndMetadata == stagedOffset =>
        pendingOffsetCommits.remove(topicPartition)
      case _ =>
        // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
        // its entries would be removed from the cache by the `removeOffsets` method.
    }
  }

  def failPendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata): Unit = {
    pendingOffsetCommits.get(topicPartition) match {
      case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
      case _ =>
    }
  }

  def prepareOffsetCommit(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    receivedConsumerOffsetCommits = true
    pendingOffsetCommits ++= offsets
  }

  def prepareTxnOffsetCommit(producerId: Long, offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $offsets is pending")
    receivedTransactionalOffsetCommits = true
    val producerOffsets = pendingTransactionalOffsetCommits.getOrElseUpdate(producerId,
      mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])

    offsets.forKeyValue { (topicPartition, offsetAndMetadata) =>
      producerOffsets.put(topicPartition, CommitRecordMetadataAndOffset(None, offsetAndMetadata))
    }
  }

  def hasReceivedConsistentOffsetCommits : Boolean = {
    !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits
  }

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  def failPendingTxnOffsetCommit(producerId: Long, topicPartition: TopicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition)
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +
          s"to be appended to the log")
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId)
      case _ =>
        // We may hit this case if the partition in question has emigrated already.
    }
  }

  def onTxnOffsetCommitAppend(producerId: Long, topicPartition: TopicPartition,
                              commitRecordMetadataAndOffset: CommitRecordMetadataAndOffset): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata)
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset)
      case _ =>
        // We may hit this case if the partition in question has emigrated.
    }
  }

  /* Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
   * to the log.
   *
   * 作用是完成一个待决事务（PendingTransaction）的位移提交。所谓的待决事务，就是指正在进行中、还没有完成的事务。
   * 在处理待决事务的过程中，可能会出现将待决事务中涉及到的分区的位移值添加到 offsets 中的情况。
   */
  def completePendingTxnOffsetCommit(producerId: Long, isCommit: Boolean): Unit = {
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId)
    if (isCommit) {
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.forKeyValue { (topicPartition, commitRecordMetadataAndOffset) =>
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +
              s"and groupId $groupId even though the offset commit record itself hasn't been appended to the log.")

          val currentOffsetOpt = offsets.get(topicPartition)
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              "committed and loaded into the cache.")
            offsets.put(topicPartition, commitRecordMetadataAndOffset)
          } else {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.")
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  def activeProducers: collection.Set[Long] = pendingTransactionalOffsetCommits.keySet

  def hasPendingOffsetCommitsFromProducer(producerId: Long): Boolean =
    pendingTransactionalOffsetCommits.contains(producerId)

  def hasPendingOffsetCommitsForTopicPartition(topicPartition: TopicPartition): Boolean = {
    pendingOffsetCommits.contains(topicPartition) ||
      pendingTransactionalOffsetCommits.exists(
        _._2.contains(topicPartition)
      )
  }

  def removeAllOffsets(): immutable.Map[TopicPartition, OffsetAndMetadata] = removeOffsets(offsets.keySet.toSeq)

  def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    topicPartitions.flatMap { topicPartition =>
      pendingOffsetCommits.remove(topicPartition)
      pendingTransactionalOffsetCommits.forKeyValue { (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition)
      }
      val removedOffset = offsets.remove(topicPartition)
      removedOffset.map(topicPartition -> _.offsetAndMetadata)
    }.toMap
  }

  def removeExpiredOffsets(currentTimestamp: Long, offsetRetentionMs: Long): Map[TopicPartition, OffsetAndMetadata] = {

    // 获取订阅分区过期的位移值
    def getExpiredOffsets(baseTimestamp: CommitRecordMetadataAndOffset => Long,// 接收 CommitRecordMetadataAndOffset 类型的字段，然后计算时间戳，并返回
                          subscribedTopics: Set[String] = Set.empty): Map[TopicPartition, OffsetAndMetadata] = {
      // 遍历offsets中的所有分区，过滤出同时满足以下3个条件的所有分区
      //  条件1：分区所属主题不在订阅主题列表之内, 就说明该消费者组此时正在消费中，正在消费的主题是不能执行过期位移移除的。
      //  条件2：该主题分区已经完成位移提交
      //  条件3：该主题分区在位移主题中对应消息的存在时间超过了阈值
      // ===============
      // 条件 1：分区所属主题不在订阅主题列表之内。当方法传入了不为空的主题集合时，就说
      //        明该消费者组此时正在消费中，正在消费的主题是不能执行过期位移移除的。
      //条件 2：主题分区已经完成位移提交，那种处于提交中状态，也就是保存在
      //       pendingOffsetCommits 字段中的分区，不予考虑。
      //条件 3：该主题分区在位移主题中对应消息的存在时间超过了阈值。老版本的 Kafka 消息
      //        直接指定了过期时间戳，因此，只需要判断当前时间是否越过了这个过期时间。但是，目
      //        前，新版 Kafka 判断过期与否，主要是基于消费者组状态。如果是 Empty 状态，过期的
      //        判断依据就是当前时间与组变为 Empty 状态时间的差值，是否超过 Broker 端参数
      //        offsets.retention.minutes 值；如果不是 Empty 状态，就看当前时间与提交位移消息中
      //        的时间戳差值是否超过了 offsets.retention.minutes 值。如果超过了，就视为已过期，对
      //        应的位移值需要被移除；如果没有超过，就不需要移除了。
      offsets.filter {
        case (topicPartition, commitRecordMetadataAndOffset) =>
          !subscribedTopics.contains(topicPartition.topic()) &&
          !pendingOffsetCommits.contains(topicPartition) && {
            commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp match {
              case None =>
                // current version with no per partition retention
                currentTimestamp - baseTimestamp(commitRecordMetadataAndOffset) >= offsetRetentionMs
              case Some(expireTimestamp) =>
                // older versions with explicit expire_timestamp field => old expiration semantics is used
                currentTimestamp >= expireTimestamp
            }
          }
      }.map {
        // 为满足以上3个条件的分区提取出commitRecordMetadataAndOffset中的位移值
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata)
      }.toMap
    }

    // 调用getExpiredOffsets方法获取主题分区的过期位移
    val expiredOffsets: Map[TopicPartition, OffsetAndMetadata] = protocolType match {
      case Some(_) if is(Empty) =>
        // no consumer exists in the group =>
        // - if current state timestamp exists and retention period has passed since group became Empty,
        //   expire all offsets with no pending offset commit;
        // - if there is no current state timestamp (old group metadata schema) and retention period has passed
        //   since the last commit timestamp, expire the offset
        getExpiredOffsets(
          commitRecordMetadataAndOffset => currentStateTimestamp
            .getOrElse(commitRecordMetadataAndOffset.offsetAndMetadata.commitTimestamp)
        )

      case Some(ConsumerProtocol.PROTOCOL_TYPE) if subscribedTopics.isDefined && is(Stable) =>
        // consumers exist in the group and group is stable =>
        // - if the group is aware of the subscribed topics and retention period had passed since the
        //   the last commit timestamp, expire the offset. offset with pending offset commit are not
        //   expired
        getExpiredOffsets(
          _.offsetAndMetadata.commitTimestamp,
          subscribedTopics.get
        )

      case None =>
        // protocolType is None => standalone (simple) consumer, that uses Kafka for offset storage only
        // expire offsets with no pending offset commit that retention period has passed since their last commit
        getExpiredOffsets(_.offsetAndMetadata.commitTimestamp)

      case _ =>
        Map()
    }

    if (expiredOffsets.nonEmpty)
      debug(s"Expired offsets from group '$groupId': ${expiredOffsets.keySet}")
    // 将过期位移对应的主题分区从offsets中移除
    offsets --= expiredOffsets.keySet
    // 返回主题分区对应的过期位移
    expiredOffsets
  }

  def allOffsets: Map[TopicPartition, OffsetAndMetadata] = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata)
  }.toMap

  def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition).map(_.offsetAndMetadata)

  // visible for testing
  private[group] def offsetWithRecordMetadata(topicPartition: TopicPartition): Option[CommitRecordMetadataAndOffset] = offsets.get(topicPartition)

  def numOffsets: Int = offsets.size

  def hasOffsets: Boolean = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty

  private def assertValidTransition(targetState: GroupState): Unit = {
    if (!targetState.validPreviousStates.contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, targetState.validPreviousStates.mkString(","), targetState, state))
  }

  override def toString: String = {
    "GroupMetadata(" +
      s"groupId=$groupId, " +
      s"generation=$generationId, " +
      s"protocolType=$protocolType, " +
      s"currentState=$currentState, " +
      s"members=$members)"
  }

}

