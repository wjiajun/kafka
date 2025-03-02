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

import kafka.utils.{Logging, ReplicationUtils, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CompletableFuture, TimeUnit}

import kafka.api.LeaderAndIsr
import org.apache.kafka.common.errors.InvalidUpdateVersionException
import org.apache.kafka.common.utils.Time

import scala.collection.mutable

/**
 * @param checkIntervalMs How often to check for ISR
 * @param maxDelayMs  Maximum time that an ISR change may be delayed before sending the notification
 * @param lingerMs  Maximum time to await additional changes before sending the notification
 */
case class IsrChangePropagationConfig(checkIntervalMs: Long, maxDelayMs: Long, lingerMs: Long)

object ZkIsrManager {
  // This field is mutable to allow overriding change notification behavior in test cases
  @volatile var DefaultIsrPropagationConfig: IsrChangePropagationConfig = IsrChangePropagationConfig(
    checkIntervalMs = 2500,
    lingerMs = 5000,
    maxDelayMs = 60000,
  )
}

class ZkIsrManager(scheduler: Scheduler, time: Time, zkClient: KafkaZkClient) extends AlterIsrManager with Logging {

  private val isrChangeNotificationConfig = ZkIsrManager.DefaultIsrPropagationConfig
  // Visible for testing
  private[server] val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(time.milliseconds())
  private val lastIsrPropagationMs = new AtomicLong(time.milliseconds())

  override def start(): Unit = {
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _,
      period = isrChangeNotificationConfig.checkIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  override def submit(
    topicPartition: TopicPartition,
    leaderAndIsr: LeaderAndIsr,
    controllerEpoch: Int
  ): CompletableFuture[LeaderAndIsr]= {
    debug(s"Writing new ISR ${leaderAndIsr.isr} to ZooKeeper with version " +
      s"${leaderAndIsr.zkVersion} for partition $topicPartition")

    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topicPartition,
      leaderAndIsr, controllerEpoch)

    val future = new CompletableFuture[LeaderAndIsr]()
    if (updateSucceeded) {
      // Track which partitions need to be propagated to the controller
      isrChangeSet synchronized {
        isrChangeSet += topicPartition
        lastIsrChangeMs.set(time.milliseconds())
      }

      // We rely on Partition#isrState being properly set to the pending ISR at this point since we are synchronously
      // applying the callback
      future.complete(leaderAndIsr.withZkVersion(newVersion))
    } else {
      future.completeExceptionally(new InvalidUpdateVersionException(
        s"ISR update $leaderAndIsr for partition $topicPartition with controller epoch $controllerEpoch " +
          "failed with an invalid version error"))
    }
    future
  }

  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   *
   * KafkaController对相应路径添加了Wathcer，当Watcher被触发后会向其管理的Broker发送UpdateMetadataRequest，频繁地触发Watcher会影响KafkaController、ZooKeeper甚至其他Broker的性能。
   * 为了避免这种情况，设置了一定的写入条件
   */
  private[server] def maybePropagateIsrChanges(): Unit = {
    val now = time.milliseconds()
    isrChangeSet synchronized {
      // ISR变更传播的条件，需要同时满足：
      // 1. 存在尚未被传播的ISR变更
      // 2. 最近5秒没有任何ISR变更，或者自上次ISR变更已经有超过1分钟的时间
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + isrChangeNotificationConfig.lingerMs < now ||
          lastIsrPropagationMs.get() + isrChangeNotificationConfig.maxDelayMs < now)) {
        // 创建ZooKeeper相应的Znode节点
        zkClient.propagateIsrChanges(isrChangeSet)
        // 清空isrChangeSet集合
        isrChangeSet.clear()
        // 更新最近ISR变更时间戳
        lastIsrPropagationMs.set(now)
      }
    }
  }
}
