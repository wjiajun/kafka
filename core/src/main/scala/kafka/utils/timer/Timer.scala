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
package kafka.utils.timer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{DelayQueue, Executors, TimeUnit}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    *
    * 将给定的定时任务插入到时间轮上，等待后续延迟执行
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    *
    * 向前推进时钟，执行已达过期时间的延迟任务
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    *   获取时间轮上总的定时任务数
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
   *  关闭定时器
    */
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1, // 时间tick间隔
                  wheelSize: Int = 20, // 分层时间轮的bucket数，每层相同
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // timeout timer
  // 单线程的线程池用于异步执行定时任务
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))

  // 延迟队列保存所有Bucket，即所有TimerTaskList对象
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  // 总定时任务数
  private[this] val taskCounter = new AtomicInteger(0)
  // 时间轮对象
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  // 维护线程安全的读写锁
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(timerTask: TimerTask): Unit = {
    // 获取读锁。在没有线程持有写锁的前提下
    // 多个线程能够同时向时间轮添加定时任务
    readLock.lock()
    try {
      // 调用addTimerTaskEntry执行插入逻辑
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  // 1. 如果该任务既未取消也未过期，那么，addTimerTaskEntry 方法将其添加到时间轮；
  // 2. 如果该任务已取消，则该方法什么都不做，直接返回；
  // 3. 如果该任务已经过期，则提交到相应的线程池，等待后续执行。
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    // 视timerTaskEntry状态决定执行什么逻辑：
    // 1. 未过期未取消：添加到时间轮
    // 2. 已取消：什么都不做
    // 3. 已过期：提交到线程池，等待执行
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      // 定时任务未取消，说明定时任务已过期
      // 否则timingWheel.add方法应该返回True
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   * 驱动时钟向前推进。
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    // 获取delayQueue中下一个已过期的Bucket
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      // 获取写锁
      // 一旦有线程持有写锁，其他任何线程执行add或advanceClock方法时会阻塞
      writeLock.lock()
      try {
        while (bucket != null) {
          // 推动时间轮向前"滚动"到Bucket的过期时间点
          timingWheel.advanceClock(bucket.getExpiration)
          // 将该Bucket下的所有定时任务重写回到时间轮
          // 调用reinset,尝试将bucket中的任务重新添加到时间轮。此过程并不一定是将任务提交到taskExecutor，对于未到期的任务只是从原来的时间轮降级到下层时间轮继续等待
          bucket.flush(addTimerTaskEntry)
          // 读取下一个Bucket对象
          bucket = delayQueue.poll()
        }
      } finally {
        // 释放写锁
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown(): Unit = {
    taskExecutor.shutdown()
  }

}
