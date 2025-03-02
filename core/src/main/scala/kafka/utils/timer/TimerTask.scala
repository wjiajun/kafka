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

trait TimerTask extends Runnable {

  // 相对时间
  val delayMs: Long // timestamp in millisecond // 通常是request.timeout.ms参数值
  // 每个TimerTask实例关联一个TimerTaskEntry
  // 就是说每个定时任务需要知道它在哪个Bucket链表下的哪个链表元素上

  private[this] var timerTaskEntry: TimerTaskEntry = null

  // 取消定时任务，原理就是将关联的timerTaskEntry置空
  def cancel(): Unit = {
    synchronized {
      if (timerTaskEntry != null) timerTaskEntry.remove()
      timerTaskEntry = null
    }
  }

  // 关联timerTaskEntry，原理是给timerTaskEntry字段赋值
  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()

      timerTaskEntry = entry
    }
  }

  // 获取关联的timerTaskEntry实例
  private[timer] def getTimerTaskEntry: TimerTaskEntry = timerTaskEntry

}
