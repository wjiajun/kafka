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

package kafka.log

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}

import kafka.common.LogSegmentOffsetOverflowException
import kafka.log.UnifiedLog.{CleanedFileSuffix, DeletedFileSuffix, SwapFileSuffix, isIndexFile, isLogFile, offsetFromFile}
import kafka.server.{LogDirFailureChannel, LogOffsetMetadata}
import kafka.server.epoch.LeaderEpochFileCache
import kafka.utils.{CoreUtils, Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.utils.Time

import scala.collection.{Set, mutable}

case class LoadedLogOffsets(logStartOffset: Long,
                            recoveryPoint: Long,
                            nextOffsetMetadata: LogOffsetMetadata)

object LogLoader extends Logging {

  /**
   * Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
   * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
   * avoided by passing in the recovery point, however finding the correct position to do this
   * requires accessing the offset index which may not be safe in an unclean shutdown.
   * For more information see the discussion in PR#2104
   */
  val CleanShutdownFile = ".kafka_cleanshutdown"
}


/**
 * @param dir The directory from which log segments need to be loaded
 * @param topicPartition The topic partition associated with the log being loaded
 * @param config The configuration settings for the log being loaded
 * @param scheduler The thread pool scheduler used for background actions
 * @param time The time instance used for checking the clock
 * @param logDirFailureChannel The LogDirFailureChannel instance to asynchronously handle log
 *                             directory failure
 * @param hadCleanShutdown Boolean flag to indicate whether the associated log previously had a
 *                         clean shutdown
 * @param segments The LogSegments instance into which segments recovered from disk will be
 *                 populated
 * @param logStartOffsetCheckpoint The checkpoint of the log start offset
 * @param recoveryPointCheckpoint The checkpoint of the offset at which to begin the recovery
 * @param leaderEpochCache An optional LeaderEpochFileCache instance to be updated during recovery
 * @param producerStateManager The ProducerStateManager instance to be updated during recovery
 */
class LogLoader(
  dir: File,
  topicPartition: TopicPartition,
  config: LogConfig,
  scheduler: Scheduler,
  time: Time,
  logDirFailureChannel: LogDirFailureChannel,
  hadCleanShutdown: Boolean,
  segments: LogSegments,
  logStartOffsetCheckpoint: Long,
  recoveryPointCheckpoint: Long,
  leaderEpochCache: Option[LeaderEpochFileCache],
  producerStateManager: ProducerStateManager
) extends Logging {
  logIdent = s"[LogLoader partition=$topicPartition, dir=${dir.getParent}] "

  /**
   * Load the log segments from the log files on disk, and returns the components of the loaded log.
   * Additionally, it also suitably updates the provided LeaderEpochFileCache and ProducerStateManager
   * to reflect the contents of the loaded log.
   *
   * In the context of the calling thread, this function does not need to convert IOException to
   * KafkaStorageException because it is only called before all logs are loaded.
   *
   * @return the offsets of the Log successfully loaded from disk
   *
   * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that
   *                                           overflow index offset
   */
  def load(): LoadedLogOffsets = {
    // First pass: through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    // 移除上次 Failure 遗留下来的各种临时文件
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // The remaining valid swap files must come from compaction or segment split operation. We can
    // simply rename them to regular segment files. But, before renaming, we should figure out which
    // segments are compacted/split and delete these segment files: this is done by calculating
    // min/maxSwapFileOffset.
    // We store segments that require renaming in this code block, and do the actual renaming later.
    var minSwapFileOffset = Long.MaxValue
    var maxSwapFileOffset = Long.MinValue
    // 遍历所有有效.swap文件
    swapFiles.filter(f => UnifiedLog.isLogFile(new File(CoreUtils.replaceSuffix(f.getPath, SwapFileSuffix, "")))).foreach { f =>
      val baseOffset = offsetFromFile(f)
      // 创建对应的LogSegment实例
      val segment = LogSegment.open(f.getParentFile,
        baseOffset = baseOffset,
        config,
        time = time,
        fileSuffix = UnifiedLog.SwapFileSuffix)
      info(s"Found log file ${f.getPath} from interrupted swap operation, which is recoverable from ${UnifiedLog.SwapFileSuffix} files by renaming.")
      minSwapFileOffset = Math.min(segment.baseOffset, minSwapFileOffset)
      maxSwapFileOffset = Math.max(segment.readNextOffset, maxSwapFileOffset)
    }

    // Second pass: delete segments that are between minSwapFileOffset and maxSwapFileOffset. As
    // discussed above, these segments were compacted or split but haven't been renamed to .delete
    // before shutting down the broker.
    for (file <- dir.listFiles if file.isFile) {
      try {
        if (!file.getName.endsWith(SwapFileSuffix)) {
          val offset = offsetFromFile(file)
          if (offset >= minSwapFileOffset && offset < maxSwapFileOffset) {
            info(s"Deleting segment files ${file.getName} that is compacted but has not been deleted yet.")
            file.delete()
          }
        }
      } catch {
        // offsetFromFile with files that do not include an offset in the file name
        case _: StringIndexOutOfBoundsException =>
        case _: NumberFormatException =>
      }
    }

    // Third pass: rename all swap files.
    for (file <- dir.listFiles if file.isFile) {
      if (file.getName.endsWith(SwapFileSuffix)) {
        info(s"Recovering file ${file.getName} by renaming from ${UnifiedLog.SwapFileSuffix} files.")
        file.renameTo(new File(CoreUtils.replaceSuffix(file.getPath, UnifiedLog.SwapFileSuffix, "")))
      }
    }

    // Fourth pass: load all the log and index files.
    // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
    // this happens, restart loading segment files from scratch.
    retryOnOffsetOverflow(() => {
      // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
      // loading of segments. In that case, we also need to close all segments that could have been left open in previous
      // call to loadSegmentFiles().
      segments.close()
      segments.clear()
      // 重建日志段 segments Map
      loadSegmentFiles()
    })

    val (newRecoveryPoint: Long, nextOffset: Long) = {
      if (!dir.getAbsolutePath.endsWith(UnifiedLog.DeleteDirSuffix)) {
        val (newRecoveryPoint, nextOffset) = retryOnOffsetOverflow(recoverLog)

        // reset the index size of the currently active log segment to allow more entries
        // 恢复日志段对象
        segments.lastSegment.get.resizeIndexes(config.maxIndexSize)
        (newRecoveryPoint, nextOffset)
      } else {
        if (segments.isEmpty) {
          segments.add(
            LogSegment.open(
              dir = dir,
              baseOffset = 0,
              config,
              time = time,
              initFileSize = config.initFileSize))
        }
        (0L, 0L)
      }
    }

    leaderEpochCache.foreach(_.truncateFromEnd(nextOffset))
    val newLogStartOffset = math.max(logStartOffsetCheckpoint, segments.firstSegment.get.baseOffset)
    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    leaderEpochCache.foreach(_.truncateFromStart(logStartOffsetCheckpoint))

    // Any segment loading or recovery code must not use producerStateManager, so that we can build the full state here
    // from scratch.
    if (!producerStateManager.isEmpty)
      throw new IllegalStateException("Producer state must be empty during log initialization")

    // Reload all snapshots into the ProducerStateManager cache, the intermediate ProducerStateManager used
    // during log recovery may have deleted some files without the LogLoader.producerStateManager instance witnessing the
    // deletion.
    producerStateManager.removeStraySnapshots(segments.baseOffsets.toSeq)
    UnifiedLog.rebuildProducerState(
      producerStateManager,
      segments,
      newLogStartOffset,
      nextOffset,
      config.recordVersion,
      time,
      reloadFromCleanShutdown = hadCleanShutdown,
      logIdent)
    // 更新上次恢复点属性，并返回
    val activeSegment = segments.lastSegment.get
    LoadedLogOffsets(
      newLogStartOffset,
      newRecoveryPoint,
      LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size))
  }

  /**
   * Removes any temporary files found in log directory, and creates a list of all .swap files which could be swapped
   * in place of existing segment(s). For log splitting, we know that any .swap file whose base offset is higher than
   * the smallest offset .clean file could be part of an incomplete split operation. Such .swap files are also deleted
   * by this method.
   *
   * @return Set of .swap files that are valid to be swapped in as segment files and index files
   */
  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {

    // 在方法内部定义一个名为deleteIndicesIfExist的方法，用于删除日志文件对应的索引文件
    val swapFiles = mutable.Set[File]()
    val cleanedFiles = mutable.Set[File]()
    var minCleanedFileOffset = Long.MaxValue

    // 遍历分区日志路径下的所有文件
    for (file <- dir.listFiles if file.isFile) {
      if (!file.canRead)// 如果不可读，直接抛出IOException
        throw new IOException(s"Could not read file $file")
      val filename = file.getName
      if (filename.endsWith(DeletedFileSuffix)) {
        debug(s"Deleting stray temporary file ${file.getAbsolutePath}")
        Files.deleteIfExists(file.toPath)// 说明是上次Failure遗留下来的文件，直接删除
      } else if (filename.endsWith(CleanedFileSuffix)) {
        minCleanedFileOffset = Math.min(offsetFromFile(file), minCleanedFileOffset)
        cleanedFiles += file
      } else if (filename.endsWith(SwapFileSuffix)) { // 如果以.swap结尾
        swapFiles += file // 加入待恢复的.swap文件集合中
      }
    }

    // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned segment offset. Such .swap
    // files could be part of an incomplete split operation that could not complete. See Log#splitOverflowedSegment
    // for more details about the split operation.
    // 从待恢复swap集合中找出那些起始位移值大于minCleanedFileOffset值的文件，直接删掉这些文件
    val (invalidSwapFiles, validSwapFiles) = swapFiles.partition(file => offsetFromFile(file) >= minCleanedFileOffset)
    invalidSwapFiles.foreach { file =>
      debug(s"Deleting invalid swap file ${file.getAbsoluteFile} minCleanedFileOffset: $minCleanedFileOffset")
      Files.deleteIfExists(file.toPath)
    }

    // Now that we have deleted all .swap files that constitute an incomplete split operation, let's delete all .clean files
    // 清除所有待删除文件集合中的文件
    cleanedFiles.foreach { file =>
      debug(s"Deleting stray .clean file ${file.getAbsolutePath}")
      Files.deleteIfExists(file.toPath)
    }

    // 最后返回当前有效的.swap文件集合
    validSwapFiles
  }

  /**
   * Retries the provided function only whenever an LogSegmentOffsetOverflowException is raised by
   * it during execution. Before every retry, the overflowed segment is split into one or more segments
   * such that there is no offset overflow in any of them.
   *
   * @param fn The function to be executed
   * @return The value returned by the function, if successful
   * @throws Exception whenever the executed function throws any exception other than
   *                   LogSegmentOffsetOverflowException, the same exception is raised to the caller
   */
  private def retryOnOffsetOverflow[T](fn: () => T): T = {
    while (true) {
      try {
        return fn()
      } catch {
        case e: LogSegmentOffsetOverflowException =>
          info(s"Caught segment overflow error: ${e.getMessage}. Split segment and retry.")
          val result = UnifiedLog.splitOverflowedSegment(
            e.segment,
            segments,
            dir,
            topicPartition,
            config,
            scheduler,
            logDirFailureChannel,
            logIdent)
          deleteProducerSnapshotsAsync(result.deletedSegments)
      }
    }
    throw new IllegalStateException()
  }

  /**
   * Loads segments from disk into the provided params.segments.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded.
   * It is possible that we encounter a segment with index offset overflow in which case the LogSegmentOffsetOverflowException
   * will be thrown. Note that any segments that were opened before we encountered the exception will remain open and the
   * caller is responsible for closing them appropriately, if needed.
   *
   * @throws LogSegmentOffsetOverflowException if the log directory contains a segment with messages that overflow the index offset
   */
  private def loadSegmentFiles(): Unit = {
    // load segments in ascending order because transactional data from one segment may depend on the
    // segments that come before it
    // 按照日志段文件名中的位移值正序排列，然后遍历每个文件
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      if (isIndexFile(file)) {// 如果原来是索引文件
        // if it is an index file, make sure it has a corresponding .log file
        val offset = offsetFromFile(file)
        val logFile = UnifiedLog.logFile(dir, offset)
        if (!logFile.exists) {// 确保存在对应的日志文件，否则记录一个警告，并删除该索引文件
          warn(s"Found an orphaned index file ${file.getAbsolutePath}, with no corresponding log file.")
          Files.deleteIfExists(file.toPath)// 删除原来的索引文件
        }
      } else if (isLogFile(file)) {// 如果原来是日志文件
        // if it's a log file, load the corresponding log segment
        val baseOffset = offsetFromFile(file)
        val timeIndexFileNewlyCreated = !UnifiedLog.timeIndexFile(dir, baseOffset).exists()
        // 创建对应的LogSegment对象实例，并加入segments中
        val segment = LogSegment.open(
          dir = dir,
          baseOffset = baseOffset,
          config,
          time = time,
          fileAlreadyExists = true)

        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {
          case _: NoSuchFileException =>
            if (hadCleanShutdown || segment.baseOffset < recoveryPointCheckpoint)
              error(s"Could not find offset index file corresponding to log file" +
                s" ${segment.log.file.getAbsolutePath}, recovering segment and rebuilding index files...")
            recoverSegment(segment)
          case e: CorruptIndexException =>
            warn(s"Found a corrupted index file corresponding to log file" +
              s" ${segment.log.file.getAbsolutePath} due to ${e.getMessage}}, recovering segment and" +
              " rebuilding index files...")
            recoverSegment(segment)
        }
        segments.add(segment)
      }
    }
  }

  /**
   * Just recovers the given segment, without adding it to the provided params.segments.
   *
   * @param segment Segment to recover
   *
   * @return The number of bytes truncated from the segment
   *
   * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
   */
  private def recoverSegment(segment: LogSegment): Int = {
    val producerStateManager = new ProducerStateManager(
      topicPartition,
      dir,
      this.producerStateManager.maxTransactionTimeoutMs,
      this.producerStateManager.maxProducerIdExpirationMs,
      time)
    UnifiedLog.rebuildProducerState(
      producerStateManager,
      segments,
      logStartOffsetCheckpoint,
      segment.baseOffset,
      config.recordVersion,
      time,
      reloadFromCleanShutdown = false,
      logIdent)
    // 重建索引文件并验证日志文件，验证失败的部分截掉
    val bytesTruncated = segment.recover(producerStateManager, leaderEpochCache)
    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    producerStateManager.takeSnapshot()
    bytesTruncated
  }

  /**
   * Recover the log segments (if there was an unclean shutdown). Ensures there is at least one
   * active segment, and returns the updated recovery point and next offset after recovery. Along
   * the way, the method suitably updates the LeaderEpochFileCache or ProducerStateManager inside
   * the provided LogComponents.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only
   * called before all logs are loaded.
   *
   * @return a tuple containing (newRecoveryPoint, nextOffset).
   *
   * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
   */
  private[log] def recoverLog(): (Long, Long) = {
    /** return the log end offset if valid */
    def deleteSegmentsIfLogStartGreaterThanLogEnd(): Option[Long] = {
      if (segments.nonEmpty) {
        val logEndOffset = segments.lastSegment.get.readNextOffset
        if (logEndOffset >= logStartOffsetCheckpoint)
          Some(logEndOffset)
        else {
          // 验证分区日志的LEO值不能小于logStartOffsetCheckpoint
          warn(s"Deleting all segments because logEndOffset ($logEndOffset) " +
            s"is smaller than logStartOffset ${logStartOffsetCheckpoint}. " +
            "This could happen if segment files were deleted from the file system.")
          removeAndDeleteSegmentsAsync(segments.values)
          leaderEpochCache.foreach(_.clearAndFlush())
          producerStateManager.truncateFullyAndStartAt(logStartOffsetCheckpoint)
          None
        }
      } else None
    }

    // If we have the clean shutdown marker, skip recovery.
    // 如果不存在以.kafka_cleanshutdown结尾的文件。通常都不存在
    if (!hadCleanShutdown) {
      // 获取到上次恢复点以外的所有unflushed日志段对象(获取全部未刷新的LogSegment，即recoveryPoint之后的全部LogSegment)
      val unflushed = segments.values(recoveryPointCheckpoint, Long.MaxValue).iterator
      var truncated = false

      // 遍历这些unflushed日志段
      while (unflushed.hasNext && !truncated) {
        val segment = unflushed.next()
        info(s"Recovering unflushed segment ${segment.baseOffset}")
        val truncatedBytes =
          try {
            // 执行日志段恢复操作
            recoverSegment(segment)
          } catch {
            case _: InvalidOffsetException =>
              val startOffset = segment.baseOffset
              warn(s"Found invalid offset during recovery. Deleting the" +
                s" corrupt segment and creating an empty one with starting offset $startOffset")
              segment.truncateTo(startOffset)
          }
        // 有验证失败的消息
        if (truncatedBytes > 0) {// 如果有无效的消息导致被截断的字节数不为0，直接删除
          // we had an invalid message, delete all remaining log
          warn(s"Corruption found in segment ${segment.baseOffset}," +
            s" truncating to offset ${segment.readNextOffset}")
          // 剩余部分删除
          removeAndDeleteSegmentsAsync(unflushed.toList)
          truncated = true
        }
      }
    }

    val logEndOffsetOption = deleteSegmentsIfLogStartGreaterThanLogEnd()

    // 这些都做完之后，如果日志段集合为空了
    if (segments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at logStartOffset
      // 至少创建一个新的日志段，以logStartOffset为日志段的起始位移，并加入日志段集合中
      segments.add(
        LogSegment.open(
          dir = dir,
          baseOffset = logStartOffsetCheckpoint,
          config,
          time = time,
          initFileSize = config.initFileSize,
          preallocate = config.preallocate))
    }

    // Update the recovery point if there was a clean shutdown and did not perform any changes to
    // the segment. Otherwise, we just ensure that the recovery point is not ahead of the log end
    // offset. To ensure correctness and to make it easier to reason about, it's best to only advance
    // the recovery point when the log is flushed. If we advanced the recovery point here, we could
    // skip recovery for unflushed segments if the broker crashed after we checkpoint the recovery
    // point and before we flush the segment.
    (hadCleanShutdown, logEndOffsetOption) match {
      case (true, Some(logEndOffset)) =>
        (logEndOffset, logEndOffset)
      case _ =>
        val logEndOffset = logEndOffsetOption.getOrElse(segments.lastSegment.get.readNextOffset)
        (Math.min(recoveryPointCheckpoint, logEndOffset), logEndOffset)
    }
  }

  /**
   * This method deletes the given log segments and the associated producer snapshots, by doing the
   * following for each of them:
   *  - It removes the segment from the segment map so that it will no longer be used for reads.
   *  - It schedules asynchronous deletion of the segments that allows reads to happen concurrently without
   *    synchronization and without the possibility of physically deleting a file while it is being
   *    read.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either
   * called before all logs are loaded or the immediate caller will catch and handle IOException
   *
   * @param segmentsToDelete The log segments to schedule for deletion
   */
  private def removeAndDeleteSegmentsAsync(segmentsToDelete: Iterable[LogSegment]): Unit = {
    if (segmentsToDelete.nonEmpty) {
      // Most callers hold an iterator into the `params.segments` collection and
      // `removeAndDeleteSegmentAsync` mutates it by removing the deleted segment. Therefore,
      // we should force materialization of the iterator here, so that results of the iteration
      // remain valid and deterministic. We should also pass only the materialized view of the
      // iterator to the logic that deletes the segments.
      val toDelete = segmentsToDelete.toList
      info(s"Deleting segments as part of log recovery: ${toDelete.mkString(",")}")
      toDelete.foreach { segment =>
        segments.remove(segment.baseOffset)
      }
      UnifiedLog.deleteSegmentFiles(
        toDelete,
        asyncDelete = true,
        dir,
        topicPartition,
        config,
        scheduler,
        logDirFailureChannel,
        logIdent)
      deleteProducerSnapshotsAsync(segmentsToDelete)
    }
  }

  private def deleteProducerSnapshotsAsync(segments: Iterable[LogSegment]): Unit = {
    UnifiedLog.deleteProducerSnapshots(segments,
      producerStateManager,
      asyncDelete = true,
      scheduler,
      config,
      logDirFailureChannel,
      dir.getParent,
      topicPartition)
  }
}
