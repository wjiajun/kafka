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

package kafka.tools

import java.io._

import com.fasterxml.jackson.databind.node.{IntNode, JsonNodeFactory, ObjectNode, TextNode}
import kafka.coordinator.group.GroupMetadataManager
import kafka.coordinator.transaction.TransactionLog
import kafka.log._
import kafka.serializer.Decoder
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.common.metadata.{MetadataJsonConverters, MetadataRecordType}
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.MetadataRecordSerde

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DumpLogSegments {

  // visible for testing
  private[tools] val RecordIndent = "|"

  def main(args: Array[String]): Unit = {
    val opts = new DumpLogSegmentsOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to parse a log file and dump its contents to the console, useful for debugging a seemingly corrupt log segment.")
    opts.checkArgs()

    // key: 索引文件的绝对路径
    // value: 索引项中的相对offset和消息的offset组成的元祖的集合
    val misMatchesForIndexFilesMap = mutable.Map[String, List[(Long, Long)]]()
    val timeIndexDumpErrors = new TimeIndexDumpErrors
    // 如果消息是未压缩的，则需要offset是连续的，若不连续，则记录到nonConsecutivePairsForLogFilesMap集合中，key为日志文件的绝对路径，value是出现不连续消息的前后两个offset组成的元组集合
    val nonConsecutivePairsForLogFilesMap = mutable.Map[String, List[(Long, Long)]]()

    for (arg <- opts.files) {
      // 处理命令参数指定的文件集合
      val file = new File(arg)
      println(s"Dumping $file")

      val filename = file.getName
      val suffix = filename.substring(filename.lastIndexOf("."))
      suffix match {
        case UnifiedLog.LogFileSuffix =>
          // 打印日志文件
          dumpLog(file, opts.shouldPrintDataLog, nonConsecutivePairsForLogFilesMap, opts.isDeepIteration,
            opts.messageParser, opts.skipRecordMetadata)
        case UnifiedLog.IndexFileSuffix =>
          // 打印索引文件
          dumpIndex(file, opts.indexSanityOnly, opts.verifyOnly, misMatchesForIndexFilesMap, opts.maxMessageSize)
        case UnifiedLog.TimeIndexFileSuffix =>
          dumpTimeIndex(file, opts.indexSanityOnly, opts.verifyOnly, timeIndexDumpErrors)
        case UnifiedLog.ProducerSnapshotFileSuffix =>
          dumpProducerIdSnapshot(file)
        case UnifiedLog.TxnIndexFileSuffix =>
          dumpTxnIndex(file)
        case _ =>
          System.err.println(s"Ignoring unknown file $file")
      }
    }

    misMatchesForIndexFilesMap.forKeyValue { (fileName, listOfMismatches) =>
      System.err.println(s"Mismatches in :$fileName")
      listOfMismatches.foreach { case (indexOffset, logOffset) =>
        System.err.println(s"  Index offset: $indexOffset, log offset: $logOffset")
      }
    }

    timeIndexDumpErrors.printErrors()

    nonConsecutivePairsForLogFilesMap.forKeyValue { (fileName, listOfNonConsecutivePairs) =>
      System.err.println(s"Non-consecutive offsets in $fileName")
      listOfNonConsecutivePairs.foreach { case (first, second) =>
        System.err.println(s"  $first is followed by $second")
      }
    }
  }

  private def dumpTxnIndex(file: File): Unit = {
    val index = new TransactionIndex(UnifiedLog.offsetFromFile(file), file)
    for (abortedTxn <- index.allAbortedTxns) {
      println(s"version: ${abortedTxn.version} producerId: ${abortedTxn.producerId} firstOffset: ${abortedTxn.firstOffset} " +
        s"lastOffset: ${abortedTxn.lastOffset} lastStableOffset: ${abortedTxn.lastStableOffset}")
    }
  }

  private def dumpProducerIdSnapshot(file: File): Unit = {
    try {
      ProducerStateManager.readSnapshot(file).foreach { entry =>
        print(s"producerId: ${entry.producerId} producerEpoch: ${entry.producerEpoch} " +
          s"coordinatorEpoch: ${entry.coordinatorEpoch} currentTxnFirstOffset: ${entry.currentTxnFirstOffset} " +
          s"lastTimestamp: ${entry.lastTimestamp} ")
        entry.batchMetadata.headOption.foreach { metadata =>
          print(s"firstSequence: ${metadata.firstSeq} lastSequence: ${metadata.lastSeq} " +
            s"lastOffset: ${metadata.lastOffset} offsetDelta: ${metadata.offsetDelta} timestamp: ${metadata.timestamp}")
        }
        println()
      }
    } catch {
      case e: CorruptSnapshotException =>
        System.err.println(e.getMessage)
    }
  }

  /* print out the contents of the index */
  // Visible for testing
  private[tools] def dumpIndex(file: File,
                               indexSanityOnly: Boolean,
                               verifyOnly: Boolean,
                               misMatchesForIndexFilesMap: mutable.Map[String, List[(Long, Long)]],
                               maxMessageSize: Int): Unit = {
    // 获取baseOffset
    val startOffset = file.getName.split("\\.")(0).toLong
    // 获取对应的日志文件
    val logFile = new File(file.getAbsoluteFile.getParent, file.getName.split("\\.")(0) + UnifiedLog.LogFileSuffix)
    val fileRecords = FileRecords.open(logFile, false)
    // 创建OffsetIndex
    val index = new OffsetIndex(file, baseOffset = startOffset, writable = false)

    if (index.entries == 0) {
      println(s"$file is empty.")
      return
    }

    //Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
    // 对索引文件做简单检查，例如：最后一个索引项的offset大于baseOffset,索引文件大小是8的倍数
    if (indexSanityOnly) {
      index.sanityCheck()
      println(s"$file passed sanity check.")
      return
    }

    for (i <- 0 until index.entries) {
      // 读取索引项
      val entry = index.entry(i)

      // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
      if (entry.offset == index.baseOffset && i > 0)
        return

      // 获取一个分片，分片的起始位置是索引项指定的位置
      val slice = fileRecords.slice(entry.position, maxMessageSize)
      // 从分片中获取第一条消息
      val firstBatchLastOffset = slice.batches.iterator.next().lastOffset
      if (firstBatchLastOffset != entry.offset) {
        // 如果消息的offset与索引项的offset不匹配，则需要记录下来
        var misMatchesSeq = misMatchesForIndexFilesMap.getOrElse(file.getAbsolutePath, List[(Long, Long)]())
        misMatchesSeq ::= (entry.offset, firstBatchLastOffset)
        misMatchesForIndexFilesMap.put(file.getAbsolutePath, misMatchesSeq)
      }
      if (!verifyOnly) // 输出索引的内容
        println(s"offset: ${entry.offset} position: ${entry.position}")
    }
  }

  // Visible for testing
  private[tools] def dumpTimeIndex(file: File,
                                   indexSanityOnly: Boolean,
                                   verifyOnly: Boolean,
                                   timeIndexDumpErrors: TimeIndexDumpErrors): Unit = {
    val startOffset = file.getName.split("\\.")(0).toLong
    val logFile = new File(file.getAbsoluteFile.getParent, file.getName.split("\\.")(0) + UnifiedLog.LogFileSuffix)
    val fileRecords = FileRecords.open(logFile, false)
    val indexFile = new File(file.getAbsoluteFile.getParent, file.getName.split("\\.")(0) + UnifiedLog.IndexFileSuffix)
    val index = new OffsetIndex(indexFile, baseOffset = startOffset, writable = false)
    val timeIndex = new TimeIndex(file, baseOffset = startOffset, writable = false)

    try {
      //Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
      if (indexSanityOnly) {
        timeIndex.sanityCheck()
        println(s"$file passed sanity check.")
        return
      }

      var prevTimestamp = RecordBatch.NO_TIMESTAMP
      for (i <- 0 until timeIndex.entries) {
        val entry = timeIndex.entry(i)

        // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
        if (entry.offset == timeIndex.baseOffset && i > 0)
          return

        val position = index.lookup(entry.offset).position
        val partialFileRecords = fileRecords.slice(position, Int.MaxValue)
        val batches = partialFileRecords.batches.asScala
        var maxTimestamp = RecordBatch.NO_TIMESTAMP
        // We first find the message by offset then check if the timestamp is correct.
        batches.find(_.lastOffset >= entry.offset) match {
          case None =>
            timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset,
              -1.toLong)
          case Some(batch) if batch.lastOffset != entry.offset =>
            timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset, batch.lastOffset)
          case Some(batch) =>
            for (record <- batch.asScala)
              maxTimestamp = math.max(maxTimestamp, record.timestamp)

            if (maxTimestamp != entry.timestamp)
              timeIndexDumpErrors.recordMismatchTimeIndex(file, entry.timestamp, maxTimestamp)

            if (prevTimestamp >= entry.timestamp)
              timeIndexDumpErrors.recordOutOfOrderIndexTimestamp(file, entry.timestamp, prevTimestamp)
        }
        if (!verifyOnly)
          println(s"timestamp: ${entry.timestamp} offset: ${entry.offset}")
        prevTimestamp = entry.timestamp
      }
    } finally {
      fileRecords.closeHandlers()
      index.closeHandler()
      timeIndex.closeHandler()
    }
  }

  private[kafka] trait MessageParser[K, V] {
    def parse(record: Record): (Option[K], Option[V])
  }

  private class DecoderMessageParser[K, V](keyDecoder: Decoder[K], valueDecoder: Decoder[V]) extends MessageParser[K, V] {
    override def parse(record: Record): (Option[K], Option[V]) = {
      val key = if (record.hasKey)
        Some(keyDecoder.fromBytes(Utils.readBytes(record.key)))
      else
        None

      if (!record.hasValue) {
        (key, None)
      } else {
        val payload = Some(valueDecoder.fromBytes(Utils.readBytes(record.value)))

        (key, payload)
      }
    }
  }

  /* print out the contents of the log */
  private def dumpLog(file: File,
                      printContents: Boolean,
                      nonConsecutivePairsForLogFilesMap: mutable.Map[String, List[(Long, Long)]],
                      isDeepIteration: Boolean,
                      parser: MessageParser[_, _],
                      skipRecordMetadata: Boolean): Unit = {
    val startOffset = file.getName.split("\\.")(0).toLong
    println("Starting offset: " + startOffset)
    val fileRecords = FileRecords.open(file, false) // 创建FileRecords对象
    try {
      var validBytes = 0L // 已经通过验证的字节数
      var lastOffset = -1L // 记录Offset

      for (batch <- fileRecords.batches.asScala) {
        printBatchLevel(batch, validBytes)
        if (isDeepIteration) {
          for (record <- batch.asScala) { // 遍历日志的消息
            if (lastOffset == -1) // 记录上次循环处理的offset
              lastOffset = record.offset
            else if (record.offset != lastOffset + 1) {
              // 如果消息是未压缩的，则需要offset是连续的，若不连续，则需要进行记录
              var nonConsecutivePairsSeq = nonConsecutivePairsForLogFilesMap.getOrElse(file.getAbsolutePath, List[(Long, Long)]())
              nonConsecutivePairsSeq ::= (lastOffset, record.offset)
              nonConsecutivePairsForLogFilesMap.put(file.getAbsolutePath, nonConsecutivePairsSeq)
            }
            lastOffset = record.offset

            var prefix = s"$RecordIndent "
            if (!skipRecordMetadata) {
              // 输出消息的相关信息
              print(s"${prefix}offset: ${record.offset} ${batch.timestampType}: ${record.timestamp} " +
                s"keySize: ${record.keySize} valueSize: ${record.valueSize}")
              prefix = " "

              if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
                print(" sequence: " + record.sequence + " headerKeys: " + record.headers.map(_.key).mkString("[", ",", "]"))
              }
              record match {
                case r: AbstractLegacyRecordBatch => print(s" isValid: ${r.isValid} crc: ${r.checksum}}")
                case _ =>
              }

              if (batch.isControlBatch) {
                val controlTypeId = ControlRecordType.parseTypeId(record.key)
                ControlRecordType.fromTypeId(controlTypeId) match {
                  case ControlRecordType.ABORT | ControlRecordType.COMMIT =>
                    val endTxnMarker = EndTransactionMarker.deserialize(record)
                    print(s" endTxnMarker: ${endTxnMarker.controlType} coordinatorEpoch: ${endTxnMarker.coordinatorEpoch}")
                  case controlType =>
                    print(s" controlType: $controlType($controlTypeId)")
                }
              }
            }
            if (printContents && !batch.isControlBatch) {
              // 解析消息
              val (key, payload) = parser.parse(record)
              key.foreach { key => // 输出消息的key
                print(s"${prefix}key: $key")
                prefix = " "
              }
              // 输出消息的value
              payload.foreach(payload => print(s" payload: $payload"))
            }
            println()
          }
        }
        // 记录已通过验证的字节数
        validBytes += batch.sizeInBytes
      }
      val trailingBytes = fileRecords.sizeInBytes - validBytes
      if (trailingBytes > 0)
        println(s"Found $trailingBytes invalid bytes at the end of ${file.getName}")
    } finally fileRecords.closeHandlers()
  }

  private def printBatchLevel(batch: FileLogInputStream.FileChannelRecordBatch, accumulativeBytes: Long): Unit = {
    if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
      print("baseOffset: " + batch.baseOffset + " lastOffset: " + batch.lastOffset + " count: " + batch.countOrNull +
        " baseSequence: " + batch.baseSequence + " lastSequence: " + batch.lastSequence +
        " producerId: " + batch.producerId + " producerEpoch: " + batch.producerEpoch +
        " partitionLeaderEpoch: " + batch.partitionLeaderEpoch + " isTransactional: " + batch.isTransactional +
        " isControl: " + batch.isControlBatch + " deleteHorizonMs: " + batch.deleteHorizonMs)
    else
      print("offset: " + batch.lastOffset)

    println(" position: " + accumulativeBytes + " " + batch.timestampType + ": " + batch.maxTimestamp +
      " size: " + batch.sizeInBytes + " magic: " + batch.magic +
      " compresscodec: " + batch.compressionType.name + " crc: " + batch.checksum + " isvalid: " + batch.isValid)
  }

  class TimeIndexDumpErrors {
    val misMatchesForTimeIndexFilesMap = mutable.Map[String, ArrayBuffer[(Long, Long)]]()
    val outOfOrderTimestamp = mutable.Map[String, ArrayBuffer[(Long, Long)]]()
    val shallowOffsetNotFound = mutable.Map[String, ArrayBuffer[(Long, Long)]]()

    def recordMismatchTimeIndex(file: File, indexTimestamp: Long, logTimestamp: Long): Unit = {
      val misMatchesSeq = misMatchesForTimeIndexFilesMap.getOrElse(file.getAbsolutePath, new ArrayBuffer[(Long, Long)]())
      if (misMatchesSeq.isEmpty)
        misMatchesForTimeIndexFilesMap.put(file.getAbsolutePath, misMatchesSeq)
      misMatchesSeq += ((indexTimestamp, logTimestamp))
    }

    def recordOutOfOrderIndexTimestamp(file: File, indexTimestamp: Long, prevIndexTimestamp: Long): Unit = {
      val outOfOrderSeq = outOfOrderTimestamp.getOrElse(file.getAbsolutePath, new ArrayBuffer[(Long, Long)]())
      if (outOfOrderSeq.isEmpty)
        outOfOrderTimestamp.put(file.getAbsolutePath, outOfOrderSeq)
      outOfOrderSeq += ((indexTimestamp, prevIndexTimestamp))
    }

    def recordShallowOffsetNotFound(file: File, indexOffset: Long, logOffset: Long): Unit = {
      val shallowOffsetNotFoundSeq = shallowOffsetNotFound.getOrElse(file.getAbsolutePath, new ArrayBuffer[(Long, Long)]())
      if (shallowOffsetNotFoundSeq.isEmpty)
        shallowOffsetNotFound.put(file.getAbsolutePath, shallowOffsetNotFoundSeq)
      shallowOffsetNotFoundSeq += ((indexOffset, logOffset))
    }

    def printErrors(): Unit = {
      misMatchesForTimeIndexFilesMap.foreach {
        case (fileName, listOfMismatches) => {
          System.err.println("Found timestamp mismatch in :" + fileName)
          listOfMismatches.foreach(m => {
            System.err.println("  Index timestamp: %d, log timestamp: %d".format(m._1, m._2))
          })
        }
      }

      outOfOrderTimestamp.foreach {
        case (fileName, outOfOrderTimestamps) => {
          System.err.println("Found out of order timestamp in :" + fileName)
          outOfOrderTimestamps.foreach(m => {
            System.err.println("  Index timestamp: %d, Previously indexed timestamp: %d".format(m._1, m._2))
          })
        }
      }

      shallowOffsetNotFound.values.foreach { listOfShallowOffsetNotFound =>
        System.err.println("The following indexed offsets are not found in the log.")
        listOfShallowOffsetNotFound.foreach { case (indexedOffset, logOffset) =>
          System.err.println(s"Indexed offset: $indexedOffset, found log offset: $logOffset")
        }
      }
    }
  }

  private class OffsetsMessageParser extends MessageParser[String, String] {
    override def parse(record: Record): (Option[String], Option[String]) = {
      GroupMetadataManager.formatRecordKeyAndValue(record)
    }
  }

  private class TransactionLogMessageParser extends MessageParser[String, String] {
    override def parse(record: Record): (Option[String], Option[String]) = {
      TransactionLog.formatRecordKeyAndValue(record)
    }
  }

  private class ClusterMetadataLogMessageParser extends MessageParser[String, String] {
    val metadataRecordSerde = new MetadataRecordSerde()

    override def parse(record: Record): (Option[String], Option[String]) = {
      val output = try {
        val messageAndVersion = metadataRecordSerde.
          read(new ByteBufferAccessor(record.value), record.valueSize())
        val json = new ObjectNode(JsonNodeFactory.instance)
        json.set("type", new TextNode(MetadataRecordType.fromId(
          messageAndVersion.message().apiKey()).toString))
        json.set("version", new IntNode(messageAndVersion.version()))
        json.set("data", MetadataJsonConverters.writeJson(
          messageAndVersion.message(), messageAndVersion.version()))
        json.toString()
      } catch {
        case e: Throwable => {
          s"Error at ${record.offset}, skipping. ${e.getMessage}"
        }
      }
      // No keys for metadata records
      (None, Some(output))
    }
  }

  private class DumpLogSegmentsOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val printOpt = parser.accepts("print-data-log", "if set, printing the messages content when dumping data logs. Automatically set if any decoder option is specified.")
    val verifyOpt = parser.accepts("verify-index-only", "if set, just verify the index log without printing its content.")
    val indexSanityOpt = parser.accepts("index-sanity-check", "if set, just checks the index sanity without printing its content. " +
      "This is the same check that is executed on broker startup to determine if an index needs rebuilding or not.")
    val filesOpt = parser.accepts("files", "REQUIRED: The comma separated list of data and index log files to be dumped.")
      .withRequiredArg
      .describedAs("file1, file2, ...")
      .ofType(classOf[String])
    val maxMessageSizeOpt = parser.accepts("max-message-size", "Size of largest message.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(5 * 1024 * 1024)
    val deepIterationOpt = parser.accepts("deep-iteration", "if set, uses deep instead of shallow iteration. Automatically set if print-data-log is enabled.")
    val valueDecoderOpt = parser.accepts("value-decoder-class", "if set, used to deserialize the messages. This class should implement kafka.serializer.Decoder trait. Custom jar should be available in kafka/libs directory.")
      .withOptionalArg()
      .ofType(classOf[java.lang.String])
      .defaultsTo("kafka.serializer.StringDecoder")
    val keyDecoderOpt = parser.accepts("key-decoder-class", "if set, used to deserialize the keys. This class should implement kafka.serializer.Decoder trait. Custom jar should be available in kafka/libs directory.")
      .withOptionalArg()
      .ofType(classOf[java.lang.String])
      .defaultsTo("kafka.serializer.StringDecoder")
    val offsetsOpt = parser.accepts("offsets-decoder", "if set, log data will be parsed as offset data from the " +
      "__consumer_offsets topic.")
    val transactionLogOpt = parser.accepts("transaction-log-decoder", "if set, log data will be parsed as " +
      "transaction metadata from the __transaction_state topic.")
    val clusterMetadataOpt = parser.accepts("cluster-metadata-decoder", "if set, log data will be parsed as cluster metadata records.")
    val skipRecordMetadataOpt = parser.accepts("skip-record-metadata", "whether to skip printing metadata for each record.")
    options = parser.parse(args : _*)

    def messageParser: MessageParser[_, _] =
      if (options.has(offsetsOpt)) {
        new OffsetsMessageParser
      } else if (options.has(transactionLogOpt)) {
        new TransactionLogMessageParser
      } else if (options.has(clusterMetadataOpt)) {
        new ClusterMetadataLogMessageParser
      } else {
        val valueDecoder: Decoder[_] = CoreUtils.createObject[Decoder[_]](options.valueOf(valueDecoderOpt), new VerifiableProperties)
        val keyDecoder: Decoder[_] = CoreUtils.createObject[Decoder[_]](options.valueOf(keyDecoderOpt), new VerifiableProperties)
        new DecoderMessageParser(keyDecoder, valueDecoder)
      }

    lazy val shouldPrintDataLog: Boolean = options.has(printOpt) ||
      options.has(offsetsOpt) ||
      options.has(transactionLogOpt) ||
      options.has(clusterMetadataOpt) ||
      options.has(valueDecoderOpt) ||
      options.has(keyDecoderOpt)

    lazy val skipRecordMetadata = options.has(skipRecordMetadataOpt)
    lazy val isDeepIteration: Boolean = options.has(deepIterationOpt) || shouldPrintDataLog
    lazy val verifyOnly: Boolean = options.has(verifyOpt)
    lazy val indexSanityOnly: Boolean = options.has(indexSanityOpt)
    lazy val files = options.valueOf(filesOpt).split(",")
    lazy val maxMessageSize = options.valueOf(maxMessageSizeOpt).intValue()

    def checkArgs(): Unit = CommandLineUtils.checkRequiredArgs(parser, options, filesOpt)

  }
}
