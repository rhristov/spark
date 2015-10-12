/*
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

package org.apache.spark.shuffle

import java.io._

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkEnv, Logging}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.storage._
import org.apache.spark.util.Utils

import IndexShuffleBlockResolver.NOOP_REDUCE_ID

import scala.collection.mutable.ArrayBuffer

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(conf: SparkConf) extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = SparkEnv.get.blockManager

  private val transportConf = SparkTransportConf.fromSparkConf(conf)

  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId))
  }

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   * */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   * */
  def writeIndexFile(shuffleId: Int, mapId: Int, lengths: Array[Long]): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)))
    Utils.tryWithSafeFinally {
      // We take in lengths of each block, need to convert it to offsets.
      var offset = 0L
      out.writeLong(offset)
      for (length <- lengths) {
        offset += length
        out.writeLong(offset)
      }
    } {
      out.close()
    }
  }

  override def getBlockData(blockId: ShuffleBlockId): (ManagedBuffer, Seq[Long]) = {
    //println("get blockdata index shuffle block resolver")
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    val in = new DataInputStream(new FileInputStream(indexFile))
    try {
      val partitionSizes = new ArrayBuffer[Long]
      ByteStreams.skipFully(in, blockId.startPartition * 8)
      val offset = in.readLong()
      var part = blockId.startPartition
      var nextOffset = offset

      while (part != blockId.endPartition) {
        val currentOffset = in.readLong()
        partitionSizes += currentOffset - nextOffset
        part += 1

        nextOffset = currentOffset
      }

      val nioBuf2 = (new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset)).nioByteBuffer()
      //println((0 until nioBuf2.limit()).map(i => nioBuf2.get(i)).mkString(", "))
      //println("offset ", offset, "   nextoffset", nextOffset, " startPartition,", blockId.startPartition, " end partition ", blockId.endPartition, partitionSizes)
      (new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset),
       partitionSizes.toSeq)
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store and DiskBlockObjectWriter.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  // TODO: Avoid this entirely by having the DiskBlockObjectWriter not require a BlockId.
  val NOOP_REDUCE_ID = 0
}
