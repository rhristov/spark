package org.apache.spark.sort

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import _root_.io.netty.buffer.ByteBuf
import org.apache.spark._
import org.apache.spark.network.{ManagedBuffer, FileSegmentManagedBuffer, NettyManagedBuffer}
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.util.collection.{Sorter, SortDataFormat}


/**
 * A version of the sort code that uses Unsafe to allocate off-heap blocks.
 *
 * See also [[UnsafeSerializer]] and [[UnsafeOrdering]].
 */
object UnsafeSort extends Logging {

  val NUM_EBS = 8

  val ord = new UnsafeOrdering

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt

    val conf = new SparkConf()

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val sc = new SparkContext(
      new SparkConf().setAppName(s"UnsafeSort - $sizeInGB GB - $numParts partitions"))
    val input = createInputRDDUnsafe(sc, sizeInGB, numParts)

    val hosts = Sort.readSlaves()

    val partitioner = new UnsafePartitioner(numParts)
    val sorted = new ShuffledRDD(input, partitioner)
      .setKeyOrdering(new UnsafeOrdering)
      .setSerializer(new UnsafeSerializer(recordsPerPartition))

    val recordsAfterSort: Long = sorted.mapPartitionsWithIndex { (part, iter) =>

      val startTime = System.currentTimeMillis()
      val sortBuffer = UnsafeSort.sortBuffers.get()
      assert(sortBuffer != null)
      var offset = 0L
      var numShuffleBlocks = 0

      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      scala.Console.flush()

      while (iter.hasNext) {
        val a = iter.next()._2.asInstanceOf[ManagedBuffer]
        a match {
          case buf: NettyManagedBuffer =>
            val bytebuf = buf.convertToNetty().asInstanceOf[ByteBuf]
            val len = bytebuf.readableBytes()
            assert(len % 100 == 0)
            assert(bytebuf.hasMemoryAddress)

            val start = bytebuf.memoryAddress + bytebuf.readerIndex
            UnsafeSort.UNSAFE.copyMemory(start, sortBuffer.address + offset, len)
            offset += len
            bytebuf.release()

          case buf: FileSegmentManagedBuffer =>
            val fs = new FileInputStream(buf.file)
            val channel = fs.getChannel
            channel.position(buf.offset)
            assert(buf.length < 4 * 1024 * 1024)
            sortBuffer.ioBuf.clear()
            sortBuffer.ioBuf.limit(buf.length.toInt)
            sortBuffer.setIoBufAddress(sortBuffer.address + offset)
            //var read0 = 0L
            //while (read0 < buf.length) {
              val read0 = channel.read(sortBuffer.ioBuf)
              //println(s"read $thisRead total $read0 buf size ${buf.length} ")
              //read0 += thisRead
            //}
            assert(read0 == buf.length)
            offset += read0
            channel.close()
            fs.close()
        }

        numShuffleBlocks += 1
      }

      val timeTaken = System.currentTimeMillis() - startTime
      logInfo(s"Reduce: took $timeTaken ms to fetch $numShuffleBlocks shuffle blocks $offset bytes")
      println(s"Reduce: took $timeTaken ms to fetch $numShuffleBlocks shuffle blocks $offset bytes")

      buildLongPointers(sortBuffer, offset)
      val pointers = sortBuffer.pointers

      val numRecords = (offset / 100).toInt

      // Sort!!!
      {
        val startTime = System.currentTimeMillis
        val sorter = new Sorter(new LongArraySorter).sort(
          sortBuffer.pointers, 0, numRecords, ord)
        //radixSort(sortBuffer, 0, numRecords) // This is slower than timsort if data partly sorted
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"Reduce: Sorting $numRecords records took $timeTaken ms")
        println(s"Reduce: Sorting $numRecords records took $timeTaken ms")
        scala.Console.flush()
      }

      val count: Long = {
        val startTime = System.currentTimeMillis
        val volIndex = part % NUM_EBS
        val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts-out"
        if (!new File(baseFolder).exists()) {
          new File(baseFolder).mkdirs()
        }

        val outputFile = s"$baseFolder/part$part.dat"
        val os = new BufferedOutputStream(new FileOutputStream(outputFile), 4 * 1024 * 1024)
        val buf = new Array[Byte](100)
        val arrOffset = BYTE_ARRAY_BASE_OFFSET
        var i = 0
        while (i < numRecords) {
          val addr = pointers(i)
          UNSAFE.copyMemory(null, addr, buf, arrOffset, 100)
          os.write(buf)
          i += 1
        }
        os.close()
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"Reduce: writing $numRecords records took $timeTaken ms")
        println(s"Reduce: writing $numRecords records took $timeTaken ms")
        i.toLong
      }
      Iterator(count)
    }.reduce(_ + _)

    println("total number of records: " + recordsAfterSort)
  }

  final val UNSAFE: sun.misc.Unsafe = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get().asInstanceOf[sun.misc.Unsafe]
  }

  final val BYTE_ARRAY_BASE_OFFSET: Long = UNSAFE.arrayBaseOffset(classOf[Array[Byte]])

  /**
   * A class to hold information needed to run sort within each partition.
   *
   * @param capacity number of records the buffer can support. Each record is 100 bytes.
   */
  final class SortBuffer(capacity: Long) {
    require(capacity <= Int.MaxValue)

    /** size of the buffer, starting at [[address]] */
    val len: Long = capacity * 100

    /** address pointing to a block of memory off heap */
    val address: Long = {
      val blockSize = capacity * 100
      logInfo(s"Allocating $blockSize bytes")
      val blockAddress = UNSAFE.allocateMemory(blockSize)
      logInfo(s"Allocating $blockSize bytes ... allocated at $blockAddress")
      println(s"Allocating $blockSize bytes ... allocated at $blockAddress")
      blockAddress
    }

    /**
     * A dummy direct buffer. We use this in a very unconventional way. We use reflection to
     * change the address of the offheap memory to our large buffer, and then use channel read
     * to directly read the data into our large buffer.
     *
     * i.e. the 4MB allocated here is not used at all. We are only the 4MB for tracking.
     */
    val ioBuf: ByteBuffer = ByteBuffer.allocateDirect(5 * 1000 * 1000)

    /** list of pointers to each block, used for sorting. */
    var pointers: Array[Long] = new Array[Long](capacity.toInt)

    /** a second pointers array to facilitate merge-sort */
    var pointers2: Array[Long] = new Array[Long](capacity.toInt)

    private[this] val ioBufAddressField = {
      val f = classOf[java.nio.Buffer].getDeclaredField("address")
      f.setAccessible(true)
      f
    }

    /** Return the memory address of the memory the [[ioBuf]] points to. */
    def ioBufAddress: Long = ioBufAddressField.getLong(ioBuf)

    def setIoBufAddress(addr: Long) = {
      ioBufAddressField.setLong(ioBuf, addr)
    }
  }

  /** A thread local variable storing a pointer to the buffer allocated off-heap. */
  val sortBuffers = new ThreadLocal[SortBuffer]

  def readFileIntoBuffer(inputFile: String, sortBuffer: SortBuffer) {
    logInfo(s"reading file $inputFile")
    val startTime = System.currentTimeMillis()
    val fileSize = new File(inputFile).length
    assert(fileSize % 100 == 0)

    val baseAddress: Long = sortBuffer.address
    var is: FileInputStream = null
    var channel: FileChannel = null
    var read = 0L
    try {
      is = new FileInputStream(inputFile)
      channel = is.getChannel()
      while (read < fileSize) {
        // This should read read0 bytes directly into our buffer
        sortBuffer.setIoBufAddress(baseAddress + read)
        val read0 = channel.read(sortBuffer.ioBuf)
        //UNSAFE.copyMemory(sortBuffer.ioBufAddress, baseAddress + read, read0)
        sortBuffer.ioBuf.clear()
        read += read0
      }
    } finally {
      if (channel != null) {
        channel.close()
      }
      if (is != null) {
        is.close()
      }
    }
    val timeTaken = System.currentTimeMillis() - startTime
    logInfo(s"finished reading file $inputFile ($read bytes), took $timeTaken ms")
    println(s"finished reading file $inputFile ($read bytes), took $timeTaken ms")
    scala.Console.flush()
    assert(read == fileSize)
  }

  def buildLongPointers(sortBuffer: SortBuffer, bufferSize: Long) {
    val startTime = System.currentTimeMillis()
    // Create the pointers array
    var pos = 0L
    var i = 0
    val pointers = sortBuffer.pointers
    while (pos < bufferSize) {
      pointers(i) = sortBuffer.address + pos
      pos += 100
      i += 1
    }
    val timeTaken = System.currentTimeMillis() - startTime
    logInfo(s"finished building index, took $timeTaken ms")
    println(s"finished building index, took $timeTaken ms")
    scala.Console.flush()
  }

  /** Read chunks from a file and sort them in a background thread, producing a sorted buffer at the end */
  def readFileAndSort(inputFile: String, sortBuffer: SortBuffer) {
    logInfo(s"reading and sorting file $inputFile")
    var startTime = System.currentTimeMillis()
    val fileSize = new File(inputFile).length
    assert(fileSize % 100 == 0)
    assert(sortBuffer.ioBuf.limit % 100 == 0)

    // Size of chunks we'll sort in the background thread; this is set to get 8 chunks
    val chunkSize = 50 * sortBuffer.ioBuf.limit

    // A queue of requests we send to the thread; each one specifies a range in the buffer that
    // we're ready to sort (as record indices), except when we pass in (-1, -1) for the last one
    val sortRequests = new java.util.concurrent.LinkedBlockingQueue[(Int, Int)]

    // A set of sorted ranges we have in our buffer (specified as record indices); we use this to
    // merge-sort later
    var sortedRanges = new scala.collection.mutable.ArrayBuffer[(Int, Int)]

    val backgroundThread = new Thread() {
      override def run() {
        while (true) {
          val range = sortRequests.take()
          if (range == (-1, -1)) {
            return
          }
          //new Sorter(new LongArraySorter).sort(sortBuffer.pointers, range._1, range._2, ord)
          radixSort(sortBuffer, range._1, range._2)
        }
      }
    }
    backgroundThread.start()

    val baseAddress: Long = sortBuffer.address

    // Initialize pointers array since we use it for sorting
    var pos = 0L
    var i = 0
    val pointers = sortBuffer.pointers
    while (pos < fileSize) {
      pointers(i) = baseAddress + pos
      pos += 100
      i += 1
    }

    var is: FileInputStream = null
    var channel: FileChannel = null
    var read = 0L
    try {
      is = new FileInputStream(inputFile)
      channel = is.getChannel()
      while (read < fileSize) {
        // This should read read0 bytes directly into our buffer
        sortBuffer.setIoBufAddress(baseAddress + read)
        val read0 = channel.read(sortBuffer.ioBuf)
        //UNSAFE.copyMemory(sortBuffer.ioBufAddress, baseAddress + read, read0)
        sortBuffer.ioBuf.clear()
        read += read0
        if (read % chunkSize == 0) {
          val range = (((read - chunkSize) / 100).toInt, (read / 100).toInt)
          sortRequests.put(range)
          sortedRanges += range
        }
      }
      if (read % chunkSize != 0) {
        val range = (((read - (read % chunkSize)) / 100).toInt, (read / 100).toInt)
        sortRequests.put(range)
        sortedRanges += range
      }
    } finally {
      if (channel != null) {
        channel.close()
      }
      if (is != null) {
        is.close()
      }
    }

    sortRequests.put((-1, -1))
    backgroundThread.join()

    var timeTaken = System.currentTimeMillis() - startTime
    logInfo(s"finished reading and sorting chunks in $inputFile ($read bytes), took $timeTaken ms")
    println(s"finished reading and sorting chunks in $inputFile ($read bytes), took $timeTaken ms")
    scala.Console.flush()
    assert(read == fileSize)

    // Merge the sorted ranges, two by two, until we're down to one range
    startTime = System.currentTimeMillis()
    while (sortedRanges.size > 1) {
      println("sorted ranges: " + sortedRanges.mkString(", "))
      val newRanges = new scala.collection.mutable.ArrayBuffer[(Int, Int)]
      val pointers = sortBuffer.pointers
      val pointers2 = sortBuffer.pointers2
      for (i <- 0 until sortedRanges.size / 2) {
        val (s1, e1) = sortedRanges(2 * i)
        val (s2, e2) = sortedRanges(2 * i + 1)
        assert(e1 == s2)
        // Merge the records from the two ranges into pointers2
        var i1 = s1
        var i2 = s2
        var pos = s1
        while (i1 < e1 && i2 < e2) {
          if (ord.compare(pointers(i1), pointers(i2)) < 0) {
            pointers2(pos) = pointers(i1)
            i1 += 1
            pos += 1
          } else {
            pointers2(pos) = pointers(i2)
            i2 += 1
            pos += 1
          }
        }
        while (i1 < e1) {
          pointers2(pos) = pointers(i1)
          i1 += 1
          pos += 1
        }
        while (i2 < e2) {
          pointers2(pos) = pointers(i2)
          i2 += 1
          pos += 1
        }
        newRanges += ((s1, e2))
      }
      if (sortedRanges.size % 2 == 1) {
        // Copy in the last range unmodified
        val range = sortedRanges.last
        newRanges += range
        System.arraycopy(pointers, range._1, pointers2, range._1, range._2 - range._1)
      }
      sortBuffer.pointers = pointers2
      sortBuffer.pointers2 = pointers
      sortedRanges = newRanges
    }

    timeTaken = System.currentTimeMillis() - startTime
    logInfo(s"finished merge pass of $inputFile ($read bytes), took $timeTaken ms")
    println(s"finished merge pass of $inputFile ($read bytes), took $timeTaken ms")
    scala.Console.flush()
    assert(read == fileSize)
  }

  def createInputRDDUnsafe(sc: SparkContext, sizeInGB: Int, numParts: Int)
    : RDD[(Long, Array[Long])] = {

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val totalRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(totalRecords.toDouble / numParts).toLong

    val hosts = Sort.readSlaves()
    new NodeLocalRDD[(Long, Array[Long])](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val start = recordsPerPartition * part
        val volIndex = part % NUM_EBS

        val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts"
        val outputFile = s"$baseFolder/part$part.dat"

        val fileSize = new File(outputFile).length
        assert(fileSize % 100 == 0)

        if (sortBuffers.get == null) {
          // Allocate 10% overhead since after shuffle the partitions can get slightly uneven.
          val capacity = recordsPerPartition + recordsPerPartition / 10
          sortBuffers.set(new SortBuffer(capacity))
        }

        val sortBuffer = sortBuffers.get()

        /*
        readFileIntoBuffer(outputFile, sortBuffer)
        buildLongPointers(sortBuffer, fileSize)

        // Sort!!!
        {
          val startTime = System.currentTimeMillis
          //val sorter = new Sorter(new LongArraySorter).sort(
          //  sortBuffer.pointers, 0, recordsPerPartition.toInt, ord)
          radixSort(sortBuffer, 0, recordsPerPartition.toInt)
          val timeTaken = System.currentTimeMillis - startTime
          logInfo(s"Sorting $recordsPerPartition records took $timeTaken ms")
          println(s"Sorting $recordsPerPartition records took $timeTaken ms")
          scala.Console.flush()
        }
        */

        readFileAndSort(outputFile, sortBuffer)

        Iterator((recordsPerPartition, sortBuffer.pointers))
      }
    }
  }

  // A radix sort with base 65536, i.e. per 2 bytes.
  private def radixSort(sortBuf: SortBuffer, start: Int, end: Int) {
    val KEY_LEN = 5

    var data = sortBuf.pointers
    var data2 = sortBuf.pointers2

    // Number of times each byte appears in each position in the key
    val counts = Array.fill(KEY_LEN, 65536)(0)

    var i = 0
    var j = 0

    // Do a first pass to compute how many times each byte occurs in each position
    i = start
    while (i < end) {
      j = 0
      while (j < KEY_LEN) {
        val b = UNSAFE.getShort(data(i) + j + j) & 0xFFFF
        counts(j)(b) += 1
        j += 1
      }
      i += 1
    }

    // Now move the longs around to sort them by each position
    j = KEY_LEN - 1
    // Position at which we can insert the next record with a given value of byte j
    val insertPos = Array.fill(65536)(0)
    while (j >= 0) {
      insertPos(0) = start
      for (b <- 1 until 65536) {
        insertPos(b) = insertPos(b - 1) + counts(j)(b - 1)
      }
      assert(insertPos(65535) + counts(j)(65535) == end)
      var pos = start
      while (pos < end) {
        val b = UNSAFE.getShort(data(pos) + j + j) & 0xFFFF
        data2(insertPos(b)) = data(pos)
        pos += 1
        insertPos(b) += 1
      }
      val tmp = data
      data = data2
      data2 = tmp
      j -= 1
    }

    /*
    // Validate that the data is sorted
    i = start
    while (i < end - 1) {
      assert(ord.compare(data(i), data(i + 1)) <= 0)
      i += 1
    }
    */

    if (sortBuf.pointers != data) {
      System.arraycopy(data, start, sortBuf.pointers, start, end - start)
    }
  }

  // A radix sort with base 256, i.e. per character. Seems slower than 16.
  private def radixSort256(sortBuf: SortBuffer, start: Int, end: Int) {
    val KEY_LEN = 10

    var data = sortBuf.pointers
    var data2 = sortBuf.pointers2

    // Number of times each byte appears in each position in the key
    val counts = Array.fill(KEY_LEN, 256)(0)

    var i = 0
    var j = 0

    // Do a first pass to compute how many times each byte occurs in each position
    i = start
    while (i < end) {
      j = 0
      while (j < KEY_LEN) {
        val b = UNSAFE.getByte(data(i) + j) & 0xFF
        counts(j)(b) += 1
        j += 1
      }
      i += 1
    }

    // Now move the longs around to sort them by each position
    j = KEY_LEN - 1
    while (j >= 0) {
      // Position at which we can insert the next record with a given value of byte j
      val insertPos = Array.fill(256)(0)
      insertPos(0) = start
      for (b <- 1 until 256) {
        insertPos(b) = insertPos(b - 1) + counts(j)(b - 1)
      }
      assert(insertPos(255) + counts(j)(255) == end)
      var pos = start
      while (pos < end) {
        val b = UNSAFE.getByte(data(pos) + j) & 0xFF
        data2(insertPos(b)) = data(pos)
        pos += 1
        insertPos(b) += 1
      }
      val tmp = data
      data = data2
      data2 = tmp
      j -= 1
    }

    /*
    // Validate that the data is sorted
    i = start
    while (i < end - 1) {
      assert(ord.compare(data(i), data(i + 1)) <= 0)
      i += 1
    }
    */

    if (sortBuf.pointers != data) {
      // Shouldn't happen since we have an even key length, but nonetheless
      System.arraycopy(data, start, sortBuf.pointers, start, end - start)
    }
  }

  private[spark]
  final class LongArraySorter extends SortDataFormat[Long, Array[Long]] {
    /** Return the sort key for the element at the given index. */
    override protected def getKey(data: Array[Long], pos: Int): Long = data(pos)

    /** Swap two elements. */
    override protected def swap(data: Array[Long], pos0: Int, pos1: Int) {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    /** Copy a single element from src(srcPos) to dst(dstPos). */
    override protected def copyElement(src: Array[Long], srcPos: Int,
                                       dst: Array[Long], dstPos: Int) {
      dst(dstPos) = src(srcPos)
    }

    /**
     * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
     * Overlapping ranges are allowed.
     */
    override protected def copyRange(src: Array[Long], srcPos: Int,
                                     dst: Array[Long], dstPos: Int, length: Int) {
      System.arraycopy(src, srcPos, dst, dstPos, length)
    }

    /**
     * Allocates a Buffer that can hold up to 'length' elements.
     * All elements of the buffer should be considered invalid until data is explicitly copied in.
     */
    override protected def allocate(length: Int): Array[Long] = new Array[Long](length)
  }

}
