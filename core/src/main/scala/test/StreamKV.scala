package test

import com.pingcap.tikv.key.Key
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tispark.write.SerializableKey
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.math.Ordering.ordered
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.pingcap.tikv.{BytePairWrapper, ImportSSTManager, TiConfiguration, TiSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import test.StreamKV.ingestNumber

import java.util
import scala.collection.JavaConverters._
import java.util.UUID

object StreamKV {
  var prefix: String = "test01"
  var keyCount: Int = 10000 //1000000
  var valueLength: Int = 64
  var ingestNumber: Int = 1

  def main(args: Array[String]): Unit = {
    if(args.length > 0) {
      prefix = args(0)
    }

    if(args.length > 1) {
      keyCount = args(1).toInt
    }

    if(args.length > 2) {
      valueLength = args(2).toInt
    }

    if(args.length > 3) {
      ingestNumber = args(3).toInt
    }

    println("**************")
    println(s"prefix=$prefix")
    println(s"keyCount=$keyCount")
    println(s"valueLength=$valueLength")
    println("**************")
    val start = System.currentTimeMillis()

    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val value = genValue()

    val rdd = spark.sparkContext.parallelize(1 to keyCount).map { i =>
      (s"${prefix}_${genKey(i)}".toArray.map(_.toByte), value.toArray.map(_.toByte))
    }

    val conf = TiConfiguration.createDefault("172.16.5.81:4588")
    new StreamKV().writeAndIngest(conf, rdd)

    val end = System.currentTimeMillis()
    println("total seconds: " + (end - start) / 1000)
  }

  private def genKey(i: Int): String = {
    var s = ""

    if(i < 10) {
      s = s + "0000000"
    } else if(i < 100) {
      s = s + "000000"
    } else if(i < 1000) {
      s = s + "00000"
    } else if(i < 10000) {
      s = s + "0000"
    } else if(i < 100000) {
      s = s + "000"
    } else if(i < 1000000) {
      s = s + "00"
    }else if(i < 10000000) {
      s = s + "0"
    }
    s + i
  }

  private def genValue(): String = {
    var s = ""
    (1 to valueLength).foreach { i =>
      s = s + "A"
    }
    s
  }
}

class StreamKV extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  @transient private val scheduledExecutorService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build);

  @transient private var tiSession: TiSession = _
  private var tiConf: TiConfiguration = _

  private var partitioner: TiReginSplitPartitionerV2 = _

  // region split
  val optionsSplitRegionBackoffMS = 120000
  val optionsScatterRegionBackoffMS = 30000
  val optionsScatterWaitMS = 300000

  // sample
  private val optionsRegionSplitNum = 0
  private val optionsMinRegionSplitNum = 1
  private val optionsRegionSplitKeys = 960000
  private val optionsMaxRegionSplitNum = 64
  private val optionsSampleSplitFrac = 1000
  private val optionsRegionSplitUsingSize = true
  private val optionsBytesPerRegion = 100663296

  private def writeAndIngest(conf: TiConfiguration, rdd: RDD[(Array[Byte], Array[Byte])]): Unit = {
    tiConf = conf;
    tiSession = TiSession.getInstance(conf)

    // sort
    val rdd2 = rdd.map { pair =>
      (new SerializableKey(pair._1), pair._2)
    }.sortByKey().persist(StorageLevel.DISK_ONLY)

    // calculate regionSplitPoints
    val orderedSplitPoints = getRegionSplitPoints(rdd2)

    // switch to normal mode
    switchToNormalMode()

    // call region split and scatter
    tiSession.splitRegionAndScatter(
      orderedSplitPoints.map(_.bytes).asJava,
      optionsSplitRegionBackoffMS,
      optionsScatterRegionBackoffMS,
      optionsScatterWaitMS)

    // switch to import mode
    scheduledExecutorService.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        switchToImportMode()
      }
    }, 0, 2, TimeUnit.SECONDS)

    // refetch region info
    val minKey = rdd2.map(p => p._1).min().getRowKey
    val maxKey = rdd2.map(p => p._1).max().getRowKey
    val orderedRegions = getRegionInfo(minKey, maxKey)
    logger.info("orderedRegions size = " + orderedRegions.size)

    // repartition rdd according region
    partitioner = new TiReginSplitPartitionerV2(orderedRegions)
    val rdd3 = rdd2.partitionBy(partitioner).persist(StorageLevel.DISK_ONLY)
    logger.info("rdd3.getNumPartitions = " + rdd3.getNumPartitions)

    // call writeAndIngest for each partition
    (1 to ingestNumber).foreach { i =>
        logger.info(s"writeAndIngest round: $i")
        rdd3.foreachPartition { itor =>
          writeAndIngest(itor.map(pair => (pair._1.bytes, pair._2)), partitioner)
        }
      }
    }

  private def writeAndIngest(iterator: Iterator[(Array[Byte], Array[Byte])], partitioner: TiReginSplitPartitionerV2): Unit = {
    val (itor1, tiro2) = iterator.duplicate

    var minKey: Key = Key.MAX
    var maxKey: Key = Key.MIN
    var region: TiRegion = null
    var key: Key = null
    val dataSize = itor1.map { itor =>
      key = Key.toRawKey(itor._1)

      if(region == null) {
        region = partitioner.getRegion(key)
      }

      if(key.compareTo(minKey) < 0) {
        minKey = key
      }
      if(key.compareTo(maxKey) > 0) {
        maxKey = key
      }
    }.size

    if(dataSize > 0) {
      if (region == null) {
        logger.warn("region == null, skip ingest this partition")
      } else {
        val uuid = genUUID()

        logger.warn(s"start to ingest this partition ${util.Arrays.toString(uuid)}")
        val pairsIterator = tiro2.map { keyValue =>
          new BytePairWrapper(keyValue._1, keyValue._2)
        }.asJava

        val importSSTManager = new ImportSSTManager(uuid, TiSession.getInstance(tiConf), minKey, maxKey, region)
        importSSTManager.write(pairsIterator)
        logger.warn(s"finish to ingest this partition ${util.Arrays.toString(uuid)}")
      }
    }
  }

  private def genUUID(): Array[Byte] = {
   val uuid = UUID.randomUUID()

    val out = new Array[Byte](16)
    val msb = uuid.getMostSignificantBits
    val lsb = uuid.getLeastSignificantBits
    for (i <- 0 until 8) {
      out(i) = ((msb >> ((7 - i) * 8)) & 0xff).toByte
    }
    for (i <- 8 until 16) {
      out(i) = ((lsb >> ((15 - i) * 8)) & 0xff).toByte
    }

    out
  }

  private def switchToImportMode(): Unit = {
    tiSession.getImportSSTClient.switchTiKVToImportMode()
  }

  private def switchToNormalMode(): Unit = {
    tiSession.getImportSSTClient.switchTiKVToNormalMode()
  }

  private def getRegionInfo(min: Key, max: Key): List[TiRegion] = {
    val regions = new mutable.ArrayBuffer[TiRegion]()

    tiSession.getRegionManager.invalidateAll()

    var current = min

    while (current.compareTo(max) <= 0) {
      val region = tiSession.getRegionManager.getRegionByKey(current.toByteString)
      regions.append(region)
      current = region.getRowEndKey
    }

    regions.toList
  }

  private def getRegionSplitPoints(rdd: RDD[(SerializableKey, Array[Byte])]): List[SerializableKey] = {
    val count = rdd.count()

    val regionSplitPointNum = if (optionsRegionSplitNum > 0) {
      optionsRegionSplitNum
    } else {
      Math.min(
        Math.max(
          optionsMinRegionSplitNum,
          Math.ceil(count.toDouble / optionsRegionSplitKeys).toInt),
        optionsMaxRegionSplitNum)
    }
    logger.info(s"regionSplitPointNum=$regionSplitPointNum")

    val sampleSize = (regionSplitPointNum + 1) * optionsSampleSplitFrac
    logger.info(s"sampleSize=$sampleSize")

    val sampleData = if(sampleSize < count) {
      rdd.sample(false, sampleSize.toDouble / count).collect()
    } else {
      rdd.collect()
    }
    logger.info(s"sampleData size=${sampleData.length}")

    val splitPointNumUsingSize = if (optionsRegionSplitUsingSize) {
      val avgSize = getAverageSizeInBytes(sampleData)
      logger.info(s"avgSize=$avgSize Bytes")
      if (avgSize <= optionsBytesPerRegion / optionsRegionSplitKeys) {
        regionSplitPointNum
      } else {
        Math.min(
          Math.floor((count.toDouble / optionsBytesPerRegion) * avgSize).toInt,
          sampleData.length / 10)
      }
    } else {
      regionSplitPointNum
    }
    logger.info(s"splitPointNumUsingSize=$splitPointNumUsingSize")

    val finalRegionSplitPointNum = Math.min(
      Math.max(optionsMinRegionSplitNum, splitPointNumUsingSize),
      optionsMaxRegionSplitNum)
    logger.info(s"finalRegionSplitPointNum=$finalRegionSplitPointNum")

    val sortedSampleData = sampleData
      .map(_._1)
      .sorted(new Ordering[SerializableKey] {
        override def compare(x: SerializableKey, y: SerializableKey): Int = {
          x.compareTo(y)
        }
      })
    val orderedSplitPoints = new Array[SerializableKey](finalRegionSplitPointNum)
    val step = Math.floor(sortedSampleData.length.toDouble / (finalRegionSplitPointNum + 1)).toInt
    for (i <- 0 until finalRegionSplitPointNum) {
      orderedSplitPoints(i) = sortedSampleData((i + 1) * step)
    }

    logger.info(s"orderedSplitPoints size=${orderedSplitPoints.length}")
    orderedSplitPoints.toList
  }

  private def getAverageSizeInBytes(keyValues: Array[(SerializableKey, Array[Byte])]): Int = {
    var avg: Double = 0
    var t: Int = 1
    keyValues.foreach { keyValue =>
      val keySize: Double = keyValue._1.bytes.length + keyValue._2.length
      avg = avg + (keySize - avg) / t
      t = t + 1
    }
    Math.ceil(avg).toInt
  }
}