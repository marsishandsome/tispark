package test

import com.pingcap.tikv.key.Key
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tispark.write.SerializableKey
import org.apache.spark.Partitioner

class TiReginSplitPartitionerV2(orderedRegions: List[TiRegion])
  extends Partitioner {
  override def getPartition(key: Any): Int = {
    val serializableKey = key.asInstanceOf[SerializableKey]
    val rawKey = Key.toRawKey(serializableKey.bytes)

    if(orderedRegions.isEmpty) {
      0
    } else {
      val firstRegion = orderedRegions.head
      if(rawKey.compareTo(firstRegion.getRowStartKey) < 0) {
        0
      }  else {
        orderedRegions.indices.foreach { i =>
          val region = orderedRegions(i)
          if(rawKey.compareTo(region.getRowStartKey) >= 0 && rawKey.compareTo(region.getRowEndKey) < 0) {
            return i + 1
          }
        }
        orderedRegions.size + 1
      }
    }
  }

  def getRegion(key: Key): TiRegion = {
    orderedRegions.foreach { region =>
      if(key.compareTo(region.getRowStartKey) >= 0 && key.compareTo(region.getRowEndKey) < 0) {
        return region
      }
    }
    null
  }

  override def numPartitions: Int = {
    orderedRegions.size + 2
  }
}

