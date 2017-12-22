
package org.apache.spark.graphx

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

import org.apache.spark.HashPartitioner

import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.BitSet

/**
 * Created by XinhuiTian on 17/6/26.
 */
trait IngressEdgePartitioner
  extends Serializable {
  def numPartitions: Int
  def fromEdges[T <: Edge[_] : ClassTag](rdd: RDD[T]): RDD[T]
}

object AggregateDirection extends Enumeration {
  type AggregateDirection = Value
  val InOnly, OutOnly = Value
}

case class IngressHybridVertexCut(
    partitions: Int = -1,
    aggDir: AggregateDirection.Value = AggregateDirection.OutOnly,
    threshold: Int = 100)
  extends IngressEdgePartitioner {
  require(threshold >= 0, s"Number of threshold ($threshold) cannot be negative.")
  def numPartitions: Int = partitions
  @implicitNotFound(msg = "No ClassTag available for ${T}")
  def fromEdges[T <: Edge[_] : ClassTag](rdd: RDD[T]): RDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    val mixingPrime = 1125899906842597L % partNum
    aggDir match {
      case AggregateDirection.InOnly =>
        // val lookUpTable = rdd.map { e => (e.dstId, (e.srcId, e.attr))}
        val ecut_edges = rdd.map{ e => ((e.dstId * mixingPrime) % partNum, e) }.
          partitionBy(new HashPartitioner(partNum))
        ecut_edges.mapPartitions { iter =>
          val messages = iter.toArray
          val indegrees = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
          messages.foreach{ message =>
            indegrees.changeValue(message._2.dstId, 1, _ + 1)
          }
          messages.map{ message =>
            if (indegrees(message._2.dstId) <= threshold) {
              message
            } else {
              ((message._2.srcId * mixingPrime) % partNum, message._2)
            }
          }.toIterator
        }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }

      case AggregateDirection.OutOnly =>
        val ecut_edges = rdd.map{ e => ((e.srcId * mixingPrime) % partNum, e) }.
          partitionBy(new HashPartitioner(partNum))
        ecut_edges.mapPartitions { iter =>
          val messages = iter.toArray
          val outdegrees = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
          messages.foreach{ message =>
            outdegrees.changeValue(message._2.srcId, 1, _ + 1)
          }
          messages.map{ message =>
            if (outdegrees(message._2.srcId) <= threshold) {
              message
            } else {
              ((message._2.dstId * mixingPrime) % partNum, message._2)
            }
          }.toIterator
        }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
      }
    }
  }

// only consider OutOnly
case class IngressEdgePartitionBiCut(
    partitions: Int = -1,
    aggDir: AggregateDirection.Value = AggregateDirection.OutOnly)
  extends IngressEdgePartitioner {

  def numPartitions: Int = partitions

  def fromEdges[T <: Edge[_] : ClassTag](rdd: RDD[T]): RDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    val mixingPrime = 1125899906842597L % partNum
    aggDir match {

      /*
      case AggregateDirection.InOnly => // bidstcut
        // val lookUpTable = rdd.map { e => (e.dstId, (e.srcId, e.attr))}
        // get all the srcIds for each dstId
        // 1. edges are grouped by dstId
        // 2. for each partition, compute the partitions for each srcId
        // gather the result with dstId as the key: (dstId, Array_partition_count)
        val groupE = rdd.map{e => (e.dstId, e.srcId)}.groupByKey
        val countMap = groupE.map{e =>
          val arr = new Array[Long](numPartitions)
          // get which partition each srcId belongs to
          e._2.map{v => arr ((v % numPartitions).toInt) += 1}
          // get the array recording hwo many srcIds in each partition
          (e._1, arr)
        }

        // define a array out of the rdds?
        val procNumEdges = new Array[Long](numPartitions)
        val mht = countMap.map { e =>
          val k = e._1 // dstId
          val v = e._2 // partition array
        // original partition ID
          var bestProc: PartitionID = (k % numPartitions).toInt
        // srcId num in each partition - sqrt(total_edges_in_this_partition)
          var bestScore: Double = v.apply (bestProc) - math.sqrt (1.0 * procNumEdges (bestProc))
          for (i <- 0 to numPartitions - 1) {
            // for each partition, compute the score for this edge
            val score: Double = v.apply (i) - math.sqrt (1.0 * procNumEdges (i))
            if (score > bestScore) {
              bestProc = i
              bestScore = score
            }
          }
          // computing edge number for each partition

          for (i <- 0 to numPartitions - 1) {
            procNumEdges (bestProc) += v.apply (i)
          }
          // (dstId, bestProc)
          (k, bestProc)
        }

        val combineRDD = (rdd.map{e => (e.dstId, e)}).join (mht)
        combineRDD.map{e =>
          (e._2._2, e._2._1)
        }.partitionBy (new HashPartitioner (numPartitions)).map{
          _._2
        }
        */

      case AggregateDirection.OutOnly =>
        // val lookUpTable = rdd.map { e => (e.dstId, (e.srcId, e.attr))}
        val hashPartitioner = new HashPartitioner(partNum)
        val groupE = rdd.map{e => (e.srcId, e.dstId)}.groupByKey
        val countMap = groupE.map{e =>
          val arr = new Array[Long](numPartitions)
          e._2.map{v => arr (hashPartitioner.getPartition(v)) += 1}
          (e._1, arr)
        }

        var procNumEdges = new Array[Long](numPartitions)
        val mht = countMap.map { e =>

          /**
           * for each edge
           * 1. the initial bestProc is the hash-based partition num
           * 2. the initial bestScore is the neighbor number in each partition
           * 3. find assigned to which partition, this edge can get the best score
           * 4. add the edge numbers to the procNumEdges of the best proc
           */
          val k = e._1
          val v = e._2
          var bestProc: PartitionID = hashPartitioner.getPartition(k)
          var bestScore: Double = v.apply (bestProc) - math.sqrt (1.0 * procNumEdges (bestProc))
          for (i <- 0 to numPartitions - 1) {
            val score: Double = v.apply (i) - math.sqrt (1.0 * procNumEdges (i))
            if (score > bestScore) {
              bestProc = i
              bestScore = score
            }
          }
          for (i <- 0 to numPartitions - 1) {
            procNumEdges (bestProc) += v.apply (i)
          }
          (k, bestProc)
        }

        val combineRDD = (rdd.map{e => (e.srcId, e)}).join (mht)
        combineRDD.map { e =>
          (e._2._2, e._2._1)
        }.partitionBy (new HashPartitioner (numPartitions)).map {
          _._2
        }
    }
  }
}
// reuse the PartitionStrategy
case class IngressEdgePartition2D(partitions: Int = -1)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[T <: Edge[_] : ClassTag](rdd: RDD[T]): RDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    rdd.map{ e =>
      val part =
        PartitionStrategy.EdgePartition2D.getPartition(e.srcId, e.dstId, partNum)
      (part, e) }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
  }
}
/**
 * Assigns edges to partitions using only the source vertex ID, colocating edges with the same
 * source.
 */
case class IngressEdgePartition1D(partitions: Int = -1)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[T <: Edge[_] : ClassTag](rdd: RDD[T]): RDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    rdd.map{ e =>
      val part =
        PartitionStrategy.EdgePartition1D.getPartition(e.srcId, e.dstId, partNum)
      (part, e) }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
  }
}
/**
 * Assigns edges to partitions by hashing the source and destination vertex IDs, resulting in a
 * random vertex cut that colocates all same-direction edges between two vertices.
 */
case class IngressRandomVertexCut(partitions: Int = -1)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[T <: Edge[_] : ClassTag](rdd: RDD[T]): RDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    rdd.map{ e =>
      val part =
        PartitionStrategy.RandomVertexCut.getPartition(e.srcId, e.dstId, partNum)
      (part, e) }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
  }
}
/**
 * Assigns edges to partitions by hashing the source and destination vertex IDs in a canonical
 * direction, resulting in a random vertex cut that colocates all edges between two vertices,
 * regardless of direction.
 */
case class IngressCanonicalRandomVertexCut(partitions: Int = -1)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[T <: Edge[_] : ClassTag](rdd: RDD[T]): RDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    rdd.map{ e =>
      val part = PartitionStrategy.CanonicalRandomVertexCut.
        getPartition(e.srcId, e.dstId, partNum)
      (part, e) }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
  }
}

case class IngressObliviousVertexCut(
    partitions: Int = -1,
    useHash: Boolean = false,
    useRecent: Boolean = false)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions

  def fromEdges[T <: Edge[_] : ClassTag](rdd: RDD[T]): RDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    // val tmp =
    rdd.mapPartitions{iter =>
      //  def test(arr: Array[Int], num: Int) = {
      //       arr(num % arr.size) += 1
      // }
      val srcFavor = new BitSet (partNum)
      val dstFavor = new BitSet (partNum)
      // store the number of edges for each partition
      // only in one edge partition
      val partNumEdges = new Array[Int](partNum)
      srcFavor.setUntil (partNum)
      dstFavor.setUntil (partNum)
      // assert(iter.size > 0)
      //    val messages = new Array[(Int, T)](iter.size)
      //    var i = 0
      //    while(i < iter.size) {
      iter.toArray.map{e =>
        // test(partNumEdges, e.dstId.toInt)
        // partNumEdges(i % partNumEdges.size) = 4
        val part =
          getPartition (e.srcId, e.dstId, partNum, srcFavor, dstFavor,
            partNumEdges, useHash, useRecent)
        (part, e)
        //  messages(i) = (part, e)
        // i += 1
      }.toIterator
      // partNumEdges.iterator
      // .collect
    }.partitionBy (new HashPartitioner (partNum)).map{
      _._2
    }
    //        tmp.iterator.foreach(i=>print("."+i))
    //       rdd
  }

  private def getPartition(srcId: VertexId, dstId: VertexId, partNum: Int,
      srcFavor: BitSet, dstFavor: BitSet, partNumEdges: Array[Int],
      useHash: Boolean, useRecent: Boolean): Int = {
    val epsilon = 1.0
    val minEdges = partNumEdges.min
    val maxEdges = partNumEdges.max
    val partScores = // compute score for each edge partition
      (0 until partNum).map{i =>
        val sf = srcFavor.get (i) || (useHash && (srcId % partNum == i))
        val tf = dstFavor.get (i) || (useHash && (dstId % partNum == i))
        val f = (sf, tf) match {
          case (true, true) => 2.0 // have both src and dst roles in this partition
          case (false, false) => 0.0 // not on this partition,
          case _ => 1.0
        }
        // related to whether have edges (have, larger),
        // and current edge num of this part (less, larger)
        f + (maxEdges - partNumEdges (i)).toDouble / (epsilon + maxEdges - minEdges)
      }
    val maxScore = partScores.max
    // print("-" + maxScore)
    val topParts = partScores.zipWithIndex.filter{p =>
      math.abs (p._1 - maxScore) < 1e-5
    }.map{
      _._2
    }
    // Hash the edge to one of the best procs.
    val edgePair = if (srcId < dstId) (srcId, dstId) else (dstId, srcId)
    val bestPart = topParts (math.abs (edgePair.hashCode) % topParts.size)
    if (useRecent) {
      srcFavor.clear
      dstFavor.clear
    }
    srcFavor.set (bestPart)
    dstFavor.set (bestPart)
    partNumEdges (bestPart) = partNumEdges (bestPart) + 1
    bestPart
  }
}

object IngressEdgePartitioner {
  def fromString(s: String,
      parts: Int,
      threshold: Int = 100,
      aggDir: AggregateDirection.Value = AggregateDirection.OutOnly):
  IngressEdgePartitioner = s match {
    case "RandomVertexCut" => new IngressRandomVertexCut(parts)
    case "EdgePartition1D" => new IngressEdgePartition1D(parts)
    case "EdgePartition2D" => new IngressEdgePartition2D(parts)
    case "HybridVertexCut" => new IngressHybridVertexCut(parts, aggDir, threshold)
    case "ObliviousVertexCut" => new IngressObliviousVertexCut(parts)
    case "CanonicalRandomVertexCut" => new IngressCanonicalRandomVertexCut(parts)
    case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + s)
  }
}




