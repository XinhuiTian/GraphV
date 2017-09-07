package org.apache.spark.graphloca.impl

import scala.reflect.ClassTag

import org.apache.spark.HashPartitioner

import org.apache.spark.graphloca._
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.util.collection.BitSet

/**
 * Created by XinhuiTian on 17/5/8.
 */
trait IngressEdgePartitioner extends Serializable {
    def numPartitions: Int
    def fromEdges[T: ClassTag](rdd: SimpleEdgeRDD[T]): SimpleEdgeRDD[T]
}

case class IngressEdgePartition2D(partitions: Int = -1)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[T: ClassTag](rdd: SimpleEdgeRDD[T]): SimpleEdgeRDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.numPartitions
    val newPartitionRDD = rdd.partitionRDD.mapPartitions { iter =>
      val (pid, edgePart) = iter.next()
      edgePart.edges.map { e =>
        val part =
          PartitionStrategy.EdgePartition2D.getPartition (e.srcId, e.dstId, partNum)
        (part, e)
      }.iterator
    }.forcePartitionBy(new HashPartitioner(partNum)).map{ _._2 }
      .mapPartitionsWithIndex { (pid, iter) =>
        Iterator((pid, new SimpleEdgePartition(iter.toArray.asInstanceOf[Array[Edge[T]]])))
      }
    new SimpleEdgeRDD(newPartitionRDD)
  }
}
/**
 * Assigns edges to partitions using only the source vertex ID, colocating edges with the same
 * source.
 */
case class IngressEdgePartition1D(partitions: Int = -1)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[T: ClassTag](rdd: SimpleEdgeRDD[T]): SimpleEdgeRDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.numPartitions
    val newPartitionRDD = rdd.partitionRDD.mapPartitions { iter =>
      val (pid, edgePart) = iter.next ()
      edgePart.edges.map{e =>
        val part =
          PartitionStrategy.EdgePartition1D.getPartition (e.srcId, e.dstId, partNum)
        (part, e)
      }.iterator
    }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
      .mapPartitionsWithIndex { (pid, iter) =>
        Iterator((pid, new SimpleEdgePartition(iter.toArray.asInstanceOf[Array[Edge[T]]])))
      }
    new SimpleEdgeRDD(newPartitionRDD)
  }
}
/**
 * Assigns edges to partitions by hashing the source and destination vertex IDs, resulting in a
 * random vertex cut that colocates all same-direction edges between two vertices.
 */
case class IngressRandomVertexCut(partitions: Int = -1)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[T: ClassTag](rdd: SimpleEdgeRDD[T]): SimpleEdgeRDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.numPartitions
    val newPartitionRDD = rdd.partitionRDD.mapPartitions { iter =>
      val (pid, edgePart) = iter.next ()
      edgePart.edges.map { e =>
      val part =
        PartitionStrategy.RandomVertexCut.getPartition(e.srcId, e.dstId, partNum)
      (part, e) }.iterator
      }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
      .mapPartitionsWithIndex { (pid, iter) =>
        Iterator((pid, new SimpleEdgePartition(iter.toArray.asInstanceOf[Array[Edge[T]]])))
      }
    new SimpleEdgeRDD(newPartitionRDD)
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

  def fromEdges[T: ClassTag](rdd: SimpleEdgeRDD[T]): SimpleEdgeRDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.numPartitions
    val newPartitionRDD = rdd.partitionRDD.mapPartitions { iter =>
      val (pid, edgePart) = iter.next ()
      edgePart.edges.map { e =>
        val part = PartitionStrategy.CanonicalRandomVertexCut.
          getPartition (e.srcId, e.dstId, partNum)
        (part, e)
      }.iterator
    }.partitionBy (new HashPartitioner (partNum)).map(_._2)
     .mapPartitionsWithIndex { (pid, iter) =>
      Iterator((pid, new SimpleEdgePartition(iter.toArray.asInstanceOf[Array[Edge[T]]])))
    }
    new SimpleEdgeRDD(newPartitionRDD)
  }
}

case class IngressObliviousVertexCut(
    partitions: Int = -1,
    useHash: Boolean = false,
    useRecent: Boolean = false)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[T: ClassTag](rdd: SimpleEdgeRDD[T]): SimpleEdgeRDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.numPartitions
    // val tmp =
    val newPartitionRDD = rdd.partitionRDD.mapPartitions { iter =>
      //  def test(arr: Array[Int], num: Int) = {
      //       arr(num % arr.size) += 1
      // }
      val (pid, edgePart) = iter.next()
      val srcFavor = new BitSet(partNum)
      val dstFavor = new BitSet(partNum)
      val partNumEdges = new Array[Int](partNum)
      srcFavor.setUntil(partNum)
      dstFavor.setUntil(partNum)
      // assert(iter.size > 0)
      //    val messages = new Array[(Int, T)](iter.size)
      //    var i = 0
      //    while(i < iter.size) {
      edgePart.edges.map { e =>
        // test(partNumEdges, e.dstId.toInt)
        // partNumEdges(i % partNumEdges.size) = 4
        val part =
          getPartition(e.srcId, e.dstId, partNum, srcFavor, dstFavor,
            partNumEdges, useHash, useRecent)
        (part, e)
        //  messages(i) = (part, e)
        // i += 1
      }.toIterator
      // partNumEdges.iterator
      // .collect
    }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
      .mapPartitionsWithIndex { (pid, iter) =>
        Iterator((pid, new SimpleEdgePartition(iter.toArray.asInstanceOf[Array[Edge[T]]])))
      }
    new SimpleEdgeRDD(newPartitionRDD)
    //        tmp.iterator.foreach(i=>print("."+i))
    //       rdd
  }
  private def getPartition(srcId: VertexId, dstId: VertexId, partNum: Int,
      srcFavor: BitSet, dstFavor: BitSet, partNumEdges: Array[Int],
      useHash: Boolean, useRecent: Boolean): Int = {
    val epsilon = 1.0
    val minEdges = partNumEdges.min
    val maxEdges = partNumEdges.max
    val partScores =
      (0 until partNum).map { i =>
        val sf = srcFavor.get(i) || (useHash && (srcId % partNum == i))
        val tf = dstFavor.get(i) || (useHash && (dstId % partNum == i))
        val f = (sf, tf) match {
          case (true, true) => 2.0
          case (false, false) => 0.0
          case _ => 1.0
        }
        f + (maxEdges - partNumEdges(i)).toDouble / (epsilon + maxEdges - minEdges)
      }
    val maxScore = partScores.max
    // print("-" + maxScore)
    val topParts = partScores.zipWithIndex.filter{ p =>
      math.abs(p._1 - maxScore) < 1e-5 }.map{ _._2}
    // Hash the edge to one of the best procs.
    val edgePair = if (srcId < dstId) (srcId, dstId) else (dstId, srcId)
    val bestPart = topParts(math.abs(edgePair.hashCode) % topParts.size)
    if (useRecent) {
      srcFavor.clear
      dstFavor.clear
    }
    srcFavor.set(bestPart)
    dstFavor.set(bestPart)
    partNumEdges(bestPart) = partNumEdges(bestPart) + 1
    bestPart
  }
}
