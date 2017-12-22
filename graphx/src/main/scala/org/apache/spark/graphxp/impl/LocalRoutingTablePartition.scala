
package org.apache.spark.graphxp.impl

import org.apache.spark.graphxp._
import org.apache.spark.graphxp.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

/**
 * Created by XinhuiTian on 17/6/20.
 * Another routingTable implementation maintaining a globalVid2local index
 */

object LocalRoutingTablePartition {
  type LocalRoutingTableMessage = (VertexId, (PartitionID, Int)) // the localID in each edge part

  private def toMessage(vid: VertexId, pid: PartitionID, position: Byte, localId: Int)
  : LocalRoutingTableMessage = {
    val positionUpper2 = position << 30
    val pidLower30 = pid & 0x3FFFFFFF
    (vid, (positionUpper2 | pidLower30, localId))
  }

  private def vidFromMessage(msg: LocalRoutingTableMessage): VertexId = msg._1
  private def pidFromMessage(msg: LocalRoutingTableMessage): PartitionID = msg._2._1 & 0x3FFFFFFF
  private def positionFromMessage(msg: LocalRoutingTableMessage): Byte = (msg._2._1 >> 30).toByte
  private def localIdFromMessage(msg: LocalRoutingTableMessage): Int = msg._2._2

  val empty: LocalRoutingTablePartition = new LocalRoutingTablePartition(Array.empty)

  /** Generate a `RoutingTableMessage` for each vertex referenced in `edgePartition`. */
  def edgePartitionToMsgs(pid: PartitionID, edgePartition: EdgePartition[_, _])
    : Iterator[LocalRoutingTableMessage] = {
    // Determine which positions each vertex id appears in using a map where the low 2 bits
    // represent src and dst
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, (Byte, Int)]
    edgePartition.localIterator.foreach { e =>
      map.changeValue(e.srcId, ((0x1.toByte, e.localSrcId)),
        (a: (Byte, Int)) => ((a._1 | 0x1).toByte, a._2))
      map.changeValue(e.dstId, ((0x2.toByte, e.localDstId)),
        (a: (Byte, Int)) => ((a._1 | 0x2).toByte, a._2))
    }
    map.iterator.map { vidAndPosition =>
      val vid = vidAndPosition._1
      val position = vidAndPosition._2._1
      val localVid = vidAndPosition._2._2
      toMessage(vid, pid, position, localVid)
    }
  }

  def fromMsgs(numEdgePartitions: Int, iter: Iterator[LocalRoutingTableMessage])
    : LocalRoutingTablePartition = {

    val pid2vid = Array.fill(numEdgePartitions)(new PrimitiveVector[(VertexId, Int)])
    val srcFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])
    val dstFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])

    for (msg <- iter) {
      val vid = vidFromMessage(msg)
      val pid = pidFromMessage(msg)
      val position = positionFromMessage(msg)
      val localVid = localIdFromMessage(msg)
      pid2vid(pid) += (vid, localVid)
      srcFlags(pid) += (position & 0x1) != 0
      dstFlags(pid) += (position & 0x2) != 0
    }

    new LocalRoutingTablePartition(pid2vid.zipWithIndex.map {
      case (vids, pid) => (vids.trim().array, toBitSet(srcFlags(pid)), toBitSet(dstFlags(pid)))
    })
  }

  /** Compact the given vector of Booleans into a BitSet. */
  private def toBitSet(flags: PrimitiveVector[Boolean]): BitSet = {
    val bitset = new BitSet(flags.size)
    var i = 0
    while (i < flags.size) {
      if (flags(i)) {
        bitset.set(i)
      }
      i += 1
    }
    bitset
  }
}

private[graphxp]
class LocalRoutingTablePartition(
    private val routingTable: Array[(Array[(VertexId, Int)], BitSet, BitSet)])
  extends Serializable {

  /** The maximum number of edge partitions this `RoutingTablePartition` is built to join with. */
  val numEdgePartitions: Int = routingTable.length

  /** Returns the number of vertices that will be sent to the specified edge partition. */
  def partitionSize(pid: PartitionID): Int = routingTable(pid)._1.length

  /** Returns an iterator over all vertex ids stored in this `RoutingTablePartition`. */
  def iterator: Iterator[VertexId] = routingTable.iterator.flatMap(_._1.iterator.map(_._1))

  /**
   * Runs `f` on each vertex id to be sent to the specified edge partition. Vertex ids can be
   * filtered by the position they have in the edge partition.
   */
  def foreachWithinEdgePartition
  (pid: PartitionID, includeSrc: Boolean, includeDst: Boolean)
    (f: ((VertexId, Int)) => Unit) {
    val (vidsCandidate, srcVids, dstVids) = routingTable(pid)
    val size = vidsCandidate.length
    if (includeSrc && includeDst) {
      // Avoid checks for performance
      vidsCandidate.iterator.foreach(v => f(v))
    } else if (!includeSrc && !includeDst) {
      // Do nothing
    } else {
      val relevantVids = if (includeSrc) srcVids else dstVids
      relevantVids.iterator.foreach { i => f(vidsCandidate(i)) }
    }
  }

  def toL2LRoutingTable(
      global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int])
   : L2LRoutingTablePartition = {
    val l2lRoutingTable = routingTable.map { array =>
      val l2lArray = array._1.map(v => (global2local(v._1), v._2))
      (l2lArray, array._2, array._3)
    }

    new L2LRoutingTablePartition(l2lRoutingTable)
  }

}

private[graphxp]
class L2LRoutingTablePartition(
    private val routingTable: Array[(Array[(Int, Int)], BitSet, BitSet)]) extends Serializable {

  /** The maximum number of edge partitions this `RoutingTablePartition` is built to join with. */
  val numEdgePartitions: Int = routingTable.length

  /** Returns the number of vertices that will be sent to the specified edge partition. */
  def partitionSize(pid: PartitionID): Int = routingTable(pid)._1.length

  /** Returns an iterator over all vertex ids stored in this `RoutingTablePartition`. */
  def iterator: Iterator[VertexId] = routingTable.iterator.flatMap(_._1.iterator.map(_._1))

  /**
   * Runs `f` on each vertex id to be sent to the specified edge partition. Vertex ids can be
   * filtered by the position they have in the edge partition.
   */
  def foreachWithinEdgePartition
  (pid: PartitionID, includeSrc: Boolean, includeDst: Boolean)
    (f: ((Int, Int)) => Unit) {
    val (vidsCandidate, srcVids, dstVids) = routingTable(pid)
    val size = vidsCandidate.length
    if (includeSrc && includeDst) {
      // Avoid checks for performance
      vidsCandidate.iterator.foreach(v => f(v))
    } else if (!includeSrc && !includeDst) {
      // Do nothing
    } else {
      val relevantVids = if (includeSrc) srcVids else dstVids
      relevantVids.iterator.foreach { i => f(vidsCandidate(i)) }
    }
  }

}
