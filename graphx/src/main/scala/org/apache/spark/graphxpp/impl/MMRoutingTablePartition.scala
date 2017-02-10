
package org.apache.spark.graphxpp.impl

import org.apache.spark.graphxpp._
import org.apache.spark.util.collection.PrimitiveVector

import scala.collection.mutable.ArrayBuffer

/**
 * Created by XinhuiTian on 16/9/28.
 * used for select master pid for each vertex
 */
/*
object MMRoutingTablePartition {

  type RoutingTableMessage = (VertexId, Iterable[PartitionID])

  def fromMsgs(numPartitions: Int, iter: Iterator[RoutingTableMessage])
    : Iterator[(Int, MMRoutingTablePartition)] = {
    val pid2masters = Array.fill(numPartitions)(new ArrayBuffer[RoutingTableMessage])
    val pid2mirrors = Array.fill(numPartitions)(new ArrayBuffer[(VertexId, PartitionID)])
    /* select one pid as the master pid */
    val masterPids = iter.map(msg => (msg._2.head, (msg._1, msg._2.tail)))
    //masterPids.foreach(println)
    /* for each master pid, add the master vids into the master array */
    for (msg <- masterPids) {
      val pid = msg._1
      val vidWithMirrors = msg._2
      val vid = vidWithMirrors._1
      pid2masters(pid) += vidWithMirrors
      /* for each mirror pid of one vid, add the master pid to the mirror array */
      for (mirrorPid <- msg._2._2) {
        pid2mirrors(mirrorPid) += ((vid, pid))
      }
    }

    //pid2masters.foreach(println)
    //pid2mirrors.foreach(println)

    val tables: ArrayBuffer[(Int, MMRoutingTablePartition)] =
      new ArrayBuffer[(PartitionID, MMRoutingTablePartition)]
    for (pid <- 0 until numPartitions) {
      tables += ((pid,
        new MMRoutingTablePartition(pid2masters(pid).toArray, pid2mirrors(pid).toArray)))
    }
    //tables.iterator.map(iter => iter._2.masterVertWithMirrors.toBuffer).foreach(println)
    //tables.iterator.map(iter => iter._2.mirrorIterator).foreach(println)

    tables.iterator
  }

  def createRoutingTable(numParts: Int, originalPart: MMRoutingTablePartition)
  : MMRoutingTablePartition = {
    val pid2vid = Array.fill(numParts)(new PrimitiveVector[VertexId])
    for (master <- originalPart.masterIterator) {
      master._2.foreach(pid => pid2vid(pid) += master._1)
    }
    new MMRoutingTablePartition(originalPart.masterVertWithMirrors,
        originalPart.mirrorTable, pid2vid.map(_.trim().toArray))

  }

  val empty: MMRoutingTablePartition = new MMRoutingTablePartition(Array.empty, Array.empty)
}

// TODO: too many memory usage!!
class MMRoutingTablePartition(
  val masterVertWithMirrors: Array[(VertexId, Iterable[PartitionID])],
  val mirrorTable: Array[(VertexId, PartitionID)],
  val routingTable: Array[Array[VertexId]] = Array.empty) {

  def masterIterator: Iterator[(VertexId, Iterable[PartitionID])]
  = new Iterator[(VertexId, Iterable[PartitionID])] {
    private[this] var pos = 0

    override def hasNext: Boolean = pos < masterVertWithMirrors.size

    override def next(): (VertexId, Iterable[PartitionID]) = {
      val nextMaster = masterVertWithMirrors(pos)
      pos += 1
      nextMaster
    }
  }

  def mirrorIterator: Iterator[(VertexId, PartitionID)]
  = new Iterator[(VertexId, PartitionID)] {
    private[this] var pos = 0

    override def hasNext: Boolean = pos < mirrorTable.size

    override def next(): (VertexId, PartitionID) = {
      val nextMaster = mirrorTable(pos)
      pos += 1
      nextMaster
    }
  }

  def iterator: Iterator[VertexId] = new Iterator[VertexId] {
    private[this] var pos = 0

    override def hasNext: Boolean = pos < masterVertWithMirrors.size

    override def next(): VertexId = {
      val nextVertexId = masterVertWithMirrors(pos)._1
      pos += 1
      nextVertexId
    }
  }

  def mergeWithOther(other: MMRoutingTablePartition): MMRoutingTablePartition = {
    val newMasterVertWithMirrors = masterVertWithMirrors ++ other.masterVertWithMirrors
    val newMirrorTable = mirrorTable ++ other.mirrorTable

    new MMRoutingTablePartition(newMasterVertWithMirrors, newMirrorTable)
  }

}
*/

class MMRoutingTablePartition(
  private val routingTable: Array[(Array[(VertexId, PartitionID)])]) extends Serializable {


}
