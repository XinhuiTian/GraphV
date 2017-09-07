package org.apache.spark.graphloca.impl

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.graphloca._
import org.apache.spark.graphloca.util.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

/**
 * Created by XinhuiTian on 17/5/6.
 */
class SimpleEdgePartition[ED: ClassTag](
    val masters: OpenHashSet[VertexId],
    val masterDegrees: Array[(Int, Int)], // first master size elems are in degrees
    val vertices: OpenHashSet[VertexId],
    val globalDegrees: Array[(Int, Int)], // first vertices size elems are in degrees
    val edges: Array[Edge[ED]]) {

  def this(edges: Array[Edge[ED]]) = this(null, null, null, null, edges)

  def size = edges.length

  var localInDegrees: Array[Int] = null

  var localOutDegrees: Array[Int] = null

  var totalInDegrees: Long = 0

  var totalOutDegrees: Long = 0

  // the new masters that should be added in this partition
  // and removed from its original partition
  // var newMasters: Array[(VertexId, PartitionID)] = null

  /*
  def masterDegreeIterator: Iterator[(VertexId, (Int, Int))] = {
    new Iterator[(VertexId, (Int, Int))] {
      var pos = masters.nextPos (0)

      def hasNext: Boolean = pos < vertices.size

      def next: (VertexId, (Int, Int)) = {
        val value = (masters.getValue (pos), (masterDegrees(pos)))
        pos = masters.nextPos (pos)
        value
      }
    }
  }
  */

  /*
  def localDegreeIterator: Iterator[(VertexId, (Int, Int))] = {
    if (localInDegrees == null)
      return Iterator.empty

    new Iterator[(VertexId, (Int, Int))] {
      var pos = vertices.nextPos (0)

      def hasNext: Boolean = pos < vertices.size

      def next: (VertexId, (Int, Int)) = {
        val value = (vertices.getValue (pos), (localInDegrees (pos), localOutDegrees (pos)))
        pos = vertices.nextPos (pos)
        value
      }
    }
  }

  def globalDegreeIterator: Iterator[(VertexId, (Int, Int))] =
    new Iterator[(VertexId, (Int, Int))] {
      var pos = vertices.nextPos(0)
      def hasNext: Boolean = pos < vertices.size
      def next: (VertexId, (Int, Int)) = {
        val value = (vertices.getValue(pos), (globalDegrees(pos)))
        pos = vertices.nextPos(pos)
        value
      }
    }
    */

  def getInCompleteVertices: Array[VertexId] = {
    /*
    val comVs = new ArrayBuffer[VertexId]
    var pos = vertices.nextPos(0)
    while (pos > 0) {
      if (globalDegrees(pos)._1 == localInDegrees(pos)) {
        comVs += vertices.getValue(pos)
        // println("In com get " + vertices.getValue(pos))
      }
      pos = vertices.nextPos(pos + 1)
    }
    println("comVs: " + comVs.length)
    comVs.toArray
    */
    globalDegreeIterator.filter { v =>
      val vid = v._1
      val inDegree = v._2._1
      val pos = vertices.getPos(vid)
      localInDegrees(pos) == inDegree
    }.map(_._1).toArray
  }

  def getOutCompleteVertices: Array[VertexId] = {
    val comVs = new ArrayBuffer[VertexId]
    var pos = vertices.nextPos(0)
    while (pos > 0) {
      if (globalDegrees(pos)._2 == localOutDegrees(pos)) {
        comVs += vertices.getValue(pos)
      }
      pos = vertices.nextPos(pos + 1)
    }
    comVs.toArray
  }

  def updateLocalDegrees = {
    this.localInDegrees = computeLocalInDegrees
    this.localOutDegrees = computeLocalOutDegrees
  }

  // compute the local degrees for local vertices
  private def computeLocalInDegrees: Array[Int] = computeLocalDegrees(true)

  private def computeLocalOutDegrees: Array[Int] = computeLocalDegrees(false)

  def computeLocalDegrees(isIn: Boolean): Array[Int] = {
    val newAttrs = new Array[Int](vertices.capacity)
    val newMask = new BitSet(vertices.capacity)

    var i = 0
    while (i < size) {
      var pos = 0
      if (isIn) {
        pos = vertices.getPos(edges(i).dstId)
      } else {
        pos = vertices.getPos(edges(i).srcId)
      }

      if (pos >= 0) {
        if (newMask.get(pos)) {
          newAttrs(pos) = newAttrs(pos) + 1
          // count += 1
        } else { // otherwise just store the new value
          newMask.set(pos)
          newAttrs(pos) = 1
        }
      }
      i += 1
    }
    newAttrs
  }
  // only used for degree compute
  def aggregateMsgs: Iterator[(VertexId, (Int, Int))] = {
    // println("vertices.size: " + vertices.size, vertices)
    val aggregates = new PrimitiveKeyOpenHashMap[VertexId, (Int, Int)]
    var i = 0
    while (i < size) {
      val srcId = edges(i).srcId
      val dstId = edges(i).dstId
      aggregates.changeValue(dstId, (1, 0), a => (a._1 + 1, a._2))
      aggregates.changeValue(srcId, (0, 1), a => (a._1, a._2 + 1))
      /*
      if (useSrc) {
        val srcMsg = sendMsg(allVertAttrs(vertices.getPos(srcId)))
        aggregates.changeValue(dstId, srcMsg, a => mergeMsg(srcMsg, a))
      }
      if (useDst) {
        val dstMsg = sendMsg(allVertAttrs(vertices.getPos(dstId)))
        aggregates.changeValue(srcId, dstMsg, a => mergeMsg(dstMsg, a))
      }
      */
      i += 1
    }

    aggregates.iterator
  }

  // only used for degree compute
  def updateMasters(
      values: Iterator[(VertexId, (Int, Int))]): SimpleEdgePartition[ED] = {
    val newAttrs = new Array[(Int, Int)](masters.capacity)
    val newMask = new BitSet(masters.capacity)

    values.foreach { product =>
      val vid = product._1
      val vdata = product._2
      val pos = masters.getPos(vid)
      // println("Value pos " + vid + " " + pos)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          newAttrs(pos) = (newAttrs(pos)._1 + vdata._1, newAttrs(pos)._2 + vdata._2)
          // count += 1
        } else { // otherwise just store the new value
          newMask.set(pos)
          newAttrs(pos) = vdata
        }
      }
    }

    var i = newMask.nextSetBit(0)
    while (i >= 0) {
      masterDegrees(i) = newAttrs(i)
      i = newMask.nextSetBit(i + 1)
    }
    this.withGlobalDegrees(newAttrs)
  }

  def masterDegreeIterator: Iterator[(VertexId, (Int, Int))] =
    masters.iterator.map(vid => (vid, masterDegrees(masters.getPos(vid))))

  def localDegreeIterator: Iterator[(VertexId, (Int, Int))] =
    vertices.iterator.map(vid =>
      (vid,
        (localInDegrees(vertices.getPos(vid)),
        localOutDegrees(vertices.getPos(vid)))))

  def globalDegreeIterator: Iterator[(VertexId, (Int, Int))] =
    vertices.iterator.map(vid =>
      (vid, globalDegrees(vertices.getPos(vid))))

  def withVertexAttrs(
      vertAttrs: PrimitiveKeyOpenHashMap[VertexId, (Int, Int)]):
  SimpleEdgePartition[ED] = {
    new SimpleEdgePartition(
      null, null,
      vertAttrs.keySet,
      vertAttrs._values,
      edges)
  }

  def withGlobalDegrees(globalDegrees: Array[(Int, Int)]): SimpleEdgePartition[ED] = {
    new SimpleEdgePartition(
      masters, masterDegrees, vertices, globalDegrees,
      edges)
  }

  def withMasters(newMasters: OpenHashSet[VertexId]): SimpleEdgePartition[ED] = {
    new SimpleEdgePartition(
      newMasters, masterDegrees, vertices, globalDegrees,
      edges)
  }
}
