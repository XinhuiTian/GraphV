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

package org.apache.spark.graphxpp.impl

// // scalastyle:off println
import org.apache.spark.graphxpp.EdgeActiveness.EdgeActiveness

import scala.collection.immutable.IndexedSeq
import scala.reflect.ClassTag

import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphxpp.utils.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{BitSet, PrimitiveVector}
import org.apache.spark.{HashPartitioner, graphxpp}
import org.apache.spark.graphxpp._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
 * Created by XinhuiTian on 16/12/21.
 */

class ShippedMsg[VD: ClassTag](val vids: Array[VertexId], val attrs: Array[VD])
  extends Serializable {
  def iterator: Iterator[(VertexId, VD)] =
    (0 until vids.length).iterator.map { i => (vids(i), attrs(i)) }

  override def toString: String = {
    var str = ""
    vids.foreach(vid => str += vid + " ")
    str += "\n"
    attrs.foreach(attr => str += attr + " ")
    str += "\n"
    str
  }
}

case class RemoteMsg[A: ClassTag](vid: VertexId, msg: A)

case class AllMsgs[A: ClassTag](localMasterMsgs: Iterator[(VertexId, A)],
  globalMirrorMsgs: Iterator[(PartitionID, (VertexId, A))])

// TODO: using a bitmap to record the mirrors of each master
private[graphxpp]
class EdgePartition[
@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassTag, VD: ClassTag](
  localSrcIds: Array[Int],
  localDstIds: Array[Int],
  data: Array[ED],
  index: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
  // masters: Array[Iterable[PartitionID]],
  masters: Array[Array[Int]],
  mirrors: Array[PartitionID],
  global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
  local2global: Array[VertexId],
  masterAttrs: Array[VD],
  mirrorAttrs: Array[VD],
  masterMask: BitSet,
  numPartitions: Int,
  activeSet: Option[VertexSet]
)
  extends Serializable {

  val edgeSize: Int = localSrcIds.size

  def vertexSize: Int = local2global.length

  def mastersSize: Int = vertexSize - mirrors.length

  def getMasters: Array[VertexId] = local2global.slice(0, mastersSize)

  def getMirrors: Array[VertexId] = local2global.slice(mastersSize, vertexSize)

  def getMastersWithAttr: IndexedSeq[(VertexId, VD)] = {
    (0 until mastersSize).map(i => (local2global(i), masterAttrs(i)))
  }

  def getMirrorsWithAttr: IndexedSeq[(VertexId, VD)] = {
    (0 until mirrors.size).map(i => (local2global(i + mastersSize), mirrorAttrs(i)))
  }

  def getLocalMastersWithAttr: Array[VD] = masterAttrs

  def getRoutingTable: Array[Array[Int]] = masters

  def vertexAttr(index: Int): VD = {
    if (index < mastersSize) {
      masterAttrs (index)
    } else {
      mirrorAttrs (index - mastersSize)
    }
  }
  // def getMirrors: Array[(VertexId, PartitionID)] = mirrors

  def getLocal2Global: Array[VertexId] = local2global

  def getGlobal2Local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int] = global2local

  def getActiveSet: Option[VertexSet] = activeSet

  def iterator: Iterator[Edge[ED]] = new Iterator[Edge[ED]] {
    private[this] val edge = new Edge[ED]
    private[this] var pos = 0

    def hasNext(): Boolean = pos < localSrcIds.length

    def next(): Edge[ED] = {
      edge.srcId = local2global(localSrcIds(pos))
      edge.dstId = local2global(localDstIds(pos))
      edge.attr = data(pos)
      pos += 1
      edge
    }
  }

  def tripletIterator(
    includeSrc: Boolean = true, includeDst: Boolean = true)
  : Iterator[EdgeTriplet[ED, VD]] = new Iterator[EdgeTriplet[ED, VD]] {
    private[this] var pos = 0

    override def hasNext: Boolean = pos < edgeSize

    override def next(): EdgeTriplet[ED, VD] = {
      val triplet = new EdgeTriplet[ED, VD]
      val localSrcId = localSrcIds(pos)
      val localDstId = localDstIds(pos)
      triplet.srcId = local2global(localSrcId)
      triplet.dstId = local2global(localDstId)
      if (includeSrc) {
        triplet.srcAttr = vertexAttr(localSrcId)
      }
      if (includeDst) {
        triplet.dstAttr = vertexAttr(localDstId)
      }
      triplet.attr = data(pos)
      pos += 1
      triplet
    }
  }

  def isActive(vid: VertexId): Boolean = {
    activeSet match {
      case None => false
      case _ =>
        activeSet.get.contains(vid)
    }

  }

  // local edge level operation
  // input: get all the masters that have changed the attrs
  // output: all the masters in this partition attrs have been changed
  // 1.18: add the master mask, need to change the edgePartition structure?
  def diff(otherAttrs: Array[VD]): EdgePartition[ED, VD] = {
    val newMask = this.masterMask
    var i = newMask.nextSetBit(0)
    while (i >= 0) {
      if (masterAttrs(i) == otherAttrs(i)) {
        newMask.unset(i)
      }
      i = newMask.nextSetBit(i + 1)
    }
    this.withMasterAttrs(otherAttrs).withMask(newMask)
  }

  // only used when the updated msg has the same type with current partition
  def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[ED, VD] = {
    val newMirrorAttrs = new Array[VD](mirrors.size)
    System.arraycopy(mirrorAttrs, 0, newMirrorAttrs, 0, mirrorAttrs.length)

    while (iter.hasNext) {
      val kv = iter.next()
      newMirrorAttrs(global2local(kv._1) - mastersSize) = kv._2
    }

    new EdgePartition(localSrcIds, localDstIds, data, index, masters, mirrors,
                       global2local, local2global, masterAttrs, newMirrorAttrs,
                      masterMask, numPartitions, activeSet)
  }

  def map[ED2: ClassTag](iter: Iterator[ED2]): EdgePartition[ED2, VD] = {
    // Faster than iter.toArray, because the expected size is known.
    val newData = new Array[ED2](data.length)
    var i = 0
    while (iter.hasNext) {
      newData(i) = iter.next()
      i += 1
    }
    assert(newData.length == i)
    this.withData(newData)
  }

  // TODO: use mask to accelerate
  def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2):
    EdgePartition[ED, VD2] = {
    // val masterSize = mastersSize
    val newMasterAttrs = new Array[VD2](mastersSize)
    for (i <- 0 until mastersSize)
      newMasterAttrs(i) = f(local2global(i), masterAttrs(i))

    this.withMasterAttrs(newMasterAttrs)
  }

  // TODO: whether use a bitmask or not?
  def leftJoin[A: ClassTag, VD2: ClassTag](other: VertexAttrBlock[A], withActives: Boolean = true)
    (f: (VertexId, VD, Option[A]) => VD2): EdgePartition[ED, VD2] = {
    val otherValues = Array.fill[Option[A]](mastersSize)(None)
    val newValues = new Array[VD2](mastersSize)
    val activeVertices = new PrimitiveVector[VertexId]
    // println("EdgePartition.leftJoin")
    // other.msgs.foreach(println)

    val otherMsgs = other.clone()
    // otherMsgs.msgs.foreach(println)
    otherMsgs.msgs.foreach{ v =>
      val localVid = global2local(v._1)
      // println(v + " " + globalVid)
      otherValues.update(localVid, Some(v._2))
      activeVertices += v._1
    }
    // otherValues.foreach(println)
    // System.arraycopy(masterAttrs, 0, newValues, 0, mastersSize)
    // TODO: currently must compare all masters
    for (i <- 0 until mastersSize) {
      newValues(i) = f(local2global(i), masterAttrs(i), otherValues(i))
    }
    // println("leftJoin")

    // activeVertices.iterator.foreach(println)
    if (withActives) {
      this.withMasterAttrs (newValues)
        .withActiveSet (activeVertices.iterator)
    } else {
      this.withMasterAttrs (newValues)
    }
  }

  // change attrs of local masters, get the msg for other partitions
  def shipMasterVertexAttrs: Iterator[(PartitionID, ShippedMsg[VD])] = {
    Iterator.tabulate(numPartitions) { pid =>
      val initialSize = 64
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      var i = 0
      masters(pid).foreach { vid =>
          if (masterMask.get(vid)) {
            vids += local2global (vid)
            attrs += masterAttrs (vid)
          }
        i += 1
      }
      (pid, new ShippedMsg(vids.trim().array, attrs.trim().array))
    }
  }

  // use a msgs collection, ship all master msgs to the mirrors
  def shipVertexAttrs(msgs: Iterator[(Int, VD)]):
    Iterator[(PartitionID, Iterator[(VertexId, VD)])] = {

    /*
    val vids = Array.fill(numParts)(new PrimitiveVector[VertexId])
    val attrs = Array.fill(numParts)(new PrimitiveVector[VD2])

    for (msg <- msgs) {
      val vid = msg._1
      val attr = msg._2

      for (pid <- masters(vid)) {
        vids(pid) += local2global(vid)
        attrs(pid) += attr
      }
    }

    Iterator.tabulate(numParts) { pid =>
      (pid, new ShippedMsg(vids(pid).toArray, attrs(pid).toArray))
    }


    val remoteMsgs = Array.fill(numParts)(new PrimitiveVector[(VertexId, VD)])

    for (msg <- msgs) {
      val vid = msg._1
      val attr = msg._2

      for (pid <- masters(vid)) {
        remoteMsgs(pid) += (local2global(vid), attr)
      }
    } */


    Iterator.tabulate(numPartitions) { pid =>
      val routingTable = masters(pid)
      val attrs = new PrimitiveVector[(VertexId, VD)](mastersSize)
      for (attr <- msgs) {
        if (routingTable.contains(attr._1)) {
          attrs += (local2global(attr._1), attr._2)
        }
      }
      (pid, attrs.trim().iterator)
    }
  }

  /*
  def syncMirrors(msgs: Iterator[(VertexId, VD)]): EdgePartition[ED, VD] = {
    // compute the size of mirrors
    val mirrorSize = mirrors.size
    val masterSize = local2global.length - mirrorSize
    val newMirrorAttrs = new Array[VD](mirrorSize)
    for (msg <- msgs) {
      val localMirror = global2local(msg._1) - masterSize
      newMirrorAttrs(localMirror) = msg._2
    }
    // this
    this.withMirrorAttrs(newMirrorAttrs)
  }
  */

  def syncMirrors(msgs: Iterator[(VertexId, VD)]): EdgePartition[ED, VD] = {
    // compute the size of mirrors
    val mirrorSize = mirrors.length
    val newMirrorAttrs = new Array[VD](mirrorSize)

    while (msgs.hasNext) {
      val kv = msgs.next()
      newMirrorAttrs(global2local(kv._1) - mastersSize) = kv._2
    }
    // this
    this.withMirrorAttrs(newMirrorAttrs)
  }

  def shipActiveSet: Iterator[(PartitionID, Iterator[VertexId])] = {
    if (activeSet == None) {
      Iterator.empty
    } else {
      val shippedMsgs = Array
        .fill[PrimitiveVector[VertexId]](numPartitions)(new PrimitiveVector[VertexId])

      /*
      val vids = activeSet.get.iterator.foreach { vid =>
        val localVid = global2local(vid)
        val pids = masters(localVid)
        for (pid <- pids) {
          shippedMsgs(pid) += vid
        }
      }
      */

      // println("shipActiveSet")
      // activeSet.get.iterator.foreach(println)


      Iterator.tabulate(numPartitions) { pid =>
        val activeVids = new PrimitiveVector[VertexId]
        val actives = activeSet.get.iterator
        for (vid <- actives) {
          // println("active: " + global2local(vid))
          // masters(pid).foreach(println)
          if (masters(pid).contains(global2local(vid))) {
            activeVids += vid
          }
        }
        (pid, activeVids.iterator)
      }
    }
  }
  def syncActiveSet(msgs: Iterator[VertexId]): EdgePartition[ED, VD] = {
    val allActiveVertices = activeSet match {
      case None => msgs
      case _ => activeSet.get.iterator ++ msgs
    }

    this.withActiveSet(allActiveVertices)
  }

  // separate local master msgs and mirror msgs
  def filterRemoteMsgs[A: ClassTag](localAgg: Iterator[(Int, A)]): AllMsgs[A] = {
    val partIds = new PrimitiveVector[PartitionID]
    val outMirrors = new PrimitiveVector[VertexId]
    val outMsgs = new PrimitiveVector[A]

    val localAggres = localAgg.toArray.clone()
    val localMsgs = localAggres
      .filter(agg => agg._1 < mastersSize)
      .map(agg => (local2global(agg._1), agg._2)).toIterator


    for (agg <- localAggres) yield {
      if (agg._1 >= mastersSize) {
        val globalMirror = local2global(agg._1)
        outMirrors += globalMirror
        partIds += mirrors(agg._1 - mastersSize)
        outMsgs += agg._2
      }
    }
    /*
    localAggres.filter(agg => agg._1 >= mastersSize).foreach { agg =>
      outMirrors += local2global(agg._1)
      partIds += mirrors(agg._1 - mastersSize)
      outMsgs += agg._2
    }*/

    val remoteSize = partIds.length

    val remoteMsgs = Iterator.tabulate(remoteSize) { pid =>
      (partIds(pid), (outMirrors(pid), outMsgs(pid)))
    }
        // .map(agg => (agg._1.toLong, agg._2))

    // AllMsgs(localMsgs, remoteMsgs)

    // localAgg.filter()
    AllMsgs(localMsgs, remoteMsgs)
  }

  /*
  def aggregateMsgsForVertices[VD2: ClassTag](
    iter: Iterator[Product2[VertexId, VD2]],
    reduceFunc: (VD2, VD2) => VD2): Iterator[(VertexId, VD2)] = {
    val newMasterValues = new Array[VD2](mastersSize)
    val newMirrorValues = new Array[VD2](mirrors.size)
  }
  */

  def aggregateMessagesEdgeScan[A: ClassTag](
    sendMsg: EdgeContext[VD, ED, A] => Unit,
    mergeMsg: (A, A) => A,
    tripletFields: TripletFields,
    activeness: EdgeActiveness
  ): Iterator[(Int, A)] = {
    val aggregates = new Array[A](local2global.size)
    val bitset = new BitSet(local2global.size)

    val ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    var i = 0
    // for all edges
    // try to use local vid for aggregation?
    while (i < localDstIds.size) {
      val localSrcId = localSrcIds(i)
      val srcId = local2global(localSrcId)
      val localDstId = localDstIds(i)
      val dstId = local2global(localDstId)
      // println("srcId " + srcId + " " + isActive(srcId) + " dstId " + dstId + " " + isActive(dstId))
      val edgeIsActive =
        if (activeness == EdgeActiveness.Neither) true
        else if (activeness == EdgeActiveness.SrcOnly) isActive(srcId)
        else if (activeness == EdgeActiveness.DstOnly) isActive(dstId)
        else if (activeness == EdgeActiveness.Both) isActive(srcId) && isActive(dstId)
        else if (activeness == EdgeActiveness.Either) isActive(srcId) || isActive(dstId)
        else throw new Exception("unreachable")

      if (edgeIsActive) {
        // println("edgeIsActive")
        val srcAttr = if (tripletFields.useSrc) vertexAttr(localSrcId) else null.asInstanceOf[VD]
        val dstAttr = if (tripletFields.useDst) vertexAttr(localDstId) else null.asInstanceOf[VD]
        ctx.set(srcId, dstId, localSrcId, localDstId, srcAttr, dstAttr, data(i))
        // println(ctx.toEdgeTriplet)
        sendMsg(ctx)
      }
      i += 1
    }
    bitset.iterator.map { localId => (localId, aggregates(localId)) }
  }

  /** Return a new `EdgePartition` with the specified active set, provided as an iterator. */
  def withActiveSet(iter: Iterator[VertexId]): EdgePartition[ED, VD] = {
    val activeSet = new VertexSet
    // println("WithActiveSet: ")
    // iter.foreach(println)
    while (iter.hasNext) { // println("adding to activeSet")
      activeSet.add(iter.next()) }
    new EdgePartition(localSrcIds, localDstIds, data, index, masters, mirrors,
                       global2local, local2global, masterAttrs, mirrorAttrs,
                      masterMask, numPartitions, Some(activeSet))
  }

  def withMasterAttrs[VD2: ClassTag](newMasterAttrs: Array[VD2]): EdgePartition[ED, VD2] = {
    // VD == VD2
    /*
    if (eq != null) {
      println("eq")
      new EdgePartition(localSrcIds, localDstIds, data, index, masters, mirrors,
                         global2local, local2global, attrs, mirrorAttrs.asInstanceOf[Array[VD2]],
                         numPartitions, activeSet)
    }
    else {
    */
      // println("Not eq")
    new EdgePartition(localSrcIds, localDstIds, data, index, masters, mirrors,
                         global2local, local2global, newMasterAttrs, new Array[VD2](mirrors.size),
                      masterMask, numPartitions, activeSet)
    // }
  }

  def withMirrorAttrs(newMirrorAttrs: Array[VD]): EdgePartition[ED, VD] = {
    new EdgePartition(localSrcIds, localDstIds, data, index, masters, mirrors,
                       global2local, local2global, masterAttrs, newMirrorAttrs,
                      masterMask, numPartitions, activeSet)
  }

  /** Return a new `EdgePartition` with the specified edge data. */
  def withData[ED2: ClassTag](newData: Array[ED2]): EdgePartition[ED2, VD] = {
    new EdgePartition(localSrcIds, localDstIds, newData, index, masters, mirrors,
      global2local, local2global, masterAttrs, mirrorAttrs,
      masterMask, numPartitions, activeSet)
  }

  def withMask(newMask: BitSet): EdgePartition[ED, VD] = {
    new EdgePartition(localSrcIds, localDstIds, data, index, masters, mirrors,
      global2local, local2global, masterAttrs, mirrorAttrs,
      newMask, numPartitions, activeSet)
  }
}

private class AggregatingEdgeContext[VD, ED, A](
  mergeMsg: (A, A) => A,
  aggregates: Array[A],
  bitset: BitSet)
  extends EdgeContext[VD, ED, A] {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _
  private[this] var _srcAttr: VD = _
  private[this] var _dstAttr: VD = _
  private[this] var _attr: ED = _

  def set(
    srcId: VertexId, dstId: VertexId,
    localSrcId: Int, localDstId: Int,
    srcAttr: VD, dstAttr: VD,
    attr: ED) {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
  }

  def setSrcOnly(srcId: VertexId, localSrcId: Int, srcAttr: VD) {
    _srcId = srcId
    _localSrcId = localSrcId
    _srcAttr = srcAttr
  }

  def setRest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED) {
    _dstId = dstId
    _localDstId = localDstId
    _dstAttr = dstAttr
    _attr = attr
  }

  override def srcId: VertexId = _srcId
  override def dstId: VertexId = _dstId
  override def srcAttr: VD = _srcAttr
  override def dstAttr: VD = _dstAttr
  override def attr: ED = _attr

  override def sendToSrc(msg: A) {
    send(_localSrcId, msg)
  }
  override def sendToDst(msg: A) {
    send(_localDstId, msg)
  }

  @inline private def send(localId: Int, msg: A) {
    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
  }
}

