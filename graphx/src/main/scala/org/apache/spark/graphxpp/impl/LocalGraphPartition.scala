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

import scala.reflect.ClassTag

import org.apache.spark.graphxpp._
import org.apache.spark.graphxpp.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.graphxpp.utils.collection.PGCsrMap
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

/**
 * Created by XinhuiTian on 17/3/24.
 */
class LocalGraphPartition[
@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassTag, VD: ClassTag](
    localEdges: EdgeInfos[ED],
    masterRouteTable: RoutingTablePartition,
    global2local: PrimitiveKeyOpenHashMap[VertexId, Int],
    local2global: Array[VertexId],
    masterBytes: Array[Byte],
    vertexAttrs: Array[VD],
    activeSet: Option[OpenHashSet[Int]]
) {


  def lowMasterSize: Int = 0

  def highMasterStartIndex: Int = 0

  def highMasterEndIndex: Int = 0

  def highMirrorStartIndex: Int = 0

  def highMirrorEndIndex: Int = 0

  /*
  def updateAndPropagate[A: ClassTag](
      vertexSendMsg: AggregateVertexContext[VD, ED, A] => Unit,
      edgeSendMsg: AggregateEdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      vprog: (VertexId, VD, A) => VD,
      tripletFields: TripletFields,
      activeness: EdgeActiveness): Iterator[(VertexId, A)] = {
    // 1. compute vertex aggregation
    val aggres = new Array[A](vertexAttrs.length)
    val vctx = new AggregateVertexContext[VD, ED, A](mergeMsg)
    // src only, src to dst
    if (activeness == EdgeActiveness.SrcOnly) {
      // for all the masters
      for (i <- 0 until lowMasterSize) {
        var vertexIsActive = false
        val inEdges = localEdges.getInEdges(i)
        for (src <- inEdges) {
          if (isActive(src)) {
            vertexIsActive = true
          }
        }
        // currently, recompute all edges
        if (vertexIsActive) {
          vctx.reset
          val globalDstId = local2global(i)
          localEdges.getInEdgeAttrs(i).foreach { edge =>
            val localSrcId = edge._1
            val globalSrcId = local2global(localSrcId)
            vctx.setSrc(globalSrcId, globalDstId,
              vertexAttrs(localSrcId), vertexAttrs(i), edge._2)
            vertexSendMsg(vctx)
          }

          // for in edges only, update vertex attr immediately
          vertexAttrs (i) = vprog (local2global (i), vertexAttrs (i), vctx.aggregate)
        }
      }
    }

    // dst only, dst to src
    if (activeness == EdgeActiveness.DstOnly) {
      // for all the masters
      for (i <- 0 until lowMasterSize) {
        var vertexIsActive = false
        val outEdges = localEdges.getOutEdges(i)
        for (dst <- outEdges) {
          if (isActive(dst)) {
            vertexIsActive = true
          }
        }
        // currently, recompute all edges
        if (vertexIsActive) {
          vctx.reset
          val globalSrcId = local2global(i)
          localEdges.getOutEdgeAttrs(i).foreach { edge =>
            val localDstId = edge._1
            val globalDstId = local2global(localDstId)
            vctx.setDst(globalSrcId, globalDstId,
              vertexAttrs(i), vertexAttrs(localDstId), edge._2)
            vertexSendMsg(vctx)
          }
          // for in edges only, update vertex attr immediately
          vertexAttrs (i) = vprog (local2global (i), vertexAttrs (i), vctx.aggregate)
        }
      }
    }

    // both side computation
    if (activeness == EdgeActiveness.Both) {
      // for all the masters
      for (i <- 0 until lowMasterSize) {
        var vertexIsActive = false
        val outEdges = localEdges.getOutEdges(i)
        for (dst <- outEdges) {
          if (isActive(dst)) {
            vertexIsActive = true
          }
        }

        val inEdges = localEdges.getInEdges(i)
        for (src <- inEdges) {
          if (isActive(src)) {
            vertexIsActive = true
          }
        }

        // currently, recompute all edges
        if (vertexIsActive) {
          vctx.reset
          val globalId = local2global(i)
          localEdges.getOutEdgeAttrs(i).foreach { edge =>
            val localDstId = edge._1
            val globalDstId = local2global(localDstId)
            vctx.setDst(globalId, globalDstId,
              vertexAttrs(i), vertexAttrs(localDstId), edge._2)
            vertexSendMsg(vctx)
          }

          localEdges.getInEdgeAttrs(i).foreach { edge =>
            val localSrcId = edge._1
            val globalSrcId = local2global(localSrcId)
            vctx.setSrc(globalSrcId, globalId,
              vertexAttrs(localSrcId), vertexAttrs(i), edge._2)
            vertexSendMsg(vctx)
          }

          // for all direction edges, update vertex attr immediately
          vertexAttrs (i) = vprog (local2global (i), vertexAttrs (i), vctx.aggregate)
        }
      }
    }

    // 2. compute edge aggregation
    val bitset = new BitSet(vertexAttrs.length)
    val ectx = new AggregateEdgeContext[VD, ED, A](mergeMsg, aggres, bitset)

    if (activeness == EdgeActiveness.SrcOnly) {
      // for all the high degree master and mirrors
      for (i <- highMasterStartIndex to highMirrorEndIndex) {
        // currently, recompute all edges
        ectx.setLocalDst(i)
        localEdges.getInEdgeAttrs(i).foreach { edge =>
          val localSrcId = edge._1
          if (isActive(localSrcId)) {
            val globalSrcId = local2global(localSrcId)
            val globalDstId = local2global(i)
            ectx.setSrc (globalSrcId, globalDstId, vertexAttrs (localSrcId), vertexAttrs(i), edge._2)
            edgeSendMsg
          }
        }
        // for in edges only, update vertex attr immediately
        vertexAttrs (i) = vprog (local2global (i), vertexAttrs (i), vctx.aggregate)
      }
    }

  }

  def isLowMaster(vid: Int): Boolean

  def isActive(vid: Int): Boolean = {
    activeSet.get.contains(vid)
  }

  */
}

class EdgeInfos[ED: ClassTag](
  localInEdges: PGCsrMap, // for dsts
  localOutEdges: PGCsrMap, // for srcs
  edgeAttrs: Array[ED],
  edgeDstIndex: Array[Int]) {

  def getInEdges(dstId: Int): Iterator[Int] = localInEdges.valueRange(dstId)

  def getOutEdges(srcId: Int): Iterator[Int] = localOutEdges.valueRange(srcId)

  def getInEdgeAttr(eid: Int): ED = {
    val inEdgeIndex = edgeDstIndex(eid)
    edgeAttrs(inEdgeIndex)
  }

  def getOutEdgeAttr(eid: Int): ED = edgeAttrs(eid)

  def getInEdgeAttrs(vid: Int): Iterator[(Int, ED)] = {
    localInEdges.keyRange(vid).map { key =>
      (localInEdges.values(key), getInEdgeAttr(key))
    }
  }

  def getOutEdgeAttrs(vid: Int): Iterator[(Int, ED)] = {
    localOutEdges.keyRange(vid).map { key =>
      (localOutEdges.values(key), getOutEdgeAttr(key))
    }
  }
}

object EdgeInfos {
  def edgeSort[ED: ClassTag](srcIds: Array[Int], dstIds: Array[Int], edgeAttrs: Array[ED]):
  EdgeInfos[ED] = {
    // sort the srcIds, outEdges
    val (permuteIndex, srcPrefix) = PGCsrMap.countingSort(srcIds)

    // change the order of dsts based on srcs
    var swap_src = 0
    var swap_dst = 0
    var swap_attr = null.asInstanceOf[ED]

    for (i <- 0 until permuteIndex.size) {
      if (i != permuteIndex(i)) {
        var j = i
        swap_src = srcIds(i)
        swap_dst = dstIds(i)
        swap_attr = edgeAttrs(i)

        var flag = true
        while (j != permuteIndex(i) && flag == true) {
          val next = permuteIndex(j)
          if (next != i) {
            srcIds(j) = srcIds(next)
            dstIds(j) = dstIds(next)
            edgeAttrs(j) = edgeAttrs(next)
            permuteIndex(j) = j
            j = next
          } else {
            srcIds(j) = swap_src
            dstIds(j) = swap_dst
            edgeAttrs(j) = swap_attr
            permuteIndex(j) = j
            flag = false
          }
        }
      }
    }

    val (permuteDstIndex, dstPrefix) = PGCsrMap.countingSort(dstIds)
    val newSrcIds = new Array[Int](srcIds.size)

    // inEdges
    for (i <- 0 until permuteDstIndex.size) {
      // println(permuteDstIndex(i))
      newSrcIds(i) = srcIds(permuteDstIndex(i))
    }

    new EdgeInfos(new PGCsrMap(dstPrefix, newSrcIds),
      new PGCsrMap(srcPrefix, dstIds),
      edgeAttrs, permuteDstIndex)
  }
}

private class AggregateVertexContext[VD, ED, A](
    mergeMsg: (A, A) => A)
  extends ExecutionContext[VD, ED, A] {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _srcAttr: VD = _
  private[this] var _dstAttr: VD = _
  private[this] var _attr: ED = _
  private[this] var _sendSrc: Boolean = _

  private[this] var _aggregate: A = _

  def setSrc(
    srcId: VertexId,
    dstId: VertexId,
    srcAttr: VD,
    dstAttr: VD,
    attr: ED) {
    set(srcId, dstId, srcAttr, dstAttr, attr)
    _sendSrc = true
  }

  def setDst(
    srcId: VertexId,
    dstId: VertexId,
    srcAttr: VD,
    dstAttr: VD,
    attr: ED) {
    set(srcId, dstId, srcAttr, dstAttr, attr)
    _sendSrc = false
  }

  def set(srcId: VertexId,
    dstId: VertexId,
    srcAttr: VD,
    dstAttr: VD,
    attr: ED): Unit = {
    _srcId = srcId
    _srcAttr = srcAttr
    _dstId = dstId
    _dstAttr = dstAttr
    _attr = attr
  }

  def reset {
    _aggregate = null.asInstanceOf[A]
  }

  override def srcId: VertexId = _srcId
  override def dstId: VertexId = _dstId
  override def srcAttr: VD = _srcAttr
  override def dstAttr: VD = _dstAttr
  override def attr: ED = _attr

  def aggregate: A = _aggregate

  override def sendToDst(msg: A): Unit = {
    send(msg)
  }

  override def sendToSrc(msg: A): Unit = {
    send(msg)
  }

  @inline private def send(msg: A): Unit = {
    if (_aggregate != null) {
      _aggregate = mergeMsg (aggregate, msg)
    } else {
      _aggregate = msg
    }
  }
}

private class AggregateEdgeContext[VD, ED, A](
  mergeMsg: (A, A) => A,
  aggregates: Array[A],
  bitset: BitSet)
  extends ExecutionContext[VD, ED, A] {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _
  private[this] var _srcAttr: VD = _
  private[this] var _dstAttr: VD = _
  private[this] var _attr: ED = _
  private[this] var _sendSrc: Boolean = _

  def setLocalSrc(localSrcId: Int) {
    _localSrcId = localSrcId
  }

  def setLocalDst(localDstId: Int) {
    _localDstId = localDstId
  }

  def setSrc(
    srcId: VertexId,
    dstId: VertexId,
    srcAttr: VD,
    dstAttr: VD,
    attr: ED) {
    set(srcId, dstId, srcAttr, dstAttr, attr)
    _sendSrc = true
  }

  def setDst(
    srcId: VertexId,
    dstId: VertexId,
    srcAttr: VD,
    dstAttr: VD,
    attr: ED) {
    set(srcId, dstId, srcAttr, dstAttr, attr)
    _sendSrc = false
  }

  def set(srcId: VertexId,
    dstId: VertexId,
    srcAttr: VD,
    dstAttr: VD,
    attr: ED): Unit = {
    _srcId = srcId
    _srcAttr = srcAttr
    _dstId = dstId
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

