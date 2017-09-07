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
package org.apache.spark.graphloca.impl

import scala.reflect.ClassTag

import org.apache.spark.graphloca._
import org.apache.spark.graphloca.util.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.BitSet

/**
 * Created by XinhuiTian on 17/3/29.
 */
private[graphloca]
class LocalGraphPartition[
@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD: ClassTag, ED: ClassTag](
  localSrcIds: Array[Int], // local src ids
  localDstIds: Array[Int], // local dst ids
  edgeData: Array[ED],     // edge attrs
  global2local: PrimitiveKeyOpenHashMap[VertexId, Int], // first masters, then mirrors
  local2global: Array[VertexId], // vertex array for local compute and msg generation
  masterAttrs: Array[VD],        // master attrs
  mirrorAttrs: Array[VD],        // mirror attrs
  masterMask: BitSet,            // which master should be considered for computations
  inComMask: BitSet,             // which vertex is in direction complete
  outComMask: BitSet,            // which vertex is out direction complete
  masterRoutingTable: RoutingTablePartition, // routing table for master-mirror update
  // mirrorPids: Array[(VertexId, PartitionID)],
  numPartitions: Int,
  activeSet: Option[VertexSet]
) {

  val edgeSize: Int = localSrcIds.size
  val vertexSize: Int = local2global.length
  val mastersSize: Int = masterAttrs.length
  val mirrorsSize: Int = mirrorAttrs.length

  def getMasters: Array[VertexId] = local2global.slice (0, mastersSize)

  def getMirrors: Array[VertexId] = local2global.slice (mastersSize, vertexSize)

  def getMask: BitSet = masterMask

  def getMastersWithAttr: IndexedSeq[(VertexId, VD)] = {
    (0 until mastersSize).map (i => (local2global (i), masterAttrs (i)))
  }

  def getMirrorsWithAttr: IndexedSeq[(VertexId, VD)] = {
    (0 until mirrorsSize).map (i => (local2global (i + mastersSize), mirrorAttrs (i)))
  }

  def getLocalMastersWithAttr: Array[VD] = masterAttrs

  def getRoutingTable: RoutingTablePartition = masterRoutingTable

  def vertexAttr(index: Int): VD = {
    if (index < mastersSize) {
      masterAttrs (index)
    } else {
      mirrorAttrs (index - mastersSize)
    }
  }

  def getLocal2Global: Array[VertexId] = local2global

  def getGlobal2Local: PrimitiveKeyOpenHashMap[VertexId, Int] = global2local

  def getActiveSet: Option[VertexSet] = activeSet

  def edgeIterator: Iterator[Edge[ED]] = new Iterator[Edge[ED]] {
    private[this] val edge = new Edge[ED]
    private[this] var pos = 0

    def hasNext(): Boolean = pos < localSrcIds.length

    def next(): Edge[ED] = {
      edge.srcId = local2global (localSrcIds (pos))
      edge.dstId = local2global (localDstIds (pos))
      edge.attr = edgeData (pos)
      pos += 1
      edge
    }
  }

  def tripletIterator(
    includeSrc: Boolean = true, includeDst: Boolean = true)
  : Iterator[EdgeTriplet[VD, ED]] = new Iterator[EdgeTriplet[VD, ED]] {
    private[this] var pos = 0

    override def hasNext: Boolean = pos < edgeSize

    override def next(): EdgeTriplet[VD, ED] = {
      val triplet = new EdgeTriplet[VD, ED]
      val localSrcId = localSrcIds (pos)
      val localDstId = localDstIds (pos)
      triplet.srcId = local2global (localSrcId)
      triplet.dstId = local2global (localDstId)
      if (includeSrc) {
        triplet.srcAttr = vertexAttr (localSrcId)
      }
      if (includeDst) {
        triplet.dstAttr = vertexAttr (localDstId)
      }
      triplet.attr = edgeData (pos)
      pos += 1
      triplet
    }
  }

  def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2):
  LocalGraphPartition[VD2, ED] = {
    val newMasterAttrs = new Array[VD2](mastersSize)
    var i = masterMask.nextSetBit(0)
    while (i >= 0) {
      newMasterAttrs (i) = f (local2global (i), masterAttrs (i))
      i = masterMask.nextSetBit(i + 1)
    }

    this.withMasterAttrs(newMasterAttrs)
  }

  def withMasterAttrs[VD2: ClassTag](newMasterAttrs: Array[VD2]): LocalGraphPartition[VD2, ED] = {
    // VD == VD2
    new LocalGraphPartition (localSrcIds, localDstIds, edgeData,
      global2local, local2global, newMasterAttrs, new Array[VD2](mirrorsSize),
      masterMask, null, null, masterRoutingTable, numPartitions, activeSet)
  }
}
