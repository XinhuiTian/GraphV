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

import org.apache.spark.graphxpp._
import org.apache.spark.util.collection.SortDataFormat


/**
 * Created by XinhuiTian on 17/3/8.
 */


object LHGraphImpl {

  // what are needed?
  // 1. master type: src/dst low or high ==> get from masters
  // 2. mirror type: src/dst low or high ==> get from edges
  // edge type:
  // 1. 0x44: src as low degree master, dst is low:   src: 0x1, dst: 0x8
  // 2. 0x64: src as low degree master, dst is high:  src: 0x1, dst: 0x80
  // 3. 0x88: dst as low degree master, src is low:   src: 0x2, dst: 0x10
  // 4. 0x98: dst as low degree master, src is high:  src: 0x8, dst: 0x10
  // 5. 0x90: dst as high degree master, src is high: src: 0x8, dst: 0x40
  //
  // position, degree, master or mirror
  // 0x1: src & low
  // 0x2: dst & low
  // 0x4: src & high
  // 0x8: dst & high
  def getMirrorType(byte: Byte): Byte = {
    byte match {
      case 0x44 => 0x2
      case 0x64 => 0x8
      case 0x88 => 0x1
      case 0x98 => 0x4
      case 0x90 => 0x4
      case _ => 0x0
    }
  }

  /*
  def generateLocalGraphPartition[ED: ClassTag, VD: ClassTag](
    pid: Int, numPart: Int, defaultValue: VD,
    edgePartition: SimpleEdgeWithVertexPartition[ED])
  : (Int, LocalGraphPartition) = {
    // Determine which positions each vertex id appears in using a map where the low 2 bits
    // represent src and dst
    // edgePartition.vertexIterator.map { vid => (vid, pid) }
    // 1. sort masters: src/dst both low, only src low, only dst low, all high
    val masters = edgePartition.masters
    val edges = edgePartition.edges.map { _._1 }
    val edgeAttrs = edgePartition.edges.map{ _._1.attr }

    val partitioner = new HashPartitioner(numPart)
    val srcHighMirrors = new PrimitiveVector[VertexId]
    val dstHighMirrors = new PrimitiveVector[VertexId]
    val lowMirrors = new PrimitiveVector[VertexId]
    edgePartition.edges.foreach { edge =>
      val mirrorType = getMirrorType(edge._2)
      val srcMasterPid = partitioner.getPartition(edge._1.srcId)
      val dstMasterPid = partitioner.getPartition(edge._1.dstId)
      mirrorType match {
        // case 0x4 => if (edge._1.srcId)
        case 0x4 =>
          if (pid != srcMasterPid) {
            srcHighMirrors += edge._1.srcId
          }
        case 0x8 =>
          if (pid != dstMasterPid) {
            dstHighMirrors += edge._1.dstId
          }
        case 0x1 =>
          if (pid != srcMasterPid) {
            lowMirrors += edge._1.srcId
          }
        case 0x2 =>
          if (pid != dstMasterPid) {
            lowMirrors += edge._1.dstId
          }
      }
    }

    val global2local = new PrimitiveKeyOpenHashMap[VertexId, Int]
    val local2global = new PrimitiveVector[VertexId]

    var currLocalId = -1

    masters.iterator.foreach(v => global2local.changeValue(v._1, {
      currLocalId += 1; local2global += v._1; currLocalId
    }, identity))

    val mirrors = srcHighMirrors.toArray ++
      dstHighMirrors.toArray ++
      lowMirrors.toArray

    mirrors.iterator.foreach(v => global2local.changeValue(v, {
      currLocalId += 1; local2global += v; currLocalId
    }, identity))

    val localSrcIds = new Array[Int](edges.length)
    // array for local dst vertices storage
    val localDstIds = new Array[Int](edges.length)

    if (edges.length > 0) {
      var i = 0
      while (i < edges.length) {
        val srcId = edges(i).srcId
        val dstId = edges(i).dstId
        localSrcIds(i) = global2local(srcId)
        localDstIds(i) = global2local(dstId)
        i += 1
      }
    }

    val localEdges =
      EdgeInfos.edgeSort(localSrcIds, localDstIds, edgeAttrs)

    val masterBytes = masters.map(_._2)
  } */

  // used for vertex sort
  def vertexArraySortFormat = {
    new SortDataFormat[Byte, Array[(VertexId, Byte)]] {
      override def getKey(data: Array[(VertexId, Byte)], pos: Int): Byte = {
        data(pos)._2
      }

      override def swap(data: Array[(VertexId, Byte)], pos0: Int, pos1: Int): Unit = {
        val tmp = data(pos0)
        data(pos0) = data(pos1)
        data(pos1) = tmp
      }

      override def copyElement(
        src: Array[(VertexId, Byte)], srcPos: Int,
        dst: Array[(VertexId, Byte)], dstPos: Int) {
        dst(dstPos) = src(srcPos)
      }

      override def copyRange(
        src: Array[(VertexId, Byte)], srcPos: Int,
        dst: Array[(VertexId, Byte)], dstPos: Int, length: Int) {
        System.arraycopy(src, srcPos, dst, dstPos, length)
      }

      override def allocate(length: Int): Array[(VertexId, Byte)] = {
        new Array[(VertexId, Byte)](length)
      }
    }
  }

  /*
  def fromEdgeSimple[ED: ClassTag, VD: ClassTag]
  (edges: RDD[(Int, SimpleEdgeWithVertexPartition[ED])],
    numPartitions: Int, defaultVertexAttr: VD,
    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY):
  LHGraphImpl[ED, VD] = {

    edges.mapPartitions { iter =>
      val part = iter.next()
      val pid = part._1
      val edgePart = part._2



      val vertexPartitioner = new HashPartitioner(numPartitions)

      // compute mirrors
      val mirrors = vertices.iterator
        .map( vid => (vid, vertexPartitioner.getPartition(vid)) )
        .filter(_._2 != pid).toArray

      val masters = edgePart.masters

      val localSrcIds = new Array[Int](edges.length)
      // array for local dst vertices storage
      val localDstIds = new Array[Int](edges.length)

      val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]

      val local2global = new PrimitiveVector[VertexId]
      var vertexAttrs = Array.empty[VD]

      var currLocalId = -1

      masters.iterator.foreach(v => global2local.changeValue(v._1,
      { currLocalId += 1; local2global += v; currLocalId }, identity))

      mirrors.foreach(v => global2local.changeValue(v._1, {
        currLocalId += 1; local2global += v._1; currLocalId
      }, identity))



    }

  }
  */
}
