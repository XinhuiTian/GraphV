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

import org.apache.spark.HashPartitioner

import org.apache.spark.graphloca._
import org.apache.spark.graphloca.impl.RoutingTablePartition._
import org.apache.spark.graphloca.util.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector, Sorter}

/**
 * Created by XinhuiTian on 17/3/30.
 */
// used for store aggregated msgs
class VertexAttrBlock[A: ClassTag](val msgs: Array[(VertexId, A)]) extends Serializable {
  def mergeMsgs(reduceFunc: (A, A) => A): VertexAttrBlock[A] = {
    val vertexMap = new PrimitiveKeyOpenHashMap[VertexId, A]
    msgs.foreach { msg => vertexMap.setMerge(msg._1, msg._2, reduceFunc)}
    new VertexAttrBlock(vertexMap.toArray)
  }
}

class GraphImpl[VD: ClassTag, ED: ClassTag](@transient val localGraph: LocalGraphImpl[VD, ED])
  extends Graph[VD, ED] {

  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    localGraph.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, part) => part.tripletIterator()
    })
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)
    (implicit eq: VD =:= VD2 = null): GraphImpl[VD2, ED] = {

    // val newEdges = edges.withPartitionsRDD(
    val newEdges = localGraph.mapVertices(map)

    new GraphImpl(newEdges)
  }

  override def cache(): GraphImpl[VD, ED] = {
    //this.localGraph.cache()
    this
  }
}

object GraphImpl {
  def buildSimpleFromEdges[ED: ClassTag](edges: RDD[Edge[ED]]):
  RDD[(Int, SimpleEdgePartition[ED])] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED]
      iter.foreach { e =>
        builder.add(e)
      }
      Iterator((pid, builder.toSimpleEdgePartition))
    }
    edgePartitions
  }

  def fromEdgesSimple[VD: ClassTag, ED: ClassTag]
  (edges: SimpleEdgeRDD[ED],
    numPartitions: Int, defaultVertexAttr: VD,
    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY):
  GraphImpl[VD, ED] = {
    val vertexPartitioner = new HashPartitioner(numPartitions)

    val v2p = edges.partitionRDD.mapPartitions(_.flatMap(Function.tupled(edgePartitionToMsgs)))
      .partitionBy(vertexPartitioner)

    val mastersWithRoutingTable = v2p.mapPartitionsWithIndex { (pid, iter) =>
      val routingTable = fromMsgs(numPartitions, iter)
      val masterMap = new OpenHashSet[VertexId]
      routingTable.iterator.foreach { vid =>
        masterMap.add(vid)
      }
      Iterator((pid, (masterMap.iterator.toArray, routingTable)))
    }

    val graphPartitions = edges.partitionRDD.zipPartitions(mastersWithRoutingTable) {
      (edgePartIter, masterPartIter) =>
        val (pid, edgePart) = edgePartIter.next()
        val (_, masterPart) = masterPartIter.next()
        Iterator((pid, buildGraphPartition(pid, numPartitions,
          defaultVertexAttr, masterPart._1, masterPart._2, edgePart)))
    }
    new GraphImpl(new LocalGraphImpl(graphPartitions))
  }

  def buildGraphPartition[VD: ClassTag, ED: ClassTag](pid: PartitionID,
    numParts: Int,
    defaultVertexAttr: VD,
    masters: Array[VertexId],
    routingTable: RoutingTablePartition,
    edgePart: SimpleEdgePartition[ED]): LocalGraphPartition[VD, ED] = {

    val vertices = new OpenHashSet[VertexId]
    edgePart.edges.foreach { e =>
      vertices.add(e.srcId)
      vertices.add(e.dstId)
    }
    val vertexPartitioner = new HashPartitioner(numParts)

    val mirrors = vertices.iterator
      .map( vid => (vid, vertexPartitioner.getPartition(vid)) )
      .filter(_._2 != pid).map(_._1)

    val edges = edgePart.edges
    // array for local src vertices storage
    val localSrcIds = new Array[Int](edges.length)
    // array for local dst vertices storage
    val localDstIds = new Array[Int](edges.length)

    val data = new Array[ED](edges.length)

    val mirrorSize = mirrors.length
    val masterSize = masters.length

    // map for global to local
    val global2local = new PrimitiveKeyOpenHashMap[VertexId, Int]

    // array for local to global
    val local2global = new PrimitiveVector[VertexId]
    var vertexAttrs = Array.empty[VD]

    val index = new PrimitiveKeyOpenHashMap[Int, Int]

    var currLocalId = -1

    masters.foreach(v => global2local.changeValue(v,
    { currLocalId += 1; local2global += v; currLocalId }, identity))

    mirrors.foreach(v => global2local.changeValue(v, {
      currLocalId += 1; local2global += v; currLocalId
    }, identity))

    val masterMask = new BitSet(masterSize)
    masterMask.setUntil(masterSize)

    if (edges.length > 0) {
      // index.update(edges(0).srcId, 0)
      val localEdges = edges.map { e =>
        new LocalEdge(global2local(e.srcId), global2local(e.dstId), e.attr)
      }

      new Sorter(LocalEdge.edgeArraySortDataFormat[ED])
        .sort(localEdges, 0, localEdges.length, LocalEdge.lexicographicOrdering)

      var currSrcId: Int = localEdges(0).srcId
      index.update(currSrcId, 0)
      var i = 0
      while (i < localEdges.length) {
        val localSrcId = localEdges(i).srcId
        localSrcIds(i) = localEdges(i).srcId
        localDstIds(i) = localEdges(i).dstId
        data(i) = localEdges(i).attr
        if (localSrcId != currSrcId) {
          currSrcId = localSrcId
          index.update(currSrcId, i)
        }
        i += 1
      }
      // vertexAttrs = new Array[VD](currLocalId + 1)
      // initialize vertex attrs
      vertexAttrs = Array.fill[VD](currLocalId + 1)(defaultVertexAttr)
    }

    new LocalGraphPartition(localSrcIds, localDstIds, data,
      global2local, local2global.trim.toArray,
      vertexAttrs.slice(0, masterSize),
      vertexAttrs.slice(masterSize, currLocalId + 1),
      masterMask, null, null, routingTable, numParts, None)
  }

}
