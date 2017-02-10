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

package org.apache.spark.graphxpp

import scala.language.implicitConversions
import org.apache.spark.graphxpp.impl.{GraphImpl, VertexAttrBlock, EdgeRDDImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Created by XinhuiTian on 16/11/27.
 */
abstract class Graph [ED: ClassTag, VD: ClassTag] protected () extends Serializable {
  var edges: EdgeRDDImpl[ED, VD]

  val vertices: RDD[VertexId] = edges.vertices

  val verticesWithAttrs: RDD[(VertexId, VD)] = edges.verticesWithAttrs

  val triplets: RDD[EdgeTriplet[ED, VD]]


  def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)
    (implicit eq: VD =:= VD2 = null): Graph[ED, VD2]


  def mapTriplets[ED2: ClassTag](
    map: EdgeTriplet[ED, VD] => ED2,
    tripletFields: TripletFields): Graph[ED2, VD] = {
    mapTriplets((pid, iter) => iter.map(map), tripletFields)
  }

  def mapTriplets[ED2: ClassTag](
    f: (PartitionID, Iterator[EdgeTriplet[ED, VD]]) => Iterator[ED2],
    tripletFields: TripletFields): Graph[ED2, VD]

  def upgrade

  /*
  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[ED2, VD] = {
    mapEdges((pid, iter) => iter.map(map))
  }

  def mapEdges[ED2: ClassTag](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])
  : Graph[ED2, VD] */

  def aggregateMessages[A: ClassTag](
    sendMsg: EdgeContext[VD, ED, A] => Unit,
    mergeMsg: (A, A) => A,
    tripletFields: TripletFields = TripletFields.All)
  : RDD[(PartitionID, VertexAttrBlock[A])] = {
    aggregateMessagesWithActiveSet(sendMsg, mergeMsg, tripletFields, None)
  }

  private[graphxpp] def aggregateMessagesWithActiveSet[A: ClassTag](
    sendMsg: EdgeContext[VD, ED, A] => Unit,
    mergeMsg: (A, A) => A,
    tripletFields: TripletFields,
    activeSetOpt: Option[(RDD[(PartitionID, VertexAttrBlock[A])], EdgeDirection)])
  : RDD[(PartitionID, VertexAttrBlock[A])]

  def joinMsgs[A: ClassTag](msgs: RDD[(PartitionID, VertexAttrBlock[A])],
      withActives: Boolean = true)
    (mapFunc: (VertexId, VD, A) => VD): GraphImpl[ED, VD]

  def outerJoinMasters[VD2: ClassTag, A: ClassTag]
    (msgs: RDD[(PartitionID, VertexAttrBlock[A])], withActives: Boolean = true)
    (updateF: (VertexId, VD, Option[A]) => VD2)(implicit eq: VD =:= VD2 = null)
  : Graph[ED, VD2]

  def cache(): Graph[ED, VD]

  val ops = new GraphOps(this)
}

object Graph {

  def fromEdges[VD: ClassTag, ED: ClassTag](
    edges: RDD[Edge[ED]],
    defaultValue: VD,
    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[ED, VD] = {
    GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
  }

  def fromEdgeTuples[VD: ClassTag](
    rawEdges: RDD[(VertexId, VertexId)],
    defaultValue: VD,
    uniqueEdges: Option[PartitionStrategy] = None,
    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): GraphImpl[Int, VD] = {

    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    val graph = GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
    graph
  }

  /**
   * Implicitly extracts the [[GraphOps]] member from a graph.
   *
   * To improve modularity the Graph type only contains a small set of basic operations.
   * All the convenience operations are defined in the [[GraphOps]] class which may be
   * shared across multiple graph implementations.
   */
  implicit def graphToGraphOps[VD: ClassTag, ED: ClassTag]
    (g: Graph[VD, ED]): GraphOps[VD, ED] = g.ops
}
