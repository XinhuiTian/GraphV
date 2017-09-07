package org.apache.spark.graphloca

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

/**
 * Created by XinhuiTian on 17/3/29.
 */
abstract class Graph [VD: ClassTag, ED: ClassTag] protected () extends Serializable {
  val localGraph: LocalGraph[VD, ED]

  val triplets: RDD[EdgeTriplet[VD, ED]]

  def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)
    (implicit eq: VD =:= VD2 = null): Graph[VD2, ED]


  /*
  def mapTriplets[ED2: ClassTag](
    map: EdgeTriplet[ED, VD] => ED2,
    tripletFields: TripletFields): Graph[VD, ED2] = {
    mapTriplets((pid, iter) => iter.map(map), tripletFields)
  }
 */
  /*
  def mapTriplets[ED2: ClassTag](
    f: (PartitionID, Iterator[EdgeTriplet[ED, VD]]) => Iterator[ED2],
    tripletFields: TripletFields): Graph[VD, ED2]

  // def upgrade

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

  private[org.apache.spark.graphloca] def aggregateMessagesWithActiveSet[A: ClassTag](
    sendMsg: EdgeContext[VD, ED, A] => Unit,
    mergeMsg: (A, A) => A,
    tripletFields: TripletFields,
    activeSetOpt: Option[(RDD[(PartitionID, VertexAttrBlock[A])], EdgeDirection)])
  : RDD[(PartitionID, VertexAttrBlock[A])]

  def joinMsgs[A: ClassTag](msgs: RDD[(PartitionID, VertexAttrBlock[A])],
    withActives: Boolean = true)
    (mapFunc: (VertexId, VD, A) => VD): GraphImpl[VD, ED]

  def outerJoinMasters[VD2: ClassTag, A: ClassTag]
  (msgs: RDD[(PartitionID, VertexAttrBlock[A])], withActives: Boolean = true)
    (updateF: (VertexId, VD, Option[A]) => VD2)(implicit eq: VD =:= VD2 = null)
  : Graph[ED, VD2]
*/
  def cache(): Graph[VD, ED]

  // val ops = new GraphOps(this)
}

object Graph {

  /*
  def fromEdges[VD: ClassTag, ED: ClassTag](
    edges: RDD[Edge[ED]],
    defaultValue: VD,
    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] = {
    GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
  }

  def fromEdgeTuples[VD: ClassTag](
    rawEdges: RDD[(VertexId, VertexId)],
    defaultValue: VD,
    uniqueEdges: Option[PartitionStrategy] = None,
    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): GraphImpl[VD, Int] = {

    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    val graph = GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
    graph
  }
  */


}
