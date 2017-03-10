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

// scalastyle:off println
import org.apache.spark.HashPartitioner
import org.apache.spark.graphxpp._
import org.apache.spark.graphxpp.utils.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector}

/**
 * Created by XinhuiTian on 16/11/28.
 */
/*
class RoutingTable(val masters: Iterable[MasterPositions],
  val mirrors: Iterable[(VertexId, PartitionID)]) {

}
*/

class VertexAttrBlock[A: ClassTag](val msgs: Array[(VertexId, A)]) extends Serializable {
  def mergeMsgs(reduceFunc: (A, A) => A): VertexAttrBlock[A] = {
    val vertexMap = new GraphXPrimitiveKeyOpenHashMap[VertexId, A]
    msgs.foreach { msg => vertexMap.setMerge(msg._1, msg._2, reduceFunc)}
    new VertexAttrBlock(vertexMap.toArray)
  }
}

class GraphImpl[ED: ClassTag, VD: ClassTag](@transient var edges: EdgeRDDImpl[ED, VD])
  extends Graph[ED, VD] {

  @transient override lazy val triplets: RDD[EdgeTriplet[ED, VD]] = {
    upgrade
    edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, part) => part.tripletIterator()
    })
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)
    (implicit eq: VD =:= VD2 = null): GraphImpl[ED, VD2] = {

    // val newEdges = edges.withPartitionsRDD(
    val newEdges = edges.mapVertices(map)

    new GraphImpl(newEdges)
  }

  override def mapTriplets[ED2: ClassTag](
    f: (PartitionID, Iterator[EdgeTriplet[ED, VD]]) => Iterator[ED2],
    tripletFields: TripletFields): Graph[ED2, VD] = {
    // edges.cache()
    upgrade
    val newEdges = edges.mapEdgePartitions { (pid, part) =>
      part.map(f(pid, part.tripletIterator(tripletFields.useSrc, tripletFields.useDst)))
    }
    new GraphImpl(newEdges)
  }

  // TODO: upgrade should only ship vertices that have not been sync
  override def upgrade: Unit = {
    // println("upgrade")
    edges.cache()
    edges = edges.upgrade

    /*
    println("After upgrade")
    edges.partitionsRDD.foreach { edge =>
      edge._2.getMirrorsWithAttr.foreach(println)
      println
    }
    */

    // oldEdges.partitionsRDD.unpersist()
  }

  override def joinMsgs[A: ClassTag](msgs: RDD[(PartitionID, VertexAttrBlock[A])],
      withActives: Boolean = true)
    (mapFunc: (VertexId, VD, A) => VD): GraphImpl[ED, VD] = {
    val uf = (id: VertexId, data: VD, o: Option[A]) => {
      o match {
        case Some(u) => mapFunc(id, data, u)
        case None => data
      }
    }
    outerJoinMasters(msgs, withActives)(uf)
  }

  override def outerJoinMasters[VD2: ClassTag, A: ClassTag]
    (msgs: RDD[(PartitionID, VertexAttrBlock[A])], withActives: Boolean = true)
    (updateF: (VertexId, VD, Option[A]) => VD2)
    (implicit eq: VD =:= VD2 = null): GraphImpl[ED, VD2] = {
    // println("outerJoinMasters")
    // msgs.cache()
    if (eq != null) {
      // val oldVertexAttr = edges.mastersWithAttrs.cache()
      // println("VD == VD2")
      edges.cache()
      val tmpEdges = edges.leftJoin (msgs, withActives)(updateF).cache()
      val oldVertexAttr = edges.localMastersWithAttrs
      val newVertexAttr = tmpEdges.localMastersWithAttrs
      val newEdgesWithDiffAttrs = GraphImpl.diff(oldVertexAttr
        .asInstanceOf[RDD[(PartitionID, LocalMastersWithMask[VD2])]], newVertexAttr)
      val newEdges = tmpEdges
        .asInstanceOf[EdgeRDDImpl[ED, VD2]]
        .updateVerticesWithMask(newEdgesWithDiffAttrs)
      new GraphImpl(newEdges)

    } else {
      val newEdges = edges.leftJoin (msgs, withActives)(updateF)
      new GraphImpl (newEdges)
    }
  }

  override def aggregateMessagesWithActiveSet[A: ClassTag](
    sendMsg: EdgeContext[VD, ED, A] => Unit,
    mergeMsg: (A, A) => A,
    tripletFields: TripletFields,
    activeSetOpt: Option[(RDD[(PartitionID, VertexAttrBlock[A])], EdgeDirection)]):
    RDD[(PartitionID, VertexAttrBlock[A])] = {

    // here upgrade the mirrors
    edges.cache()
    // println("aggregateMessagesWithActiveSet")
    upgrade


    /*
    val aggEdges = activeSetOpt match {
      case Some((activeSet, _)) =>
        // println("Having activeSet")
        // activeSet.cache()
        // activeSet.collect.foreach(part => part._2.msgs.foreach(println))

        edges.withActiveSet(activeSet)
      case None =>
        edges
    } */

    val aggEdges = edges

    // aggEdges.cache()

    // test the activeSet:


    /*
    println("ActiveSet")
    aggEdges.getActiveSet.collect.foreach{ set => set
      match {
      case Some(s)=> s.iterator.foreach(println)
      case None => println
    }}
    */

    val activeDirectionOpt = activeSetOpt.map(_._2)

    // for debug
    /*
    val preAgg1 = edges.partitionsRDD.map {
      case (pid, edgePartition) =>
        // Choose scan method
        // val activeFraction = edgePartition.numActives.getOrElse(0)
        // / edgePartition.indexSize.toFloat
        val msgs = activeDirectionOpt match {
          case Some(EdgeDirection.Both) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Both)
          case Some(EdgeDirection.Either) =>
            // TODO: Because we only have a clustered index on the source vertex ID, we can't filter
            // the index here. Instead we have to scan all edges and then do the filter.
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Either)
          case Some(EdgeDirection.Out) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.SrcOnly)
          case Some(EdgeDirection.In) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.DstOnly)
          case _ => // None
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Neither)
        }
        (pid, msgs)
        // (pid, edgePartition.filterRemoteMsgs(msgs))

    }.setName("GraphImpl.aggregateMessages - preAgg1")

    preAgg1.collect().foreach{ msg => msg._2.foreach(println); println }
    */

    val preAgg = aggEdges.partitionsRDD.map {

      case (pid, edgePartition) =>
        // Choose scan method
        // val activeFraction = edgePartition.numActives.getOrElse(0)
        // / edgePartition.indexSize.toFloat
        val msgs = activeDirectionOpt match {
          case Some(EdgeDirection.Both) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Both)
          case Some(EdgeDirection.Either) =>
            println("EdgeScan")
            // TODO: Because we only have a clustered index on the source vertex ID, we can't filter
            // the index here. Instead we have to scan all edges and then do the filter.
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Either)
          case Some(EdgeDirection.Out) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.SrcOnly)
          case Some(EdgeDirection.In) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.DstOnly)
          case _ => // None
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Neither)
        }
        // println("Get PreAgg")
        // (pid, msgs)
        // msgs: vertices generated with local vids
        (pid, edgePartition.filterRemoteMsgs(msgs))

    }.cache()

      // .setName("GraphImpl.aggregateMessages - preAgg")
    // preAgg store all the aggregated msgs before repartition
    // val haveToSendMsg = preAgg


    // preAgg.collect().foreach(println)

    // preAgg = preAgg.map(msg => (msg._1, msg._2))


    val remoteMsgs = preAgg.flatMap { msgs =>
      msgs._2.globalMirrorMsgs
    }.groupByKey

    /*
    val remoteMsgs = preAgg.mapPartitions(_.flatMap(_._2.globalMirrorMsgs))
        .forcePartitionBy(new HashPartitioner(edges.getNumPartitions))
        .mapPartitions {}
    */

    val localMasterMsgs = preAgg.flatMap { msgs =>
      Iterator((msgs._1, msgs._2.localMasterMsgs))
    }

    // localMasterMsgs.cache()

    // TODO: is it necessary to do aggregation here for mirrors?
    // shuffledMsgs may be empty
    // val shuffledMsgs = remoteMsgs.

    // TODO: do not consider the aggregation between mirror and master msgs
    val localFinalMsgs = localMasterMsgs.zipPartitions(remoteMsgs) { (masterMsgs, mirrorMsgs) =>
      if (mirrorMsgs.hasNext && masterMsgs.hasNext) {
        val (pid, allMasterPart) = masterMsgs.next()
        val (_, allMirrorPart) = mirrorMsgs.next()
        Iterator ((pid, new VertexAttrBlock ((allMasterPart ++ allMirrorPart).toArray)
          .mergeMsgs(mergeMsg)))
      } else if (!mirrorMsgs.hasNext && masterMsgs.hasNext) {
        val (pid, allMasterPart) = masterMsgs.next()
        Iterator ((pid, new VertexAttrBlock (allMasterPart.toArray).mergeMsgs(mergeMsg)))
      } else if (mirrorMsgs.hasNext && ! masterMsgs.hasNext) {
        val (pid, allMirrorPart) = mirrorMsgs.next()
        Iterator ((pid, new VertexAttrBlock (allMirrorPart.toArray).mergeMsgs(mergeMsg)))
      } else {
        Iterator.empty
      }
    }

    // if the msg for one partition is empty, no need to keep
    // currently do not consider this problem, for easy leftJoin
    // localFinalMsgs.filter(! _._2.msgs.isEmpty)
    // localFinalMsgs.cache()

    // val temp = localFinalMsgs.cache()
    // temp.foreachPartition{ iter => iter.foreach(t => t._2.msgs.foreach(println)); println}
    localFinalMsgs
  }

  override def cache(): GraphImpl[ED, VD] = {
    this.edges.cache()
    this
  }

  def withEdgeRDD[VD2: ClassTag](newEdges: EdgeRDDImpl[ED, VD2]): GraphImpl[ED, VD2] = {
    new GraphImpl(newEdges)
  }
}

object GraphImpl {

  def diff[VD: ClassTag](vertices: RDD[(PartitionID, LocalMastersWithMask[VD])],
    others: RDD[(PartitionID, LocalMastersWithMask[VD])]):
  RDD[(PartitionID, LocalMastersWithMask[VD])] = {
    vertices.zipPartitions(others) {
      (vertPartIter, otherPartIter) =>
        val (pid, vertPart) = vertPartIter.next()
        val (_, otherPart) = otherPartIter.next()
        Iterator((pid, EdgePartition.diff(vertPart, otherPart)))
    }
  }

  def buildSimpleFromEdges[ED: ClassTag](edges: RDD[Edge[ED]]):
  RDD[(Int, SimpleEdgePartition[ED])] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED]
      iter.foreach { e =>
        builder.add(e)
      }
      Iterator((pid, builder.toEdgePartition))
    }
    edgePartitions
  }

  def getVertRNum(graph: GraphImpl[_, _]): Double = {
    val totalNum = graph.edges.getReplicatedVerticesNum
    println("total: " + totalNum)
    val vertNum = graph.vertices.count()
    println("vertNum: " + vertNum)
    totalNum / vertNum
  }


  def apply[ED: ClassTag, VD: ClassTag](
    edges: RDD[Edge[ED]],
    defaultVertexAttr: VD,
    edgeStorageLevel: StorageLevel,
    vertexStorageLevel: StorageLevel):
    GraphImpl[ED, VD] = {
    GraphImpl.fromEdgesSimple(buildSimpleFromEdges(edges),
      edges.getNumPartitions, defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  /*
   * use a partitioner to repartition the simpleEdgePartition
  */
  def partitionSimplePartitions[ED: ClassTag](
    edges: RDD[(Int, SimpleEdgePartition[ED])],
    numParts: Int,
    edgePartitioner: String):
    RDD[(Int, SimpleEdgePartition[ED])] = {
    val partitioner = edgePartitioner match {
      case "EdgePartition1D" => new IngressEdgePartition1D(numParts)
      case "EdgePartition2D" => new IngressEdgePartition2D(numParts)
      case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: "
        + edgePartitioner)
    }

    partitioner.fromEdges(edges)
  }

  def partitionLHPartitions[ED: ClassTag](
    edges: RDD[(Int, SimpleEdgePartition[ED])],
    numParts: Int,
    threshold: Int):
  RDD[(Int, SimpleEdgeWithVertexPartition[ED])] = {
    val partitioner = new IngressBiDiPartition(numParts, threshold)

    partitioner.fromEdgesWithVertices(edges)
  }

  type MasterPositions = (VertexId, Iterable[PartitionID])

  def edgePartitionToMsgs(pid: Int, edgePartition: SimpleEdgePartition[_])
  : Iterator[(VertexId, PartitionID)] = {
    // Determine which positions each vertex id appears in using a map where the low 2 bits
    // represent src and dst
    // edgePartition.vertexIterator.map { vid => (vid, pid) }
    val vertices = new OpenHashSet[VertexId]
    edgePartition.edges.foreach { e =>
      vertices.add(e.srcId)
      vertices.add(e.dstId)
    }

    vertices.iterator.map( vid => (vid, pid) )
  }

  /*
  def fromEdgesSimpleWithVerts[ED: ClassTag, VD: ClassTag]
  (graph: RDD[(Int, SimpleEdgeWithVertexPartition[ED])],
    numPartitions: Int, defaultVertexAttr: VD,
    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY):
  GraphImpl[ED, VD] = {

  }
  */

  // using a hash partitioner to distribute vertices
  def fromEdgesSimple[ED: ClassTag, VD: ClassTag]
    (edges: RDD[(Int, SimpleEdgePartition[ED])],
      numPartitions: Int, defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY):
  GraphImpl[ED, VD] = {
    println(numPartitions)
    val vertexPartitioner = new HashPartitioner(numPartitions)
    // for each edge partition, get all the vertices in this partition
    // for each v in edgePart ep, v => (vpid, (v, pid))
    // repartition v2p, in each vertex partition,
    // we have all the epids for each vertex
    // construct vertex array for each epid
    val v2p = edges.mapPartitions(_.flatMap(Function.tupled(edgePartitionToMsgs)))
      .forcePartitionBy(vertexPartitioner)
    // v2p.count

    // v2p.foreach(println)
    // get all the masters
    val routingTable = v2p.mapPartitionsWithIndex { (vpid, iter) =>
      val pid2vid = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
      val masters = new OpenHashSet[VertexId]
      val masterPid = vpid

      for (msg <- iter) {
        val vid = msg._1
        val pid = msg._2
        pid2vid (pid) += vid
        masters.add (vid)
        // println(s"pid: $vpid, vid: $vid")
      }

      val p2v = pid2vid.map { vids => vids.trim ().array}
      // p2v.foreach{ vid => print("mirrors: "); vid.iterator.foreach(m => print(s"$m ")); println}
      p2v.update (masterPid, masters.iterator.toArray)
      Iterator((vpid, p2v))
    }

    // routingTable.count()

    val finalEdgePartitions = edges.zipPartitions(routingTable) {
      (edgePartIter, routingTableIter) =>
        val (pid, edgePart) = edgePartIter.next()
        val (_, routingTable) = routingTableIter.next()
        // val routingTable = vertexPart._1
        // val mirrors = vertexPart._2
        Iterator((pid, finalEdgePartitionWithoutMirrors(pid, numPartitions,
          defaultVertexAttr, routingTable, edgePart)))
    }.cache()
    // finalEdgePartitions.count()

    new GraphImpl(new EdgeRDDImpl(finalEdgePartitions))
  }

  def finalEdgePartitionWithoutMirrors[ED: ClassTag, VD: ClassTag](pid: PartitionID,
    numParts: Int,
    defaultVertexAttr: VD,
    mastersRouteTable: Array[Array[VertexId]],
    edgePart: SimpleEdgePartition[ED]): EdgePartition[ED, VD] = {

    // 1. build the global2local map using masters and mirrors
    // compute the mirrors from edgePartition
    val vertices = new OpenHashSet[VertexId]
    edgePart.edges.foreach { e =>
      vertices.add(e.srcId)
      vertices.add(e.dstId)
    }
    val vertexPartitioner = new HashPartitioner(numParts)

    val mirrors = vertices.iterator
      .map( vid => (vid, vertexPartitioner.getPartition(vid)) )
      .filter(_._2 != pid).toArray
    // mirrors.foreach { kv => print(s"pid: $pid, (${kv._1}, ${kv._2}) "); println}

    finalEdgePartition(pid, numParts, defaultVertexAttr,
      mastersRouteTable, mirrors, edgePart)
  }


  def finalEdgePartition[ED: ClassTag, VD: ClassTag](pid: PartitionID,
    numParts: Int,
    defaultVertexAttr: VD,
    mastersRouteTable: Array[Array[VertexId]],
    mirrors: Array[(VertexId, PartitionID)],
    edgePart: SimpleEdgePartition[ED]): EdgePartition[ED, VD] = {

    // 1. build the global2local map using masters and mirrors
    val edges = edgePart.edges
    // array for local src vertices storage
    val localSrcIds = new Array[Int](edges.length)
    // array for local dst vertices storage
    val localDstIds = new Array[Int](edges.length)

    // val localMasters = new Array[Iterable[PartitionID]](masters.size)

    val mirrorSize = mirrors.length
    val masterSize = mastersRouteTable(pid).length
    // println("Mirror Size: "+ mirrorSize + " Master Size: " + masterSize)

    val localMirrors = new Array[PartitionID](mirrorSize)

    var currLocalId = -1
    // array for edge attr storage
    val data = new Array[ED](edges.length)

    // store the edge number of each src
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    // map for global to local
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]

    // array for local to global
    val local2global = new PrimitiveVector[VertexId]
    var vertexAttrs = Array.empty[VD]
    // first add masters into the global2local map and local2global array
    // val masterSet =
    /*
    var masters = Array.empty[VertexId]

    if (!mastersRouteTable.isEmpty) {
      masters = mastersRouteTable(pid)
    }
    */


    mastersRouteTable(pid).foreach(v => global2local.changeValue(v,
       { currLocalId += 1; local2global += v; currLocalId }, identity))

    mirrors.foreach(v => global2local.changeValue(v._1, {
        currLocalId += 1; local2global += v._1; currLocalId
      }, identity))

    // mastersRouteTable(pid).foreach(v => localMasters(global2local(v)) = v)

    // mirrors.foreach(v => localMirrors(global2local(v._1)))

    mirrors.foreach { v =>
      localMirrors((global2local(v._1)) - mastersRouteTable(pid).length) = v._2 }

    mastersRouteTable.update(pid, Array.empty)

    println("global2local: " + global2local.size)

    val tempLocalMasters = Array.fill(numParts)(new PrimitiveVector[Int])
    println("numParts: " + numParts)

    for (i <- 0 until numParts) {
      for (vid <- mastersRouteTable(i)) {
        tempLocalMasters(i) += global2local(vid)
      }
    }

    val localMasters = tempLocalMasters.map(_.trim().array)

    if (edges.length > 0) {
      index.update(edges(0).srcId, 0)
      var currSrcId: VertexId = edges(0).srcId
      var i = 0
      while (i < edges.length) {
        val srcId = edges(i).srcId
        val dstId = edges(i).dstId
        localSrcIds(i) = global2local(srcId)
        localDstIds(i) = global2local(dstId)
        data(i) = edges(i).attr
        if (srcId != currSrcId) {
          currSrcId = srcId
          index.update(currSrcId, i)
        }
        i += 1
      }

      // vertexAttrs = new Array[VD](currLocalId + 1)
      // initialize vertex attrs
      vertexAttrs = Array.fill[VD](currLocalId + 1)(defaultVertexAttr)
    }

    // vertexAttrs.foreach(println)
    /*
    val pid2masterId = Array.fill(numParts)(new PrimitiveVector[VertexId])
    for (master <- masters) {
      val vid = master._1
      val pids = master._2
      for (pid <- pids)
        pid2masterId(pid) += vid
    } */
    // val mastersWithPid = pid2masterId.map(v => v.trim().array)


    // new added: add mask for master vertices
    // do not need a extra map
    // val masterMap = new GraphXPrimitiveKeyOpenHashMap[Int, VD]

    // val masterAttrs = vertexAttrs.slice(0, masterSize)
    val masters = (0 until masterSize)

    val masterMask = new BitSet(masterSize)
    masterMask.setUntil(masterSize)

    new EdgePartition(localSrcIds, localDstIds, data,
        index, localMasters, localMirrors,
        global2local, local2global.trim().array,
        vertexAttrs.slice(0, masterSize),
        vertexAttrs.slice(masterSize, currLocalId + 1), masterMask, numParts, None)
  }
}
