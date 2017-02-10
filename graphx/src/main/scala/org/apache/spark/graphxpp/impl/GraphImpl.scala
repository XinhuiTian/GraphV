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

import org.apache.spark.graphxpp.PartitionStrategy
import org.apache.spark.graphxp.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphxpp.impl.GraphImpl.MasterPositions
import org.apache.spark.util.collection.{BitSet, OpenHashMap, OpenHashSet, PrimitiveVector}
import org.apache.spark.{HashPartitioner, graphxpp}
import org.apache.spark.graphxpp._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created by XinhuiTian on 16/11/28.
 */
// record the raw data of edges, convenient for later partition
class SimpleEdgePartition[ED: ClassTag]
(val edges: Iterator[Edge[ED]]) {
  // val vertexIterator = edges.flatMap(e => e.srcId :: e.dstId :: Nil)
  val edgeArray = edges.toArray
  def vertexIterator: Iterator[VertexId] = edges.flatMap(e => e.srcId :: e.dstId :: Nil)
}

object SimpleEdgePartition {
  def fromEdges[ED: ClassTag](edges: RDD[Edge[ED]]):
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
}

/*
class RoutingTable(val masters: Iterable[MasterPositions],
  val mirrors: Iterable[(VertexId, PartitionID)]) {

}
*/
class AggregateMsg[A: ClassTag](val masterMsgs: RDD[(PartitionID, Iterator[(Int, A)])],
  val mirrorMsgs: RDD[(PartitionID, Iterator[(VertexId, A)])]) {
}

class LocalMsg[A: ClassTag](val localMasterMsgs: Iterator[(Int, A)],
  val localMirrorMsgs: Iterator[(VertexId, A)])

class VertexAttrBlock[A: ClassTag](val msgs: Iterator[(VertexId, A)]) extends Serializable {
  def mergeMsgs(reduceFunc: (A, A) => A): VertexAttrBlock[A] = {
    val vertexMap = new GraphXPrimitiveKeyOpenHashMap[VertexId, A]
    msgs.foreach { msg => vertexMap.setMerge(msg._1, msg._2, reduceFunc)}
    new VertexAttrBlock(vertexMap.toIterator)
  }


  override def clone(): VertexAttrBlock[A] = {
    // val newMsgs = new PrimitiveVector[(VertexId, A)]
    val newMsgs = msgs.toArray.clone()
    new VertexAttrBlock(newMsgs.iterator)
  }

}

case class MasterTriplet(vid: VertexId, master: PartitionID,
  mirrors: Option[Iterable[PartitionID]])

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
  override def upgrade = {
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
    joinMasterAttrs(msgs, withActives)(mapFunc)
  }

  def joinMasterAttrs[A: ClassTag](msgs: RDD[(PartitionID, VertexAttrBlock[A])],
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
      val newVertexAttr = edges.leftJoin (msgs, withActives)(updateF).localMastersWithAttrs
      val newEdgesWithDiffAttrs = edges.asInstanceOf[EdgeRDDImpl[ED, VD2]].diff(newVertexAttr)
      val newEdges = edges.asInstanceOf[EdgeRDDImpl[ED, VD2]].updateVertices
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
        Iterator ((pid, new VertexAttrBlock (allMasterPart ++ allMirrorPart).mergeMsgs(mergeMsg)))
      } else if (!mirrorMsgs.hasNext && masterMsgs.hasNext) {
        val (pid, allMasterPart) = masterMsgs.next()
        Iterator ((pid, new VertexAttrBlock (allMasterPart).mergeMsgs(mergeMsg)))
      } else if (mirrorMsgs.hasNext && ! masterMsgs.hasNext) {
        val (pid, allMirrorPart) = mirrorMsgs.next()
        Iterator ((pid, new VertexAttrBlock (allMirrorPart.iterator).mergeMsgs(mergeMsg)))
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

  def apply[ED: ClassTag, VD: ClassTag](
    edges: RDD[Edge[ED]],
    defaultVertexAttr: VD,
    edgeStorageLevel: StorageLevel,
    vertexStorageLevel: StorageLevel): GraphImpl[ED, VD] = {
    GraphImpl.fromEdges(SimpleEdgePartition.fromEdges(edges),
      edges.getNumPartitions, defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  type MasterPositions = (VertexId, Iterable[PartitionID])

  // using a hash partitioner to distribute vertices
  def fromEdgesSimple[ED: ClassTag, VD: ClassTag]
    (edges: RDD[(Int, SimpleEdgePartition[ED])], numPartitions: Int): Unit = {
    val vertexPartitioner = new HashPartitioner(numPartitions)
    // for each edge partition, get all the vertices in this partition
    // for each v in edgePart ep, v => (vpid, (v, pid))
    // repartition v2p, in each vertex partition,
    // we have all the epids for each vertex
    // construct vertex array for each epid
    val v2p = edges.mapPartitions(_.flatMap{ part =>
      val pid = part._1
      // vertexId + edgePartitionID + vertexPartitionID
      part._2.vertexIterator.map(vid => (vid, pid))
    }).forcePartitionBy(vertexPartitioner)
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
      }

      val p2v = pid2vid.map { vids => vids.trim ().array}
      p2v.update (masterPid, masters.iterator.toArray)
      Iterator((vpid, p2v))
    }

    // determine the mirrors for each edge partition
    val mirrorsPerPart = routingTable.mapPartitionsWithIndex{ (masterPid, iter) =>
      iter.flatMap { table =>
        // val mirrorPid = masterPid
        val routingTable = table._2
        val mirrors = routingTable.zipWithIndex
          .flatMap {case (vids, mirrorPid) =>
            if (masterPid != mirrorPid) {
              vids.map (vid => (mirrorPid, (vid, masterPid)))
            } else {
              Array.empty [(Int, (Long, Int))]
            }
          }

        mirrors
      }
    }.forcePartitionBy(vertexPartitioner)
      .mapPartitionsWithIndex {(pid, iter) =>
        // var pid = -1
        val mirrorVids = iter.map(_._2)
        Iterator((pid, mirrorVids.toArray))
      }

    val masterWithMirrors = routingTable.zipPartitions(mirrorsPerPart) {
      (tablePart, mirrorsPart) =>
        tablePart.map(table => (table._1, (table._2, mirrorsPart.flatMap(_._2).toArray)))
    }

    val finalEdgePartitions = edges.zipPartitions(masterWithMirrors) {
      (edgePart, vertexPart) =>

    }


  }

  // TODO: currently use two repartition phases to compute the master position
  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[(Int, SimpleEdgePartition[ED])],
    numPartitions: Int, defaultVertexAttr: VD, edgeStorageLevel: StorageLevel,
    vertexStorageLevel: StorageLevel): GraphImpl[ED, VD] = {
    // 1. get vertices with partitionID, same as creating VertexRDD in GraphX
    // TODO: any good optimization idea?
    // val v2p = edges.flatMap(Function.tupled(this.edgePartitionToMsgs)).groupByKey
    // val v2p = edges.mapPartitions(_.flatMap(Function.tupled(edgePartitionToMsgs))).groupByKey()
    val vertexPartitioner = new HashPartitioner(numPartitions)
    val v2p = edges.mapPartitions(_.flatMap(Function.tupled(edgePartitionToMsgs)))
      .forcePartitionBy(vertexPartitioner).mapPartitions {
      iter =>
        partitionTableFromMsgs (numPartitions, iter)
    }

    // 2. get the master pid and mirror map
    // (vid, pids) => (pid, (vid, pids)s )

    // val routingTable = buildRoutingTables(numPartitions, v2p)

    v2p.count()

    /*
    v2p.collect.foreach { part =>
      println("PartitionID: " + part._1)
      part._2._1 match {
        case None => println
        case _ => part._2._1.get.foreach{ a => a.toIterator.foreach(println); println }
      }
    } */

    /* TODO: the structure is too complex, need to be simplified */
    val routingTable = v2p.forcePartitionBy(vertexPartitioner).reduceByKey({
      (v1, v2) =>
        if (v1._1 != None) {
          (v1._1, v1._2 ++ v2._2)
        } else {
          (v2._1, v1._2 ++ v2._2)
        }
    }, numPartitions)


    /*
    routingTable.collect.foreach { part =>
      println("PartitionID: " + part._1)
      part._2._1 match {
        case None => println
        case _ => part._2._1.get.foreach{ a => a.toIterator.foreach(println); println }
      }
    }

    routingTable.collect.foreach { part =>
      println("PartitionID: " + part._1)
      part._2._1 match {
        case None => println
        case _ => part._2._2.foreach(println)
      }
      println
    }
    */
    // routingTable.count()

    // println("Mirror size")
    // routingTable.foreach(part => println(part._2.mirrors.isEmpty))
    // val mirrors = masters.flatMap(Function.tupled(this.setMirror)).groupByKey

    // 4. now we get the masters and mirrors of each edgePartition
    // begin to build the final edgePartition

    val finalEdges = routingTable.zipPartitions(edges) {
      (routingTableIter, edgeIter) =>
      val (pid, rtPart) = routingTableIter.next()
      val (_, edgePart) = edgeIter.next()
      Iterator((pid, finalEdgePartition(pid,
        numPartitions, defaultVertexAttr, rtPart._1.getOrElse(Array.empty), rtPart._2, edgePart)))
    }

    new GraphImpl(EdgeRDD.fromEdgePartitions(finalEdges))
  }

  /** Generate a `v2p` key-value array for each vertex referenced in `edgePartition`. '
   *  return a iterator containing the partitions of all vertices
   * */
  def edgePartitionToMsgs(pid: Int, edgePartition: SimpleEdgePartition[_])
  : Iterator[(VertexId, PartitionID)] = {
    // Determine which positions each vertex id appears in using a map where the low 2 bits
    // represent src and dst
    // edgePartition.vertexIterator.map { vid => (vid, pid) }
    val vertices = new OpenHashSet[VertexId]
    edgePartition.edgeArray.foreach { e =>
      vertices.add(e.srcId)
      vertices.add(e.dstId)
    }

    vertices.iterator.map( vid => (vid, pid) )
  }

  def partitionTableFromMsgs(numPartitions: Int, iter: Iterator[(Long, Int)]):
    Iterator[(PartitionID, (Option[Array[Array[VertexId]]], Array[(VertexId, PartitionID)]))] = {
    val pid2vid = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
    val masters = new OpenHashSet[VertexId]
    // val toMirrors = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
    // get the pid of this vertex partition
    val vertexPartitioner = new HashPartitioner(numPartitions)

      val first = iter.next ()
      val masterPid = vertexPartitioner.getPartition (first._1)
      // println("masterPid: " + masterPid)

      // val srcFlags = Array.fill(numPartitions)(new PrimitiveVector[Boolean])
      // val dstFlags = Array.fill(numPartitions)(new PrimitiveVector[Boolean])
      for (msg <- iter) {
        val vid = msg._1
        val pid = msg._2
        pid2vid (pid) += vid
        masters.add (vid)
      }
      pid2vid (first._2) += first._1
      masters.add (first._1)

      val p2v = pid2vid.zipWithIndex.map {case (vids, pid) => vids.trim ().array}
      // store all the masters of this edgePartition into the masterPid's value
      p2v.update (masterPid, masters.iterator.toArray)
      // val masterMsg = p2v(masterPid).map (vid => (vid, masterPid))

      val partitionMsgs = Iterator.tabulate (numPartitions) {pid =>
        // val masterMsg = p2v(pid).map(v => (v, masterPid))
        if (pid == masterPid) {
          // println("masterPid!")

          (pid, (Some (p2v), Array.empty [(VertexId, PartitionID)]))
        }
        else {
          // println("mirrorCase!")
          val masterMsg = p2v (pid).map (v => (v, masterPid))
          (pid, (None, masterMsg))
        }
      }
      /*
    partitionMsgs.foreach { part => println(part._1)
      part._2._2.foreach(println)
      println
    } */
      partitionMsgs
  }


  // select the master pid for each vertex, create the
  def chooseMaster(repls: RDD[(VertexId, Iterable[PartitionID])])
  : RDD[(PartitionID, MasterTriplet)] = {
    /* by default, set the first pid as the master */

    // TODO: need rules to decide the master positions
    repls.flatMap {iter =>
      val newIter = Random.shuffle(iter._2)
      val masterPid = iter._2.head
      val masterVid = iter._1
      val mirrorPids = iter._2.tail
      if (mirrorPids.isEmpty) {
        // no mirrors, only one partition has this vertex
        Iterator ((masterPid, MasterTriplet (masterVid, masterPid, None)))
      } else {
        val mirrors = mirrorPids.map (mirror =>
          (mirror, MasterTriplet (masterVid, masterPid, None))).toSeq
        mirrors ++ Seq ((masterPid, MasterTriplet (masterVid, masterPid, Some (mirrorPids))))
          .toIterator
      }
    }
  }

  // for all the masters in one partition, get the mirrors for other partitions
  def setMirror(pid: PartitionID, masterMap: Iterable[(VertexId, Iterable[PartitionID])]):
  Iterable[(PartitionID, (VertexId, PartitionID))] = {
    val mirrors = masterMap.map(iter => ((iter._1, pid), iter._2))
       .filter(iter => !iter._2.isEmpty)
       .flatMap(mirrorPids => mirrorPids._2.map(pid => (pid, mirrorPids._1)))
    mirrors
  }

  // there are possibly partitions without mirrors, must maintain an empty position
  // 1-5 modified: remove one phase of shuffle for mirror info construction
  /*
  def buildRoutingTables(numPartitions: Int, v2p: RDD[(VertexId, Iterable[PartitionID])]):
    RDD[(Int, RoutingPartition)] = {
    // 1. build masterTable: partitioned by partitionID
    val vertexPartitions = this.chooseMaster(v2p).groupByKey.cache()

    val masterAndMirrors = vertexPartitions.map { iter =>
      val currentPid = iter._1
      val vertices = iter._2
      (currentPid, (
        // masters
        vertices.filter( v => v.master == currentPid).map { t =>
        if (t.mirrors == None) (t.vid, Iterable.empty) else (t.vid, t.mirrors.get)},
        // mirrors
        vertices.filter(v => v.master != currentPid).map(t => (t.vid, t.master)))
      )
    }

    // masters

    // mirrors may have empty element
    /*
    val mirrors = vertexPartitions.map { iter =>
      val currentPid = iter._1
      val vertices = iter._2
      (currentPid, vertices.filter(v => v.master != currentPid).map(t => (t.vid, t.master)))
    } */

    // mirrors

    /*
    val mirrors = masters.flatMap{ part =>
      val pid2mirrors = Array.fill(numPartitions)(new PrimitiveVector[(VertexId, PartitionID)])
      part._2.map(iter => ((iter._1, part._1), iter._2))
        .foreach { master =>
          val mirrorPids = master._2
          for (pid <- mirrorPids) {
            val masterPid = master._1
            pid2mirrors(pid) += masterPid
          }
        }
      val mirrors = Iterator.tabulate(numPartitions) { pid =>
        (pid, pid2mirrors(pid).toArray)
      }


      mirrors
    }
    */

    // val finalMirrors = mirrors.reduceByKey((part1, part2) => part1 ++ part2)

    /*
    val routingTables = masters.zipPartitions(mirrors) {
      (masterIter, mirrorIter) =>
        val (pid, masterPart) = masterIter.next()
        val (_, mirrorPart) = mirrorIter.next()
        Iterator((pid, new RoutingPartition(masterPart, mirrorPart)))
    }
    */
    val routingTables = masterAndMirrors.map {part =>
      (part._1, new RoutingPartition(part._2._1, part._2._2)
    )}
    routingTables
  }
  */

  def finalEdgePartition[ED: ClassTag, VD: ClassTag](pid: PartitionID,
    numParts: Int,
    defaultVertexAttr: VD,
    mastersRouteTable: Array[Array[VertexId]],
    mirrors: Array[(VertexId, PartitionID)],
    edgePart: SimpleEdgePartition[ED]): EdgePartition[ED, VD] = {

    // 1. build the global2local map using masters and mirrors
    val edges = edgePart.edgeArray
    // array for local src vertices storage
    val localSrcIds = new Array[Int](edges.length)
    // array for local dst vertices storage
    val localDstIds = new Array[Int](edges.length)

    // val localMasters = new Array[Iterable[PartitionID]](masters.size)

    val localMirrors = new Array[PartitionID](mirrors.size)

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

    mirrors.foreach { v => localMirrors((global2local(v._1)) - mastersRouteTable(pid).length) = v._2 }

    mastersRouteTable.update(pid, Array.empty)

    val tempLocalMasters = Array.fill(numParts)(new PrimitiveVector[Int])
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

    val mirrorSize = mirrors.length
    val masterSize = vertexAttrs.length - mirrorSize


    // new added: add mask for master vertices
    // do not need a extra map
    // val masterMap = new GraphXPrimitiveKeyOpenHashMap[Int, VD]

    // val masterAttrs = vertexAttrs.slice(0, masterSize)
    val masters = (0 until masterSize)

    val masterMask = new BitSet(masterSize)
    masterMask.setUntil(masterSize)

    new EdgePartition(localSrcIds, localDstIds, data,
        index, localMasters, localMirrors, global2local,
        local2global.trim().array,  vertexAttrs.slice(0, masterSize),
        vertexAttrs.slice(masterSize, currLocalId + 1), masterMask, numParts, None)
  }
}
