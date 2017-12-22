
package org.apache.spark.graphv.enhanced

import scala.reflect.ClassTag

import org.apache.spark.graphv._
import org.apache.spark.graphv.util.GraphVAppendOnlyMap
import org.apache.spark.HashPartitioner
import org.apache.spark.internal.Logging
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{CompactBuffer, OpenHashSet}

class GraphImpl[VD: ClassTag, ED: ClassTag] protected(
    @transient val partitionsRDD: RDD[(Int, GraphPartition[VD, ED])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends Graph[VD, ED](partitionsRDD.context,
    List (new OneToOneDependency (partitionsRDD))) with Serializable with Logging {

  override def getActiveNums: Long = partitionsRDD.mapPartitions { iter =>
    val (pid, part) = iter.next()
    // println(part.getActiveNum)
    Iterator(part.getActiveNum)
  }.reduce(_ + _)

  override def activateAllMasters: GraphImpl[VD, ED] = {
    this.withPartitionsRDD(partitionsRDD.mapPartitions { partIter =>
      val (pid, part) = partIter.next()
      Iterator((pid, part.activateAllMasters))
    })
  }

  override def vertices: RDD[(VertexId, VD)] = {
    partitionsRDD.mapPartitions(partIter => partIter.next()._2.iterator)
  }

  override def edges: RDD[Edge[ED]] = {
    partitionsRDD.mapPartitions(partIter => partIter.next()._2.edgeIterator)
  }

  override def edgeSize: Int = partitionsRDD
    .mapPartitions(partIter => Iterator(partIter.next()._2.edgeSize))
    .sum().toInt

  override def mapVertices[VD2: ClassTag](
      f: (VertexId, VD) => VD2,
      needActive: Boolean = false): Graph[VD2, ED] = {
    val newParts = partitionsRDD.map { part =>
      val pid = part._1
      val newPart = part._2.mapVertices(f, needActive)
      (pid, newPart)
    }
    new GraphImpl(newParts)
  }

  override def mapTriplets[ED2: ClassTag](
      func: GraphVEdgeTriplet[VD, ED] => ED2,
      tripletFields: TripletFields): GraphImpl[VD, ED2] = {

    var newGraph = this
    // println("Whether use src: " + tripletFields.useSrc)
    if (tripletFields.useSrc) {
      println("Use Src")
      newGraph = this.syncSrcMirrors
      // newGraph.edges.foreach(println)
    }

    this.withPartitionsRDD(newGraph.partitionsRDD.mapPartitions { partIter =>
      val (pid, part) = partIter.next()
      Iterator((pid, part.mapTriplets(func)))
    })
  }

  override val partitioner =
    partitionsRDD.partitioner.orElse(Some(new HashPartitioner(partitions.length)))

  override def cache(): this.type = {
    partitionsRDD.persist()
    this
  }

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  override def syncSrcMirrors: GraphImpl[VD, ED] = {
    val syncMsgs = partitionsRDD.mapPartitions { partIter =>
      val (pid, part) = partIter.next()
      part.generateSrcSyncMessages
    }.partitionBy(partitioner.get)

    this.withPartitionsRDD(partitionsRDD.zipPartitions(syncMsgs) {
      (partIter, msgIter) =>
      val (pid, part) = partIter.next()
      Iterator((pid, part.syncSrcMirrors(msgIter)))
    })
  }

  def joinLocalMsgs[A: ClassTag](localMsgs: RDD[(Int, A)])(
      vFunc: (VertexId, VD, A) => VD): RDD[(VertexId, VD)] = {

    partitionsRDD.zipPartitions(localMsgs) { (partIter, localMsgs) =>
      val (pid, part) = partIter.next()
      part.joinLocalMsgs(localMsgs)(vFunc).masterIterator
    }
  }

  /*
  def localAggregate[A: ClassTag](localMsgs: RDD[(Int, A)])(
      reduceFunc: (A, A) => A): RDD[(Int, A)] = {

    partitionsRDD.zipPartitions(localMsgs) { (partIter, msgs) =>
      val (_, part) = partIter.next()
      part.localAggregate(msgs)(reduceFunc)
    }
  }


  def globalAggregate[A: ClassTag](localMsgs: RDD[LocalFinalMessages[A]])(
      reduceFunc: (A, A) => A): RDD[(VertexId, A)] = {

    partitionsRDD.zipPartitions(localMsgs) { (partIter, msgs) =>
      val (_, part) = partIter.next()
      part.globalAggregate(msgs)(reduceFunc)
    }
  }
  */

  override def aggregateGlobalMessages[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      edgeDirection: EdgeDirection,
      tripletFields: TripletFields,
      needActive: Boolean = true
  ): RDD[(VertexId, A)] = {
    aggregateMessages(sendMsg, mergeMsg,
      edgeDirection, tripletFields, needActive)
      .zipPartitions(partitionsRDD) { (msgIter, partIter) =>
      val (_, part) = partIter.next()
      msgIter.flatMap (msgs => msgs.iterator.map { m => (part.local2global(m._1), m._2)})
    }
    // globalAggregate(preAgg)(mergeMsg)
  }

  override def aggregateLocalMessages[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      edgeDirection: EdgeDirection,
      tripletFields: TripletFields,
      needActive: Boolean = true
  ): RDD[(Int, A)] = {
    aggregateMessages(sendMsg, mergeMsg,
      edgeDirection, tripletFields, needActive)
      .mapPartitions { msgIter =>
        msgIter.flatMap (_.iterator) }
  }

  override def aggregateMessages[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      edgeDirection: EdgeDirection,
      tripletFields: TripletFields,
      needActive: Boolean = true
  ): RDD[LocalFinalMessages[A]] = {
    edgeDirection match {
      case EdgeDirection.Out =>
        aggregateMessagesVertexCentric (sendMsg, mergeMsg,
          edgeDirection, tripletFields, needActive)
      case _ =>
        aggregateMessagesEdgeCentric (sendMsg, mergeMsg,
          edgeDirection, tripletFields, needActive)
      // case _ => throw new IllegalArgumentException("Not reachable")
    }
  }

  def aggregateMessagesVertexCentric[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      edgeDirection: EdgeDirection,
      tripletFields: TripletFields,
      needActive: Boolean = true): RDD[LocalFinalMessages[A]] = {
    val preAgg = partitionsRDD.mapPartitions { partIter =>
      val (pid, graphPart) = partIter.next()
      graphPart.aggregateMessagesVertexCentric(sendMsg, mergeMsg, tripletFields)
    }.partitionBy(new HashPartitioner(this.getNumPartitions)).cache()

    partitionsRDD.zipPartitions(preAgg) { (parts, aggMsgs) =>
      val (pid, part) = parts.next()
      Iterator(part.generateFinalMsgs(aggMsgs)(sendMsg, mergeMsg))
      // localMsgs.iterator
      // Iterator((pid, part.joinLocalMsgs(localMsgs)(vFunc)))
    }
  }

  def aggregateMessagesEdgeCentric[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      edgeDirection: EdgeDirection,
      tripletFields: TripletFields,
      needActive: Boolean = true): RDD[LocalFinalMessages[A]] = {
    println("numPartitions: " + this.getNumPartitions)
    val preAgg = partitionsRDD.mapPartitions { partIter =>
      val (pid, graphPart) = partIter.next()
      val msgs = graphPart.aggregateMessagesEdgeCentric(sendMsg,
        mergeMsg, edgeDirection, tripletFields)
      // msgs.foreach(println)
      msgs
    }.partitionBy(new HashPartitioner(this.getNumPartitions))

    partitionsRDD.zipPartitions(preAgg) { (parts, aggMsgs) =>
      val (pid, part) = parts.next()
      // aggMsgs.foreach(println)
      Iterator(part.generateLocalMsgs(aggMsgs)(mergeMsg))
      // localMsgs.iterator
      // Iterator((pid, part.joinLocalMsgs(localMsgs)(vFunc)))
    }
  }

  override def compute[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      vFunc: (VertexId, VD, A) => VD,
      edgeDirection: EdgeDirection,
      tripletFields: TripletFields,
      needActive: Boolean = false): Graph[VD, ED] = {
    edgeDirection match {
      case EdgeDirection.Out =>
        computeVertexCentric (sendMsg, mergeMsg, vFunc,
          edgeDirection, tripletFields, needActive)
      case _ =>
        computeEdgeCentric (sendMsg, mergeMsg, vFunc,
          edgeDirection, tripletFields, needActive)
    }
  }

  def computeVertexCentric[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      vFunc: (VertexId, VD, A) => VD,
      edgeDirection: EdgeDirection,
      tripletFields: TripletFields,
      needActive: Boolean = false): Graph[VD, ED] = {

    val startTime = System.currentTimeMillis()
    val preAgg = partitionsRDD.mapPartitions { partIter =>
      val (pid, graphPart) = partIter.next()
      // println("AggregateMessageVertexCentric")
      graphPart.aggregateMessagesVertexCentric(sendMsg, mergeMsg, tripletFields)
    }.partitionBy(new HashPartitioner(this.getNumPartitions))
    // preAgg.cache()

    // preAgg.count()

    // val msgTime = System.currentTimeMillis()

    // println("aggreMsgs: " + (msgTime - startTime))

    val newPartitionsRDD = partitionsRDD.zipPartitions(preAgg) { (parts, aggMsgs) =>
      val (pid, part) = parts.next()
      // println("GenerateFinalMsgs")
      val localMsgs = part.generateFinalMsgs(aggMsgs)(sendMsg, mergeMsg)
      // localMsgs.foreach(println)
      Iterator((pid, part.localLeftJoin(localMsgs, needActive)(vFunc)))
    }.cache()

    // newPartitionsRDD.count()

    // val endTime = System.currentTimeMillis()
    // println("join Time: " + (endTime - msgTime))
    new GraphImpl(newPartitionsRDD)
  }

  def computeEdgeCentric[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      vFunc: (VertexId, VD, A) => VD,
      edgeDirection: EdgeDirection,
      tripletFields: TripletFields,
      needActive: Boolean = true): Graph[VD, ED] = {
    val preAgg = partitionsRDD.mapPartitions { partIter =>
      val (pid, graphPart) = partIter.next()
      graphPart.aggregateMessagesEdgeCentric(sendMsg, mergeMsg, edgeDirection, tripletFields)
    }.partitionBy(new HashPartitioner(this.getNumPartitions))

    val newPartitionsRDD = partitionsRDD.zipPartitions(preAgg) { (parts, aggMsgs) =>
      val (pid, part) = parts.next()
      val localMsgs = part.generateLocalMsgs(aggMsgs)(mergeMsg)
      Iterator((pid, part.localLeftJoin(localMsgs)(vFunc)))
    }
    new GraphImpl(newPartitionsRDD)
  }

  override def degreeRDD(inDegree: Boolean): RDD[(VertexId, Int)] = {
    if (inDegree == true) {
      aggregateGlobalMessages(_.sendToDst(1), _ + _, EdgeDirection.Out, TripletFields.None)
    } else {
      aggregateGlobalMessages(_.sendToSrc(1), _ + _, EdgeDirection.In, TripletFields.None)
    }
  }

  override def localDegreeRDD(inDegree: Boolean): RDD[LocalFinalMessages[Int]] = {
    if (inDegree == true) {
      aggregateMessages(_.sendToDst(1), _ + _, EdgeDirection.Out, TripletFields.None)
    } else {
      aggregateMessages(_.sendToSrc(1), _ + _, EdgeDirection.In, TripletFields.None)
    }
  }

  override def localOuterJoin[VD2: ClassTag](
      other: RDD[LocalFinalMessages[VD2]],
      needActive: Boolean)(updateF: (VertexId, VD, VD2) => VD): Graph[VD, ED] = {

    val newPartitionsRDD = partitionsRDD.zipPartitions(other) { (partIter, otherIter) =>
      val (pid, part) = partIter.next()
      val finalMessages = otherIter.next()
      Iterator((pid, part.localLeftJoin(finalMessages, needActive)(updateF)))
    }

    this.withPartitionsRDD(newPartitionsRDD)
  }

  override def withPartitionsRDD[VD2: ClassTag, ED2: ClassTag](
      partitionsRDD: RDD[(Int, GraphPartition[VD2, ED2])]): GraphImpl[VD2, ED2] = {
    new GraphImpl(partitionsRDD)
  }
}

object GraphImpl {

  def buildRoutingTable[VD: ClassTag, ED: ClassTag](
      partitions: RDD[(PartitionID, GraphPartition[VD, ED])],
      useSrcMirror: Boolean = true,
      useDstMirror: Boolean = true)
  : GraphImpl[VD, ED] = {

    partitions.cache()
    val numPartitions = partitions.getNumPartitions

    val newPartitioner = partitions.partitioner
      .getOrElse(new HashPartitioner(numPartitions))
    println("Building Routing Table")

    val shippedMsgs = partitions.mapPartitions { partIter =>
      val (pid, part) = partIter.next()
      // println("smallDegreeSize largeDegreeStartPos largeDegreeEndPos"
      //   + part.smallDegreeSize + " " + part.largeDegreeStartPos + " " + part.largeDegreeEndPos
      //   + " " + part.totalVertSize)
      // part.remoteLDMirrorIterator.foreach(v => println("LDMirrors " + pid + " " + v))

      // part.remoteOutMirrorIterator.foreach(v => println("OutMirrors " + pid + " " + v))

      // part.neighborIterator.foreach(v => println("Neighbors " + pid + " " + v))

      // part.localNeighborIterator.foreach(v => println("LocalNeighbors " + pid + " " + v))
      RoutingTable.generateMessagesWithPos(pid, part, useSrcMirror, useDstMirror)
    }.partitionBy(newPartitioner)

    val finalPartitionsRDD = partitions.zipPartitions(shippedMsgs) { (partIter, msgIter) =>
      val (pid, part) = partIter.next()
      val routingTable = RoutingTable.fromMsgs(numPartitions, msgIter, part.global2local)
      Iterator((pid, part.withRoutingTable(routingTable)))
    }

    new GraphImpl(finalPartitionsRDD)
  }

  def buildGraph[VD: ClassTag, ED: ClassTag](
      edges: RDD[(VertexId, Iterable[VertexId])],
      defaultVertexValue: VD,
      useSrcMirror: Boolean = true,
      useDstMirror: Boolean = true): GraphImpl[VD, Int] = {

    val numPartitions = edges.getNumPartitions
    // remove -1s
    val newEdges = edges.map(v => (v._1, v._2.toArray)).map { v =>
      val tmpArray = v._2.filter(v => v != -1)
      if (tmpArray.length > 0) {
        (v._1, tmpArray) // having edges
      } else { // no edges
        (v._1, Array.empty[Long])
      }
    }
    newEdges.cache()
    // compute the mirror degree threshold
    val vertexSize = newEdges.count()
    val edgeSize = newEdges.flatMap(_._2).count()

    val degreeThreshold: Int = numPartitions *
       math.exp(edgeSize / (vertexSize * numPartitions)).toInt
    println("DegreeThreshold: " + degreeThreshold, edgeSize, vertexSize,
       math.exp(edgeSize / (vertexSize * numPartitions)).toInt)

    // val degreeThreshold = 100

    val partitioner = new HashPartitioner(numPartitions)

    // get the edges needed to be repartitioned
    val exchangeEdges = newEdges.filter(v => v._2.length > degreeThreshold)
      .flatMap { v =>
      val srcId = v._1
      v._2.map(dstId => ((dstId, srcId)))
    }.partitionBy(partitioner)

    val graphPartitions = exchangeEdges.mapPartitionsWithIndex { (pid, edges) =>
      val mirrorEdges = edges.map(e => (e._2, e._1))
      val createCombiner = (v: VertexId) => CompactBuffer(v)
      val mergeValue = (buf: CompactBuffer[VertexId], v: VertexId) => buf += v
      val mergeCombiners = (c1: CompactBuffer[VertexId], c2: CompactBuffer[VertexId]) => c1 ++= c2

      val mirrorMap = new GraphVAppendOnlyMap[
        VertexId, VertexId, CompactBuffer[VertexId]](
        createCombiner,
        mergeValue,
        mergeCombiners
      )
      mirrorMap.insertAll(mirrorEdges)
      Iterator((pid, mirrorMap.iterator.map { v =>
        val srcId = v._1
        val dstIds = v._2.toArray
        (srcId, dstIds)
      }))
    }.zipPartitions(newEdges) { (remoteEdgesIter, edges) =>
      val (pid, remoteEdges) = remoteEdgesIter.next
      val graphPartitionBuilder = new GraphPartitonBuilder[VD](
        degreeThreshold, numPartitions, defaultVertexValue)
      // println("localEdges: ")
      // edges.foreach(println)
      // println("remoteEdges: ")
      // remoteEdges.foreach(println)
      graphPartitionBuilder.add(edges, remoteEdges)
      Iterator((pid, graphPartitionBuilder.toGraphPartition))
    }


    // val vertexSize = graphPartitionBuilders.map(_.getVertexSize).sum()
    buildRoutingTable(graphPartitions, useSrcMirror, useDstMirror)
  }

  def fromEdgeList[VD: ClassTag](
      edgeList: RDD[(VertexId, Iterable[VertexId])],
      defaultVertexAttr: VD,
      useSrcMirror: Boolean = true,
      useDstMirror: Boolean = true,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel,
      enableMirror: Boolean = true): GraphImpl[VD, Int] = {
    if (enableMirror) {
      buildGraph(edgeList, defaultVertexAttr, useSrcMirror, useDstMirror)
    } else {
      throw new NotImplementedError()
    }
  }
}
