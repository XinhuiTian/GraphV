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

package org.apache.spark.graphx.impl

// scalastyle:off println
import java.lang.management.ManagementFactory
import java.util.concurrent.LinkedBlockingQueue

import scala.reflect.{classTag, ClassTag}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.HashPartitioner

import org.apache.spark.graphv.{MyGraph, MyGraphImpl}
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl._
import org.apache.spark.graphx.util.BytecodeUtils
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{RDDBlockId, StorageLevel, TestBlockId}
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector}

/**
 * An implementation of [[org.apache.spark.graphx.Graph]] to support computation on graphs.
 *
 * Graphs are represented using two RDDs: `vertices`, which contains vertex attributes and the
 * routing information for shipping vertex attributes to edge partitions, and
 * `replicatedVertexView`, which contains edges and the vertex attributes mentioned by each edge.
 */
class GraphImpl[VD: ClassTag, ED: ClassTag] protected (
    @transient val vertices: VertexRDD[VD],
    @transient val replicatedVertexView: ReplicatedVertexView[VD, ED])
  extends Graph[VD, ED] with Serializable {

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null)

  // TXH added
  var localIndexSet = false

  @transient override val edges: EdgeRDDImpl[ED, VD] = replicatedVertexView.edges

  /** Return a RDD that brings edges together with their source and destination vertices. */
  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    replicatedVertexView.upgrade(vertices, true, true)
    replicatedVertexView.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, part) => part.tripletIterator()
    })
  }

  override def upgrade: Graph[VD, ED] = {
    replicatedVertexView.upgrade(vertices, true, true)
    /* replicatedVertexView.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, part) => part.tripletIterator()
    }).foreach(println)
    */
    this
  }

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    vertices.persist(newLevel)
    replicatedVertexView.edges.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = {
    vertices.cache()
    replicatedVertexView.edges.cache()
    this
  }

  override def checkpoint(): Unit = {
    vertices.checkpoint()
    replicatedVertexView.edges.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    vertices.isCheckpointed && replicatedVertexView.edges.isCheckpointed
  }

  override def getCheckpointFiles: Seq[String] = {
    Seq(vertices.getCheckpointFile, replicatedVertexView.edges.getCheckpointFile).flatMap {
      case Some(path) => Seq(path)
      case None => Seq()
    }
  }

  override def unpersist(blocking: Boolean = true): Graph[VD, ED] = {
    unpersistVertices(blocking)
    replicatedVertexView.edges.unpersist(blocking)
    this
  }

  override def unpersistVertices(blocking: Boolean = true): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    // TODO: unpersist the replicated vertices in `replicatedVertexView` but leave the edges alone
    this
  }

  override def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = {
    partitionBy(partitionStrategy, edges.partitions.length)
  }

  override def partitionBy(
      partitionStrategy: PartitionStrategy, numPartitions: Int): Graph[VD, ED] = {
    val edTag = classTag[ED]
    val vdTag = classTag[VD]
    val newEdges = edges.withPartitionsRDD(edges.map { e =>
      val part: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)
      (part, (e.srcId, e.dstId, e.attr))
    }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex( { (pid, iter) =>
        val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
        iter.foreach { message =>
          val data = message._2
          builder.add(data._1, data._2, data._3)
        }
        val edgePartition = builder.toEdgePartition
        Iterator((pid, edgePartition))
      }, preservesPartitioning = true)).cache()
    GraphImpl.fromExistingRDDs(vertices.withEdges(newEdges), newEdges)
  }

  override def locAwarePartitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = {
    locAwarePartitionBy(partitionStrategy, edges.partitions.length)
  }

  override def locAwarePartitionBy(
      partitionStrategy: PartitionStrategy, numPartitions: Int): Graph[VD, ED] = {
    val edTag = classTag[ED]
    val vdTag = classTag[VD]
    val newEdges = edges.withPartitionsRDD(edges.map { e =>
      val part: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)
      (part, (e.srcId, e.dstId, e.attr))
    }
      .locAwarePartitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex( { (pid, iter) =>
        val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
        iter.foreach { message =>
          val data = message._2
          builder.add(data._1, data._2, data._3)
        }
        val edgePartition = builder.toEdgePartition
        Iterator((pid, edgePartition))
      }, preservesPartitioning = true)).cache()
    GraphImpl.fromExistingRDDs(vertices.withEdges(newEdges), newEdges)
  }

  override def reverse: Graph[VD, ED] = {
    new GraphImpl(vertices.reverseRoutingTables(), replicatedVertexView.reverse())
  }

  override def mapVertices[VD2: ClassTag]
    (f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      println("In mapVertices: VD == VD2")
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapVertexPartitions(_.map(f)).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      GraphImpl(vertices.mapVertexPartitions(_.map(f)), replicatedVertexView.edges)
    }
  }

  override def mapEdges[ED2: ClassTag](
      f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val newEdges = replicatedVertexView.edges
      .mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def mapTriplets[ED2: ClassTag](
      f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2],
      tripletFields: TripletFields): Graph[VD, ED2] = {
    vertices.cache()
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val newEdges = replicatedVertexView.edges.mapEdgePartitions { (pid, part) =>
      part.map(f(pid, part.tripletIterator(tripletFields.useSrc, tripletFields.useDst)))
    }
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def subgraph(
      epred: EdgeTriplet[VD, ED] => Boolean = x => true,
      vpred: (VertexId, VD) => Boolean = (a, b) => true): Graph[VD, ED] = {
    vertices.cache()
    // Filter the vertices, reusing the partitioner and the index from this graph
    val newVerts = vertices.mapVertexPartitions(_.filter(vpred))
    // Filter the triplets. We must always upgrade the triplet view fully because vpred always runs
    // on both src and dst vertices
    replicatedVertexView.upgrade(vertices, true, true)
    val newEdges = replicatedVertexView.edges.filter(epred, vpred)
    new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def mask[VD2: ClassTag, ED2: ClassTag] (
      other: Graph[VD2, ED2]): Graph[VD, ED] = {
    val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v }
    val newEdges = replicatedVertexView.edges.innerJoin(other.edges) { (src, dst, v, w) => v }
    new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    val newEdges = replicatedVertexView.edges.mapEdgePartitions(
      (pid, part) => part.groupEdges(merge))
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  override def aggregateMessagesWithActiveSet[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]): VertexRDD[A] = {

    /*
    if (localIndexSet == false) {
      println("CreateLocalIndex")
      replicatedVertexView.edges = createLocalIndex
      localIndexSet = true
    }
    */
    var startTime = 0L
    var endTime = 0L
    vertices.cache()
    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val view = activeSetOpt match {
      case Some((activeSet, _)) =>
        replicatedVertexView.withActiveSet(activeSet)
      case None =>
        replicatedVertexView
    }
    val activeDirectionOpt = activeSetOpt.map(_._2)

    // Map and combine.
    startTime = System.currentTimeMillis()
    /*
    println("BlockManagerId: " + view.edges.sparkContext.env.blockManager.blockManagerId.host)
    val blockManager = view.edges.sparkContext.env.blockManager
    val blockId = new TestBlockId("123")
    blockManager.putSingle(blockId, 123L, StorageLevel.MEMORY_ONLY)
    */
    /*
    val preAgg = view.edges.partitionsRDD.mapPartitionsWithIndexAndLocalShuffle((pid, blockManager, iter) =>
      iter.flatMap {
      case (pid, edgePartition) =>
        // Choose scan method
        val activeFraction = edgePartition.numActives.getOrElse(0) / edgePartition.indexSize.toFloat
        val aggMsgs = activeDirectionOpt match {
          case Some(EdgeDirection.Both) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            }
          case Some(EdgeDirection.Either) =>
            // TODO: Because we only have a clustered index on the source vertex ID, we can't filter
            // the index here. Instead we have to scan all edges and then do the filter.
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Either)
          case Some(EdgeDirection.Out) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            }
          case Some(EdgeDirection.In) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.DstOnly)
          case _ => // None
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Neither)
        }


        aggMsgs
    }).setName("GraphImpl.aggregateMessages - preAgg") */
    // preAgg.count()

    val executorCores = 12
    val slaves = vertices.sparkContext.getExecutorIds().size
    val numPartitions = vertices.getNumPartitions
    val preAgg = view.edges.partitionsRDD.mapPartitions(_.flatMap {
        case (pid, edgePartition) =>
          // Choose scan method
          val activeFraction =
            edgePartition.numActives.getOrElse(0) / edgePartition.indexSize.toFloat
          val preAggMsgs = activeDirectionOpt match {
            case Some(EdgeDirection.Both) =>
              if (activeFraction < 0.8) {
                edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                  EdgeActiveness.Both)
              } else {
                edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                  EdgeActiveness.Both)
              }
            case Some(EdgeDirection.Either) =>
              // TODO: Because we only have a clustered index on the source vertex ID,
              // we can't filter the index here.
              // Instead we have to scan all edges and then do the filter.
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Either)
            case Some(EdgeDirection.Out) =>
              if (activeFraction < 0.8) {
                edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                  EdgeActiveness.SrcOnly)
              } else {
                edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                  EdgeActiveness.SrcOnly)
              }
            case Some(EdgeDirection.In) =>
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.DstOnly)
            case _ => // None
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Neither)
          }
          preAggMsgs
          /*
          val executor = blockManager.blockManagerId.host
          val newPartitioner = new HashPartitioner(executorCores)
          val realId = (pid % numPartitions) / slaves
          val data = Array.fill(executorCores)(new PrimitiveVector[(VertexId, A)])
          preAggMsgs.foreach { msg =>
            val part = newPartitioner.getPartition(msg._1)
            data(part) += msg
          }

          for (i <- 0 until executorCores) {
            val blockId = new TestBlockId(s"${executor}_${realId}_$i")

            blockManager.putIterator(blockId, data(i).iterator, level = StorageLevel.MEMORY_ONLY)
              // println("Put block successfully: " + blockId)
          }
          Iterator((pid, executor))
          */
    }).setName("GraphImpl.aggregateMessages - preAgg")
    // val tmpAgg = preAgg.forcePartitionBy(new HashPartitioner(numPartitions))
    // println("preAgg msgs: " + preAgg.count())
    // endTime = System.currentTimeMillis()
    // println("PreAgg: " + (endTime - startTime))
    // TXH added

    // println("Slaves " + slaves)
    // println("local agg msgs: " + localAgg.count)

    /*
    val localAgg = preAgg.
      mapPartitionsWithIndexAndLocalShuffle { (pid, blockManager, iter) =>
      val executor = blockManager.blockManagerId.host


      val results = new GraphXPrimitiveKeyOpenHashMap[VertexId, A]
      //
      val realId = (pid % numPartitions) / slaves
      // println("Get Pid: " + pid + " " + executorCores)
      for (i <- 0 until executorCores) {
        // println("i: " + i)
        val blockId = new TestBlockId(s"${executor}_${i}_$realId")
        // println("Get block: " + blockId)

        // println("block status: " + blockManager.getStatus(blockId).get.memSize)
        blockManager.getLocalValues (blockId) match {
          case Some (blockResult) =>
            val msgBlock = blockResult.data.asInstanceOf [Iterator[(VertexId, A)]]
            //
            // println("Get block: " + msgBlock.size)
            msgBlock.foreach { msg => results.changeValue(msg._1, msg._2, v => mergeMsg(v, msg._2))}
            // blockManager.releaseLock(blockId)
            blockManager.removeBlock(blockId)
          case _ => throw new NotImplementedError ("Should not reach here " + blockId)
        }
      }
      results.iterator
    }
    */


    // do the final reduction reusing the index map
    startTime = System.currentTimeMillis()
    val startGcTime = computeGcTime()
    // TXH changed
    val finalVertices = vertices.aggregateUsingIndex(preAgg, mergeMsg)
    // finalVertices.count()
    endTime = System.currentTimeMillis()
    val endGcTime = computeGcTime()

    println("vertexAggre: " + (endTime - startTime))
    println("vertexAggre, GcTime: " + (endGcTime - startGcTime))
    finalVertices
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag]
      (other: RDD[(VertexId, U)])
      (updateF: (VertexId, VD, Option[U]) => VD2)
      (implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      println("outerJoinVertices: VD == VD2")
      vertices.cache()
      // updateF preserves type, so we can use incremental replication
      val st1 = System.currentTimeMillis()
      val newVerts = vertices.leftJoin(other)(updateF).cache()
      // newVerts.count()
      val et1 = System.currentTimeMillis()
      // println("get newVerts: " + (et1 - st1))
      val st2 = System.currentTimeMillis()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts).cache()
      // changedVerts.count()
      val et2 = System.currentTimeMillis()
      // println("get changedVerts: " + (et2 - st2))
      val st3 = System.currentTimeMillis()
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      // newReplicatedVertexView.edges.count()
      val et3 = System.currentTimeMillis()
      // println("updateVertices: " + (et3 - st3))
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(other)(updateF)
      GraphImpl(newVerts, replicatedVertexView.edges)
    }
  }

  /** Test whether the closure accesses the attribute with name `attrName`. */
  private def accessesVertexAttr(closure: AnyRef, attrName: String): Boolean = {
    try {
      BytecodeUtils.invokedMethod(closure, classOf[EdgeTriplet[VD, ED]], attrName)
    } catch {
      case _: ClassNotFoundException => true // if we don't know, be conservative
    }
  }

  override def inComRatio: (Double, Double) = {
    val newVerts = vertices
      .leftJoin(this.inDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }.cache()
    replicatedVertexView.asInstanceOf[ReplicatedVertexView[Int, ED]]
      .upgrade(newVerts, true, true)
    val numComVertices = replicatedVertexView.edges.partitionsRDD.mapPartitions { edgeIter =>
      val part = edgeIter.next()
      val globalInDegrees = part._2.vertexAttrs
      // globalOutDegrees.foreach(println)

      val localInDegrees = part._2.countingInEdges
      // localOutDegrees.zipWithIndex.map { id => println(part._1 + " " + id._2 +" " + id._1)}
      var count: Int = 0
      val numVerts = globalInDegrees.length
      (0 until numVerts).foreach { i =>
        // println(part._1 + " " + part._2.local2global(i) + " " + globalOutDegrees(i))
        if (globalInDegrees(i) == localInDegrees(i) && globalInDegrees(i) != 0) {
          // println(part._1 + " " + part._2.local2global(i) + " " + globalOutDegrees(i))
          count += 1
        }
      }
      Iterator(count)
    }.sum()

    val zeroVertices = newVerts.filter(vd => vd._2 == 0).count() * 1.0
    println(zeroVertices)

    // (numComVertices + zeroVertices) / vertices.count()
    val numVerts = vertices.count()
    // println(numVerts)
    // println(100 / 101)
    (numComVertices / numVerts, zeroVertices / numVerts)
  }

  override def outComRatio: (Double, Double) = {
    val newVerts = vertices
      .leftJoin(this.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }.cache()
    replicatedVertexView.asInstanceOf[ReplicatedVertexView[Int, ED]]
      .upgrade(newVerts, true, true)
    val numComVertices = replicatedVertexView.edges.partitionsRDD.mapPartitions { edgeIter =>
      val part = edgeIter.next()
      val globalOutDegrees = part._2.vertexAttrs
      // globalOutDegrees.foreach(println)

      val localOutDegrees = part._2.countingOutEdges
      // localOutDegrees.zipWithIndex.map { id => println(part._1 + " " + id._2 +" " + id._1)}
      var count: Int = 0
      val numVerts = globalOutDegrees.length
      (0 until numVerts).foreach { i =>
        // println(part._1 + " " + part._2.local2global(i) + " " + globalOutDegrees(i))
        if (globalOutDegrees(i) == localOutDegrees(i) && globalOutDegrees(i) != 0) {
          // println(part._1 + " " + part._2.local2global(i) + " " + globalOutDegrees(i))
          count += 1
        }
      }
      Iterator(count)
    }.sum()

    val zeroVertices = newVerts.filter(vd => vd._2 == 0).count() * 1.0

    // (numComVertices + zeroVertices) / vertices.count()
    val numVerts = vertices.count()
    (numComVertices / numVerts, zeroVertices / numVerts)

    /*
    graph.edges.partitionsRDD.foreachPartition { edgeIter =>
      val part = edgeIter.next()
      part._2.tripletIterator(true, true).foreach(println)
      // Iterator.empty
    } */
    // graph.triplets.count()

    // graph.vertices.foreach(println)

    /*
    val numComVertices = graph.edges.partitionsRDD.mapPartitions { edgeIter =>
      val part = edgeIter.next()
      val globalOutDegrees = part._2.vertexAttrs
      globalOutDegrees.foreach(println)

      val localOutDegrees = part._2.countingOutEdges
      // localOutDegrees.zipWithIndex.map { id => println(part._1 + " " + id._2 +" " + id._1)}
      var count: Int = 0
      val numVerts = globalOutDegrees.length
      (0 until numVerts).foreach { i =>
        // println(part._1 + " " + part._2.local2global(i) + " " + localOutDegrees(i))
        if (globalOutDegrees(i) == localOutDegrees(i)) {
          // println(part._1 + " " + part._2.local2global(i) + " " + globalOutDegrees(i))
          count += 1
        }
      }
      Iterator(count)
    }.sum()

    numComVertices / graph.vertices.count()
    */
  }

  // created by TXH
  override def toGraphV(): MyGraph[Int, Int] = {
    val edges = this.edges
    val numPartiton = this.edges.partitionsRDD.getNumPartitions
    // this.vertices.unpersist(true)
    val newEdges = edges.flatMap{ e =>
      Iterator((e.srcId, e.dstId), (e.srcId, -1L), (e.dstId, -1L))
    }

    val links = newEdges.groupByKey (new HashPartitioner (numPartiton))

    // links.cache()

    // println("Total edges: " + links.count())

    MyGraphImpl.fromEdgeList(links, defaultVertexAttr = 1,
      edgeStorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel = StorageLevel.MEMORY_ONLY)
  }


  def createLocalIndex: EdgeRDDImpl[ED, VD] = {
    // TXH added
    val executorCores = 12
    val slaves = vertices.sparkContext.getExecutorIds().size
    val newEdges = replicatedVertexView.edges.partitionsRDD.mapPartitionsWithIndexAndLocalShuffle {
      (pid, blockManager, iter) =>
        val (epid, edgePart) = iter.next()
        val executor = blockManager.blockManagerId.host
        val newPartitioner = new HashPartitioner(executorCores)
        val startPid = pid % slaves
        val data = Array.fill(executorCores)(new PrimitiveVector[(VertexId)])
        edgePart.local2global.foreach { v =>
          val part = newPartitioner.getPartition(v)
          data(part) += v
        }

        var id = startPid
        for (i <- 0 until executorCores) {
          val blockId = new TestBlockId(s"index_${executor}_${pid}_$id")
          // println(blockId)
          // (blockId)
          blockManager.putIterator(blockId, data(i).iterator, level = StorageLevel.MEMORY_ONLY)
          id += slaves
        }

        val results = new ArrayBuffer[Array[VertexId]]
        //
        id = startPid
        for (i <- 0 until executorCores) {
          val blockId = new TestBlockId(s"index_${executor}_${id}_$pid")
          // println(blockId)

          var flag = false
          while (flag == false) {
            blockManager.getLocalValues (blockId) match {
              case Some (blockResult) =>
                flag = true
                val msgBlock = blockResult.data.asInstanceOf [Iterator[(VertexId)]]
                results += msgBlock.toArray
                blockManager.removeBlock(blockId)
              // blockManager.releaseLock(blockId)
              case None =>
                flag = false // wait for the block
              // Thread.sleep(1)
              case _ => throw new NotImplementedError ("Should not reach here")
            }
          }
          id += slaves
        }

        val set = new OpenHashSet[VertexId]
        results.iterator.flatMap(_.toIterator).foreach { vid =>
          set.add(vid)
        }
        // println("Index for pid: " + epid + " " + set.capacity + " " + edgePart.local2global.length)

        Iterator((epid, edgePart.withIndex(set)))
    }
    edges.withPartitionsRDD(newEdges)

  }

} // end of class GraphImpl


object GraphImpl {

  /** Create a graph from edges, setting referenced vertices to `defaultVertexAttr`. */
  def apply[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    fromEdgeRDD(EdgeRDD.fromEdges(edges), defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  /** Create a graph from EdgePartitions, setting referenced vertices to `defaultVertexAttr`. */
  def fromEdgePartitions[VD: ClassTag, ED: ClassTag](
      edgePartitions: RDD[(PartitionID, EdgePartition[ED, VD])],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    fromEdgeRDD(EdgeRDD.fromEdgePartitions(edgePartitions), defaultVertexAttr, edgeStorageLevel,
      vertexStorageLevel)
  }

  /** Create a graph from vertices and edges, setting missing vertices to `defaultVertexAttr`. */
  def apply[VD: ClassTag, ED: ClassTag](
      vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    val edgeRDD = EdgeRDD.fromEdges(edges)(classTag[ED], classTag[VD])
      .withTargetStorageLevel(edgeStorageLevel)
    val vertexRDD = VertexRDD(vertices, edgeRDD, defaultVertexAttr)
      .withTargetStorageLevel(vertexStorageLevel)
    GraphImpl(vertexRDD, edgeRDD)
  }

  /**
   * Create a graph from a VertexRDD and an EdgeRDD with arbitrary replicated vertices. The
   * VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
   * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
   */
  def apply[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]): GraphImpl[VD, ED] = {

    vertices.cache()

    // Convert the vertex partitions in edges to the correct type
    val newEdges = edges.asInstanceOf[EdgeRDDImpl[ED, _]]
      .mapEdgePartitions((pid, part) => part.withoutVertexAttributes[VD])
      .cache()

    GraphImpl.fromExistingRDDs(vertices, newEdges)
  }

  /**
   * Create a graph from a VertexRDD and an EdgeRDD with the same replicated vertex type as the
   * vertices. The VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
   * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
   */
  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]): GraphImpl[VD, ED] = {
    new GraphImpl(vertices, new ReplicatedVertexView(edges.asInstanceOf[EdgeRDDImpl[ED, VD]]))
  }

  /**
   * Create a graph from an EdgeRDD with the correct vertex type, setting missing vertices to
   * `defaultVertexAttr`. The vertices will have the same number of partitions as the EdgeRDD.
   */
  def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
      edges: EdgeRDDImpl[ED, VD],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    val edgesCached = edges.withTargetStorageLevel(edgeStorageLevel).cache()
    val vertices =
      VertexRDD.fromEdges(edgesCached, edgesCached.partitions.length, defaultVertexAttr)
      .withTargetStorageLevel(vertexStorageLevel)
    fromExistingRDDs(vertices, edgesCached)
  }

  def computeGcTime(): Long = ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum

} // end of object GraphImpl
