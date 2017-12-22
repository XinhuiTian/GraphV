
package org.apache.spark.graphv.enhanced

import scala.reflect.ClassTag

import org.apache.spark.graphv.{Edge, EdgeDirection, VertexId}
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.HashPartitioner
import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

class MessageBlock[VD: ClassTag](val vids: Array[VertexId], val attrs: Array[VD])
  extends Serializable {
  def iterator: Iterator[(VertexId, VD)] =
    (0 until vids.length).iterator.map { i => (vids(i), attrs(i)) }
}

class LocalMessageBlock[VD: ClassTag](val vids: Array[Int], val attrs: Array[VD])
  extends Serializable {
  def iterator: Iterator[(Int, VD)] =
    (0 until vids.length).iterator.map { i => (vids(i), attrs(i)) }
}

class LocalFinalMessages[VD: ClassTag](
    val values: Array[VD],
    val mask: BitSet) extends Serializable {
  def iterator: Iterator[(Int, VD)] =
    mask.iterator.map (ind => (ind, values (ind)))
}

class GlobalFinalMessages[VD: ClassTag](
    val l2g: Array[VertexId],
    val values: Array[VD],
    val mask: BitSet) extends Serializable {
  def iterator: Iterator[(VertexId, VD)] =
    mask.iterator.map (ind => (l2g(ind), values (ind)))
}
/*
 the vertices have three types:
 1. vertices with small degrees: have all the neighborIds locally
 2. vertices with large degrees: only have the pids of their neighbors
 3. mirrors of remote vertices with large degrees: neighbors in this partition
  */
class GraphPartition[
@specialized (Char, Int, Boolean, Byte, Long, Float, Double) VD: ClassTag, ED: ClassTag](
    // val dstIds: Array[VertexId], remove edges for mirror-based vertices
    val localDstIds: Array[Int],
    val attrs: Array[VD], // src attr, the index is based on the local2global index
    val vertexIndex: Array[Int], // srcId to dstIndex
    // val mirrorIndex: GraphXPrimitiveKeyOpenHashMap[VertexId, (Int, Int)], // index of mirrors
    val edgeAttrs: Array[ED],
    val global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    // layout: |SDVertices | mirrorsForRemoteLDMasters | mixMasters | LDMasters
    // | otherNeighbors
    val local2global: Array[VertexId],
    // structures for mirrors
    // val dstIdsForRemote: Array[Int],
    // val remoteEdgeAttrs: Array[ED],
    // remote vid to dstIndex
    // val remoteVidIndex: GraphXPrimitiveKeyOpenHashMap[VertexId, (Int, Int)],
    val smallDegreeEndPos: Int,
    val largeDegreeMirrorEndPos: Int,
    val largeDegreeMasterEndPos: Int,
    val numPartitions: Int,
    val routingTable: RoutingTable,
    val activeSet: BitSet)
  extends Serializable {

  // val localVertexSize = smallDegreeSize + largeDegreeSize
  // must have at last one vertex
  require(local2global.length > 0)

  def vertexAttrSize: Int = attrs.size

  def masterSize: Int = smallDegreeEndPos + (largeDegreeMasterEndPos - largeDegreeMirrorEndPos)

  def mirrorSize: Int = totalVertSize - masterSize

  def edgeSize: Int = localDstIds.length

  def totalVertSize: Int = local2global.size

  val thisPid: Int = new HashPartitioner(numPartitions).getPartition(local2global(0))

  def iterator: Iterator[(VertexId, VD)] = local2global.zipWithIndex
    .filter(v => v._2 < smallDegreeEndPos || (
      v._2 >= largeDegreeMirrorEndPos && v._2 < largeDegreeMasterEndPos))
    .map (v => (v._1, attrs(v._2))).iterator

  def edgeIterator: Iterator[Edge[ED]] = {
    val edges = new Array[Edge[ED]](edgeAttrs.length)
    for (i <- 0 until vertexIndex.size - 1) {
      val srcId = i
      val dstIndex = vertexIndex(i)
      val dstEndPos = vertexIndex(i + 1)
      for (j <- dstIndex until dstEndPos) {
        val edge = new Edge[ED]
        edge.dstId = local2global(localDstIds(j))
        edge.attr = edgeAttrs(j)
        edge.srcId = local2global(i)
        edges(j) = edge
      }
    }

    edges.iterator
  }

  def masterIterator: Iterator[(VertexId, VD)] = iterator

  // topology mirrors + propagation mirrors
  def mirrorIterator: Iterator[Int] = ((smallDegreeEndPos until largeDegreeMirrorEndPos)
    ++ (largeDegreeMasterEndPos to totalVertSize)).iterator

  def remoteLDMirrorIterator
  : Iterator[Int] = (smallDegreeEndPos until largeDegreeMirrorEndPos).iterator

  // filter in mirrors and out mirrors from the localDstIds
  def remoteOutMirrorIterator: Iterator[Int] = localDstIds
    .filter { v =>
      (v > smallDegreeEndPos && v < largeDegreeMirrorEndPos) || v >= largeDegreeMasterEndPos }
    .distinct
    .iterator

  def neighborIterator: Iterator[VertexId] = localDstIds.map(v => local2global(v)).iterator

  def localNeighborIterator: Iterator[Int] = localDstIds.iterator

  def isActive(vid: Int): Boolean = {
    activeSet.get(vid)
  }

  def activateAllMasters: GraphPartition[VD, ED] = {
    for (i <- 0 until smallDegreeEndPos) {
      activeSet.set(i)
    }
    for (i <- largeDegreeMirrorEndPos until largeDegreeMasterEndPos) {
      activeSet.set(i)
    }

    new GraphPartition[VD, ED](localDstIds, attrs,
      vertexIndex, edgeAttrs, global2local, local2global,
      smallDegreeEndPos, largeDegreeMirrorEndPos, largeDegreeMasterEndPos,
      numPartitions, routingTable, activeSet)
  }

  def getActiveNum: Long = activeSet.iterator.filter(i => i < smallDegreeEndPos || (
    i >= largeDegreeMirrorEndPos && i < largeDegreeMasterEndPos)).length.toLong

  // used for computation init and vertices changing
  def mapVertices[VD2: ClassTag](
      f: (VertexId, VD) => VD2,
      needActive: Boolean = false): GraphPartition[VD2, ED] = {
    val newValues = new Array[VD2](vertexAttrSize)

    ((0 until smallDegreeEndPos) ++ (largeDegreeMirrorEndPos until largeDegreeMasterEndPos))
      .foreach { i =>
        newValues(i) = f (local2global(i), attrs(i))
        if (needActive) {
          if (newValues(i) != attrs(i)) {
            activeSet.set(i)
          } else {
            activeSet.unset(i)
          }
        }
      }

    new GraphPartition[VD2, ED](localDstIds, newValues,
      vertexIndex, edgeAttrs, global2local, local2global,
      smallDegreeEndPos, largeDegreeMirrorEndPos, largeDegreeMasterEndPos,
      numPartitions, routingTable, activeSet)
  }

  def mapTriplets[ED2: ClassTag](f: GraphVEdgeTriplet[VD, ED] => ED2)
  : GraphPartition[VD, ED2] = {
    val newData = new Array[ED2](edgeAttrs.length)
    for (i <- 0 until vertexIndex.size - 1) {
      val localSrcId = i
      val dstIndex = vertexIndex(i)
      val dstEndPos = vertexIndex(i + 1)
      for (j <- dstIndex until dstEndPos) {
        val triplet = new GraphVEdgeTriplet[VD, ED]
        triplet.srcId = local2global(localSrcId)
        // println("Triplet: " + triplet.srcId + " " + attrs(localSrcId))
        triplet.dstId = local2global(localDstIds(j))
        triplet.srcAttr = attrs(localSrcId)
        triplet.attr = edgeAttrs(j)
        newData(j) = f (triplet)
        // println(newData(j))
      }
    }

    new GraphPartition[VD, ED2](localDstIds, attrs,
      vertexIndex, newData, global2local, local2global,
      smallDegreeEndPos, largeDegreeMirrorEndPos, largeDegreeMasterEndPos,
      numPartitions, routingTable, activeSet)
  }

  def joinLocalMsgs[A: ClassTag](localMsgs: Iterator[(Int, A)])(
      vprog: (VertexId, VD, A) => VD): GraphPartition[VD, ED] = {

    val newValues = new Array[VD](vertexAttrSize)
    Array.copy(attrs, 0, newValues, vertexAttrSize, vertexAttrSize)


    localMsgs.foreach { msg =>
      val localVid = msg._1
      val localMsg = msg._2
      val newValue = vprog(local2global(localVid), newValues(localVid), localMsg)
      if (newValue == newValues(localVid)) {
        activeSet.unset(localVid)
      } else {
        activeSet.set(localVid)
        newValues(localVid) = newValue
      }
    }

    new GraphPartition(localDstIds, newValues,
      vertexIndex, edgeAttrs, global2local, local2global,
      smallDegreeEndPos, largeDegreeMirrorEndPos, largeDegreeMasterEndPos,
      numPartitions, routingTable, activeSet)
  }

  def localLeftJoin[VD2: ClassTag](
      other: LocalFinalMessages[VD2],
      needActive: Boolean = false)
    (f: (VertexId, VD, VD2) => VD): GraphPartition[VD, ED] = {

    val uf = (id: VertexId, data: VD, o: Option[VD2]) => {
      o match {
        case Some (u) => f (id, data, u)
        case None => data
      }
    }

    // val localMsgs = Array.fill[Option[VD2]](vertexAttrSize)(None)
    val newValues = new Array[VD](vertexAttrSize)

    /*
    other.foreach { v =>
      localMsgs(v._1) = Some(v._2)
    }
    */

    // only the master vertices
    ((0 until smallDegreeEndPos) ++ (largeDegreeMirrorEndPos until largeDegreeMasterEndPos))
      .foreach { i =>
      // println(i, local2global(i), attrs(i), localMsgs(i), vertexAttrSize)

        val otherV: Option[VD2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues (i) = uf(local2global(i), attrs(i), otherV)

        if (needActive) {
          if (newValues(i) != attrs(i)) {
            activeSet.set(i)
          } else { // already considering the None case
            activeSet.unset(i)
          }
        }
      }

    new GraphPartition(localDstIds, newValues,
      vertexIndex, edgeAttrs, global2local, local2global,
      smallDegreeEndPos, largeDegreeMirrorEndPos, largeDegreeMasterEndPos,
      numPartitions, routingTable, activeSet)
  }

  // send mirrors to the master pids to build routing tables
  def shipMirrors: Iterator[(VertexId, Int)] = mirrorIterator.map(v => (local2global(v), v))

  // functions for undirected graphs
  def sendRequests: Iterator[(VertexId, Int)] = {
    var pos = activeSet.nextSetBit(0)
    val requests = new BitSet(totalVertSize)
    while (pos >= 0 && pos < smallDegreeEndPos) {
      val neighborStartPos = vertexIndex(pos)
      val neighborEndPos = vertexIndex(pos + 1)
      for (i <- neighborStartPos until neighborEndPos) {
        requests.set(localDstIds(i))
      }
      pos = activeSet.nextSetBit(pos + 1)
    }

    while (pos >= smallDegreeEndPos && pos < largeDegreeMirrorEndPos) {
      requests.set(pos)
      pos = activeSet.nextSetBit(pos + 1)
    }

    requests.iterator.map (localId => (local2global(localId), thisPid))
  }

  def generateSyncMsgs(requests: Iterator[(VertexId, Int)])
  : Iterator[(Int, Array[(VertexId, VD)])] = {
    val syncBitSets = Array.fill[BitSet](numPartitions)(new BitSet(masterSize))

    requests.foreach { request =>
      val clientPid = request._2
      val localVid = global2local(request._1)
      syncBitSets(clientPid).set(localVid)
    }

    syncBitSets.zipWithIndex.flatMap { masterMask =>
      val clientPid = masterMask._2
      val mask = masterMask._1
      val msgs = mask.iterator.map(v => (local2global(v), attrs(v)))
      Iterator((clientPid, msgs.toArray))
    }.iterator
  }

  def syncMirrors(msgs: Iterator[(VertexId, VD)]): GraphPartition[VD, ED] = {
    // require(vertexAttrSize == totalVertSize) // must create spaces for all mirrors
    val newValues = new Array[VD](totalVertSize)
    Array.copy(attrs, 0, newValues, vertexAttrSize, vertexAttrSize)
    msgs.foreach { msg =>
      val localId = global2local(msg._1)
      newValues(localId) = msg._2
    }

    this.withNewValues(newValues)
  }

  def generateSrcSyncMessages: Iterator[(Int, Array[(Int, VD)])] = {
    val messages = Array.fill(numPartitions)(new PrimitiveVector[(Int, VD)])
    for (i <- 0 until numPartitions) {
      routingTable.foreachWithPartition(i, true, false) { v =>
        messages(i) += (v._2, attrs(v._1))
      }
    }
    messages.zipWithIndex.map(m => (m._2, m._1.toArray)).iterator
  }

  def syncSrcMirrors(msgs: Iterator[(Int, Array[(Int, VD)])]): GraphPartition[VD, ED] = {
    val newValues = new Array[VD](vertexAttrSize)
    Array.copy(attrs, 0, newValues, 0, vertexAttrSize)
    msgs.foreach(msgBlock => msgBlock._2.foreach(msg => newValues(msg._1) = msg._2))
    // newValues.zipWithIndex.foreach(v => println(local2global(v._2), v._1))
    this.withNewValues(newValues)
  }

  def generateLocalMsgs[A: ClassTag](
      msgs: Iterator[(VertexId, A)]
  )(reduceFunc: (A, A) => A): LocalFinalMessages[A] = {
    val newMask = new BitSet(vertexAttrSize)
    val finalMsgs = new Array[A](vertexAttrSize)

    msgs.foreach { msg =>
      val vid = msg._1
      val value = msg._2
      val pos = global2local.getOrElse(vid, -1)
      // println("From global to local: " + pos)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          finalMsgs(pos) = reduceFunc(finalMsgs(pos), value)
        } else {
          newMask.set(pos)
          finalMsgs(pos) = value
        }
      }
    }
    new LocalFinalMessages(finalMsgs, newMask)
  }

  // get all the push messages and mirror messages
  // for masters in this partition
  // compute the final msgs for all local vertices
  // update mirror values
  def generateFinalMsgs[A: ClassTag](
      msgs: Iterator[(Int, (LocalMessageBlock[VD], MessageBlock[A]))])(
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A): LocalFinalMessages[A] = {

    val newMask = new BitSet(vertexAttrSize)
    val finalMsgs = new Array[A](vertexAttrSize)
    val ctx = new AggregatingVertexContext[VD, ED, A](mergeMsg, finalMsgs, newMask, numPartitions)

    var pushMsgCount = 0
    var mirrorMsgCount = 0
    var localMirrorMsgCount = 0

    val mixMsgs = msgs.toArray.map(_._2)

    val mirrorMsgs = mixMsgs.flatMap(_._1.iterator)
    val pushMsgs = mixMsgs.flatMap(_._2.iterator)

    val activeLDMasters = activeSet.iterator
      .filter(v => v >= largeDegreeMirrorEndPos && v < vertexIndex.size - 1)
      .map {
        v => (v, attrs(v))
      }

    // handle all the LDmirrors, including the mixMasters
    (mirrorMsgs.iterator ++ activeLDMasters).foreach { msg =>
      val localId = msg._1
      val value = msg._2

      val neighborStartPos = vertexIndex(localId)
      val neighborEndPos = vertexIndex(localId + 1)
      val srcId = local2global(localId)
      // val srcAttr = attrs(localId)
      ctx.setSrcOnly(srcId, localId, value)

      for (i <- neighborStartPos until neighborEndPos) {
        val edgeAttr = edgeAttrs(i)
        // println("EdgeAttr: " + edgeAttr)
        ctx.setEdgeAndDst(edgeAttr, localDstIds(i), local2global(localDstIds(i)))
        mirrorMsgCount += 1
        // println(localId, localDstIds(i), edgeAttr)
        sendMsg (ctx)
      }
    }

    pushMsgs.foreach { msg =>
      val vid = msg._1
      val value = msg._2
      val pos = global2local.getOrElse(vid, -1)

      pushMsgCount += 1
      if (pos >= 0) {
        if (newMask.get(pos)) {
          finalMsgs(pos) = mergeMsg(finalMsgs(pos), value)
        } else {
          newMask.set(pos)
          finalMsgs(pos) = value
        }
      }
    }
    // handle the mixMasters
    // println("LDMEPos: " + largeDegreeMirrorEndPos + " " + vertexIndex.size)

    /*
    println("totalMessages: " + pushMsgCount, mirrorMsgCount, localMirrorMsgCount
      + " vertice num: " + this.smallDegreeEndPos,
      (this.vertexIndex.size - 1 - this.largeDegreeMirrorEndPos),
      (this.largeDegreeMasterEndPos - this.largeDegreeMirrorEndPos),
      this.vertexIndex.size - 1, this.largeDegreeMasterEndPos)
      */
    new LocalFinalMessages(finalMsgs, newMask)
  }

  def localAggregate[A: ClassTag](localMsgs: Iterator[(Int, A)])(
    reduceFunc: (A, A) => A): Iterator[(Int, A)] = {
    // also consider mirrors, may have some redundant spaces,
    // but do not have to judge whether master or not
    val attrSize = largeDegreeMasterEndPos
    val newValues = new Array[A](attrSize)
    // Array.copy(attrs, 0, newValues, vertexAttrSize, vertexAttrSize)
    val newMask = new BitSet(attrSize)

    localMsgs.foreach { product =>
      val vid = product._1
      val vdata = product._2
      // val pos = vid
      if (vid >= 0) {
        // println(vid)
        if (newMask.get (vid)) {
          newValues (vid) = reduceFunc (newValues (vid), vdata)
        } else { // otherwise just store the new value
          newMask.set (vid)
          newValues (vid) = vdata
        }
        //        println("debug")
      }
    }

    // newMask.iterator.map(v => (local2global(v), newValues(v)))
    newValues.zipWithIndex.filter(v => v._2 < smallDegreeEndPos || v._2 >= largeDegreeMirrorEndPos)
      .map(v => (v._2, v._1)).iterator
  }

  def globalAggregate[A: ClassTag](localMsgs: Iterator[(Int, A)])(
      reduceFunc: (A, A) => A): Iterator[(VertexId, A)] =
    localAggregate(localMsgs)(reduceFunc).map(msg => (local2global(msg._1), msg._2))




  /*
  def joinLocalMsgs[A: ClassTag](localMsgs: (Array[A], BitSet))(
      vprog: (VertexId, VD, A) => VD): GraphPartition[VD, ED] = {

    val uf = (id: VertexId, data: VD, o: Option[A]) => {
      o match {
        case Some (u) => vprog (id, data, u)
        case None => data
      }
    }

    val mask = localMsgs._2
    val values = localMsgs._1
    val newValues = new Array[VD](vertexAttrSize)
    for (i <- 0 until vertexAttrSize) {
      val otherV: Option[A] = if (mask.get (i)) Some (values (i)) else None
      newValues (i) = uf (local2global (i), attrs (i), otherV)
      if (attrs (i) == newValues (i)) {
        activeSet.unset (i)
      } else {
        activeSet.set (i)
      }
    }
    this.withNewValues(newValues)
  }
  */
  // edge centric computation
  /*
  def aggregateMessagesForBothSizes[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A): Iterator[(VertexId, A)] = {
    val aggregates = new Array[A](local2global.length)
    val bitset = new BitSet(local2global.length)

    // val mirrorMsgs = new Array[PrimitiveVector[(VertexId, VD)]](numPartitions)

    val ctx = new AggregatingVertexContext[VD, ED, A](mergeMsg, aggregates, bitset, numPartitions)

    // for all the active edges
    var pos = activeSet.nextSetBit(0)
    while (pos >= 0 && pos < largeDegreeStartPos) {
      val srcId = local2global(pos)
      val dstStartPos = vertexIndex(pos)
      val dstEndPos = vertexIndex(pos + 1)
      val srcAttr = attrs(pos)
      for (i <- dstStartPos until dstEndPos) {
        val localDstId = localDstIds (i)
        ctx.set (srcId, local2global (localDstId), pos, localDstId,
          srcAttr, attrs(i), edgeAttrs (i))
        sendMsg (ctx)
      }
      pos = activeSet.nextSetBit(pos + 1)
    }

    // generate mirror msgs
    bitset.iterator.map { localId =>
      (global2local(localId), aggregates(localId))
    }
  }
*/
  // considering vertices from both sides of each edge
  def aggregateMessagesEdgeCentric[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      edgeDirection: EdgeDirection,
      tripletFields: TripletFields = TripletFields.DstWithEdge): Iterator[(VertexId, A)] = {
    val aggregates = new Array[A](local2global.length)
    val bitset = new BitSet (local2global.length)

    val ctx = new AggregatingVertexContext[VD, ED, A](mergeMsg, aggregates, bitset, numPartitions)

    for (i <- 0 until vertexIndex.size - 1) {
      val clusterPos = vertexIndex (i)
      val clusterLocalSrcId = i
      val scanCluster =
        if (edgeDirection == EdgeDirection.Out) isActive (clusterLocalSrcId)
        else if (edgeDirection == EdgeDirection.Both) true
        else if (edgeDirection == EdgeDirection.In) true
        else throw new Exception ("unreachable")

      if (scanCluster) {
        val startPos = clusterPos
        val endPos = vertexIndex (clusterLocalSrcId + 1)
        val srcAttr = attrs (clusterLocalSrcId)
        ctx.setSrcOnly (local2global (clusterLocalSrcId), clusterLocalSrcId, srcAttr)
        for (i <- startPos until endPos) {
          val localDstId = localDstIds (i)
          val dstId = local2global (localDstId)
          val edgeIsActive =
            if (edgeDirection == EdgeDirection.In) isActive (localDstId)
            else if (edgeDirection == EdgeDirection.Both) isActive (localDstId) ||
              isActive (clusterLocalSrcId)
            else if (edgeDirection == EdgeDirection.Out) true
            else throw new Exception ("unreachable")
          if (edgeIsActive) {
            val dstAttr = if (localDstId > vertexAttrSize) null.asInstanceOf [VD]
            else attrs (localDstId)
            ctx.setRest (dstId, localDstId, dstAttr, edgeAttrs (i))
            sendMsg (ctx)
          }
        }
      }
    }
    bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }
  }

  // msgs: First the aggregated msgs for remote vertices, then the mirror messages
  def aggregateMessagesVertexCentric[A: ClassTag](
      sendMsg: VertexContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.SrcWithEdge)
  : Iterator[(Int, (LocalMessageBlock[VD], MessageBlock[A]))] = {
    val aggregates = new Array[A](local2global.length)
    val bitset = new BitSet(local2global.length)
    var sdVertices = 0
    var ldVertices = 0
    val startPushTime = System.currentTimeMillis()

    val ctx = new AggregatingVertexContext[VD, ED, A](mergeMsg, aggregates, bitset, numPartitions)
    // first generate the push msgs, only for smallDegreeVertices
    // which are out-completed
    var pos = activeSet.nextSetBit(0)
    while (pos >= 0 && (pos < smallDegreeEndPos)) {
      sdVertices += 1
      val srcId = local2global(pos)
      val dstStartPos = vertexIndex(pos)
      val dstEndPos = vertexIndex(pos + 1)
      val srcAttr = attrs(pos)
      for (i <- dstStartPos until dstEndPos) {
        val localDstId = localDstIds (i)
        ctx.set (srcId, local2global (localDstId), pos, localDstId,
          srcAttr, null.asInstanceOf[VD], edgeAttrs (i))
        sendMsg (ctx)
      }
      pos = activeSet.nextSetBit(pos + 1)
    }

    val endPushTime = System.currentTimeMillis()
    // println("Push Time: " + (endPushTime - startPushTime))

    val startTime = System.currentTimeMillis()
    val pushVids = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
    val pushMsgs = Array.fill(numPartitions)(new PrimitiveVector[A])
    val partitioner = new HashPartitioner(numPartitions)
    bitset.iterator.foreach { localId =>
      val globalId = local2global(localId)
      val pid = partitioner.getPartition(globalId)
      // println(globalId, aggregates(localId))
      pushVids(pid) += globalId
      pushMsgs(pid) += aggregates(localId)
    }

    // println("Process Push Msgs: " + (System.currentTimeMillis() - startTime))

    if (tripletFields.useSrc == false) {
      pushVids.zip(pushMsgs).zipWithIndex.map { msgs =>
        (msgs._2, (new LocalMessageBlock(Array.empty[Int], Array.empty[VD]),
          new MessageBlock(msgs._1._1.trim.array, msgs._1._2.trim.array)))
      }.iterator
    } else {

      // generate mirror msgs, for each active LDMaster
      val msgs = Iterator.tabulate(numPartitions) { pid =>
        var mirrorVids = new PrimitiveVector[Int]
        var mirrorAttrs = new PrimitiveVector[VD]

        routingTable.foreachWithPartition(pid, true, false) { vid =>
          if (activeSet.get (vid._1)) {
            ldVertices += 1
            // determine whether this ldmaster is active or not
            mirrorVids += vid._2
            mirrorAttrs += attrs (vid._1)
          }
        }

        (pid, (new LocalMessageBlock(mirrorVids.trim().array, mirrorAttrs.trim().array),
          new MessageBlock(pushVids(pid).trim().array, pushMsgs(pid).trim().array)))
      }

      // println(s"ldVertices: $ldVertices, sdVertices: $sdVertices")
      msgs
    }
  }

  def withNewValues(newValues: Array[VD]): GraphPartition[VD, ED] = {
    new GraphPartition[VD, ED](localDstIds, newValues,
      vertexIndex, edgeAttrs, global2local, local2global,
      smallDegreeEndPos, largeDegreeMirrorEndPos, largeDegreeMasterEndPos,
      numPartitions, routingTable, activeSet)
  }

  def withRoutingTable(newRoutingTable: RoutingTable): GraphPartition[VD, ED] = {
    new GraphPartition[VD, ED](localDstIds, attrs,
      vertexIndex, edgeAttrs, global2local, local2global,
      smallDegreeEndPos, largeDegreeMirrorEndPos, largeDegreeMasterEndPos,
      numPartitions, newRoutingTable, activeSet)
  }
}

private class SimpleVertexContext[VD, ED, A]()

private class AggregatingVertexContext[VD, ED, A](
    mergeMsg: (A, A) => A,
    aggregates: Array[A],
    bitset: BitSet,
    numPartitions: Int,
    direction: EdgeDirection = EdgeDirection.Out) extends VertexContext[VD, ED, A](direction) {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _
  private[this] var _srcAttr: VD = _
  private[this] var _dstAttr: VD = _
  private[this] var _attr: ED = _

  def set(
      srcId: VertexId, dstId: VertexId,
      localSrcId: Int, localDstId: Int,
      srcAttr: VD, dstAttr: VD, attr: ED) {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
  }

  def setSrcOnly(srcId: VertexId, localSrcId: Int, srcAttr: VD) {
    _srcId = srcId
    _localSrcId = localSrcId
    _srcAttr = srcAttr
  }

  def setEdgeAndDst(edgeAttr: ED, localDstId: Int, dstId: VertexId): Unit = {
    _attr = edgeAttr
    _localDstId = localDstId
    _dstId = dstId
  }

  def setRest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED) {
    _dstId = dstId
    _localDstId = localDstId
    _attr = attr
  }

  override def srcId: VertexId = _srcId

  override def dstId: VertexId = _dstId

  override def srcAttr: VD = _srcAttr

  override def dstAttr: VD = _dstAttr

  override def attr: ED = _attr

  override def sendToDst(msg: A) {
    send (_localDstId, msg)
  }

  override def sendToSrc(msg: A) {
    send (_localSrcId, msg)
  }

  @inline private def send(localId: Int, msg: A) {
    if (bitset.get (localId)) {
      aggregates (localId) = mergeMsg (aggregates (localId), msg)
    } else {
      aggregates (localId) = msg
      // println(localId, msg)
      bitset.set (localId)
    }
  }
}
