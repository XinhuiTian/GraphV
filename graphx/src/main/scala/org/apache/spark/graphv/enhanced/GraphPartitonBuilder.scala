
package org.apache.spark.graphv.enhanced

import scala.reflect.ClassTag

import org.apache.spark.graphv._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.HashPartitioner
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector}

class GraphPartitonBuilder[@specialized (Long, Int, Double) VD: ClassTag]
(degreeThreshold: Int, numPartitions: Int, defaultVertexValue: VD) {

  private[this] var localVertices: Array[(Long, Array[Long])] = Array.empty[(Long, Array[Long])]
  private[this] var remoteVertices: Array[(Long, Array[Long])] = Array.empty[(Long, Array[Long])]
  private[this] var indexedVertSize = 0
  private[this] var vertSize = 0

  // def getVertexSize: Int = localVertices.size

  val helperPartitioner = new HashPartitioner(numPartitions)

  /** Add a new edge to the partition. */
  def add(locals: Iterator[(Long, Array[Long])], remotes: Iterator[(Long, Array[Long])]) {

    localVertices = locals.toArray
    remoteVertices = remotes.toArray
    // vertexSize += v._2.size - 1
  }

  def computeMirrorPids(verts: Array[(Long, Array[Long])], numPart: Int)
  : Array[(Long, Array[Int])] = {
    val partitioner = new HashPartitioner(numPart)
    verts.map { v =>
      val srcId = v._1
      val map = new BitSet(numPart) // use a bitmask instead of a hashset
      v._2.foreach { dstId =>
        val pid = partitioner.getPartition(dstId)
        map.set(pid)
      }
      (v._1, map.iterator.toArray)
    }
  }

  // build local graph structures without routing table for mirrors
  def toGraphPartition: GraphPartition[VD, Int] = {

    require(localVertices.length > 0)

    val pid = helperPartitioner.getPartition(localVertices(0)._1)

    // compute mirrorPids for each large-degree vertices
    // large degree masters
    val largeDegreeMasters = localVertices.filter(_._2.length > degreeThreshold)
    // change edges to pids
    // val mirrorPids = computeMirrorPids(largeDegreeMasters, numPartitions)
    // small degree masters
    val smallDegreeVerts = localVertices.filter(_._2.length <= degreeThreshold)
    // println("SmallDegreeVerts")
    // smallDegreeVerts.map(_._1).foreach(println)
    // pure mirrors
    val pureMirrors = remoteVertices.filter(v => helperPartitioner.getPartition(v._1) != pid)
    // mirrors that are actually the masters
    val mixMasters = remoteVertices.filter(v => helperPartitioner.getPartition(v._1) == pid)

    // val edges = smallDegreeVerts.map(_._2.length).sum() + remoteVertices.map(_._2.length).sum()

    // println("edges: " + edgeSize)



    // val initVertexSize = localVertices.length + remoteVertices.length
    // val initVertexSize = smallDegreeVerts.length + pureMirrors.length + largeDegreeMasters.length
    val initVertexSize = 64
    // println(initVertexSize)
    // println("InitVertexSize " + initVertexSize)
    // release big arrays
    localVertices = null
    // remoteVertices = null
    // System.gc()

    val edgeForLocalSize = smallDegreeVerts.map(_._2.length).sum
    val edgeForRemoteSize = remoteVertices.map(_._2.length).sum
    val edgeSize = edgeForLocalSize + edgeForRemoteSize
    println("edges: " + edgeSize)
    val localDstIds = new Array[Int](edgeSize)
    // val edgeAttrs = new Array[Int](edgeSize)

    // at least initVertexSize vertices
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int](initVertexSize)
    val local2global = new PrimitiveVector[VertexId](initVertexSize)

    var currLocalId = -1
    // Fix: previous using Array[Int],
    // when adding dstIds of two different remote vertices,
    // can not promise these two vertices are adjacent in local vids
    var index = Array.empty[Int]
    // var mirrorIndex = new GraphXPrimitiveKeyOpenHashMap[VertexId, (Int, Int)]
    // (remoteVertices.length)
    var startPos = 0

    // 1. add all the vertices that need to record attrs
    // first the small degree vertices, which have complete adj lists locally
    smallDegreeVerts.map(_._1).foreach{ vid =>
      // println(vid)
      global2local.changeValue (vid, {
        currLocalId += 1
        local2global += vid
        currLocalId
      }, identity)
    }

    val smallDegreeEndPos = local2global.length

    // then mirrors, which have partial adj lists locally
    pureMirrors.map(_._1).foreach (vid => {
      global2local.changeValue (vid, {
        currLocalId += 1
        local2global += vid
        currLocalId
      }, identity)
    })

    val pureMirrorEndPos = local2global.length

    // then mixMasters, which are both mirrors and largeDegreeMasters,
    // having partial adj lists locally, also needing mirror lists
    mixMasters.map(_._1).foreach (vid => {
      global2local.changeValue (vid, {
        currLocalId += 1
        local2global += vid
        currLocalId
      }, identity)
    })

    indexedVertSize = local2global.size
    // println("IndexVertexSize: " + indexedVertSize)
    index = Array.fill[Int](indexedVertSize + 1)(-1)

    largeDegreeMasters.map(_._1).foreach (vid => {
      global2local.changeValue (vid, {
        currLocalId += 1
        local2global += vid
        currLocalId
      }, identity)
    })

    val largeDegreeMasterEndPos = local2global.size

    // after adding all the vertices, the layout should be like this:
    // |smallDegreeVertices|pureMirrorVertices|mixLDMasters|pureLDMasters|
    // in each segment, the local vertices are continuous

    println("PartitionSize: " + smallDegreeEndPos,
      pureMirrorEndPos, indexedVertSize, largeDegreeMasterEndPos)

    // vertSize = local2global.size

    // adding remote vertices
    /*
    remoteVertices.map(_._1).foreach (vid => {
      global2local.changeValue (vid, {
        currLocalId += 1
        local2global += vid
        currLocalId
      }, identity)
    })
    */
    var edgeIndex = 0

    var prevLVid = -1
    // must add sequentially
    (smallDegreeVerts ++ pureMirrors ++ mixMasters).foreach {
      v =>
        if (!v._2.isEmpty) {
          v._2.foreach { dstId =>
            val localDstId = global2local.changeValue (dstId, {
              currLocalId += 1
              local2global += dstId
              currLocalId
            }, identity)

            localDstIds (edgeIndex) = localDstId
            edgeIndex += 1
          }
        }

        val neighborSize = v._2.length
        val localSrcId = global2local (v._1)
        if (localSrcId != prevLVid + 1) {
          println("Error when adding edges", localSrcId, prevLVid)
        }
        prevLVid = localSrcId
        index.update (localSrcId, startPos)
        startPos += neighborSize
    }

    /*
    smallDegreeVerts.foreach{v =>
      if (!v._2.isEmpty) {
        v._2.foreach { dstId =>
          val localDstId = global2local.changeValue (dstId, {
            currLocalId += 1
            local2global += dstId
            currLocalId
          }, identity)

          localDstIds (edgeIndex) = localDstId
          edgeIndex += 1
        }
      }

      val neighborSize = v._2.length
      val localSrcId = global2local (v._1)
      index.update (localSrcId, startPos)
      startPos += neighborSize
    }

    // add all the dstIds for remoteVertices
    pureMirrors.foreach { v =>
      if (!v._2.isEmpty) {
        v._2.foreach { dstId =>
          val localDstId = global2local.changeValue (dstId, {
            currLocalId += 1
            local2global += dstId
            currLocalId
          }, identity)

          localDstIds (edgeIndex) = localDstId
          edgeIndex += 1
        }
      }

      val neighborSize = v._2.length
      val localSrcId = global2local(v._1)
      index.update(localSrcId, startPos)
      startPos += neighborSize
    }
    */
    // add a safe guard
    index.update(indexedVertSize, edgeSize)
    vertSize = local2global.size

    /*
    val routingTable = Array.fill[PrimitiveVector[Int]](numPartitions)(new PrimitiveVector[Int])

    mirrorPids.foreach { m =>
      val localVid = global2local(m._1)
      m._2.foreach { pid =>
        routingTable(pid) += localVid
      }
    }

    val finalRoutingTable = routingTable.map(_.trim().toArray)
    */

    val active = new BitSet (vertSize)
    active.setUntil (vertSize)
    val edgeAttrs = new Array[Int](localDstIds.length)
    new GraphPartition (
      localDstIds, Array.fill[VD](vertSize)(defaultVertexValue),
      index, edgeAttrs, global2local, local2global.trim ().array,
      smallDegreeEndPos, pureMirrorEndPos, largeDegreeMasterEndPos,
      numPartitions, null.asInstanceOf[RoutingTable], active)
  }
}
