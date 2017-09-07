package org.apache.spark.graphloca.impl

import scala.reflect.ClassTag

import org.apache.spark.HashPartitioner

import org.apache.spark.graphloca._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector}

/**
 * Created by XinhuiTian on 17/5/4.
 */
class SimpleEdgeRDD[ED: ClassTag](
    val partitionRDD: RDD[(PartitionID, SimpleEdgePartition[ED])]) extends Serializable {

  val numPartitions = partitionRDD.getNumPartitions

  val vertices = partitionRDD.mapPartitions { iter =>
    val (pid, part) = iter.next()
    part.vertices.iterator
  }

  def partitioner = new HashPartitioner(numPartitions)

  def masters = partitionRDD.mapPartitions { iter =>
    val (pid, part) = iter.next()
    part.masters.iterator
  }

  def inComVertices = getInComVertices.flatMap(_._2)

  def outComVertices = getOutComVertices.flatMap(_._2)

  def partitionBy(strategy: String):
    SimpleEdgeRDD[ED] = {
    val partitioner = strategy match {
      case "ObliviousVertexCut" => new IngressObliviousVertexCut(numPartitions)
      case "EdgePartition2D" => new IngressEdgePartition2D(numPartitions)
      case "EdgePartition1D" => new IngressEdgePartition1D(numPartitions)
      case "RandomVertexCut" => new IngressRandomVertexCut(numPartitions)
      case "CanonicalRandomVertexCut" => new IngressCanonicalRandomVertexCut(numPartitions)
      case _ => return this
    }
    partitioner.fromEdges(this)
  }

  // edges ask for the attrs
  private def askForAttrs: RDD[(VertexId, PartitionID)] = {
    partitionRDD.mapPartitions { edgePartIter =>
      val (pid, edgePart) = edgePartIter.next()
      // for each vertex in this edge part, create a (vid, pid) msg
      edgePart.vertices.iterator.map(v => (v, pid))
    }
  }

  // vertices send the attrs
  private def sendAttrs(askMsgs: RDD[(VertexId, PartitionID)]):
    RDD[(PartitionID, Array[(VertexId, (Int, Int))])] = {
    val partitionedMsgs = askMsgs.forcePartitionBy(new HashPartitioner(numPartitions))

    /* println("ack msgs")
    partitionedMsgs.foreachPartition { msgs =>
      msgs.foreach(println)
      println
    }
    */

    val attrMsgs = partitionRDD.zipPartitions(partitionedMsgs) { (edgePartIter, msgs) =>
      val (pid, edgePart) = edgePartIter.next()
      val index = edgePart.masters
      val globalDegrees = edgePart.masterDegrees
      // println(numPartitions)
      val msgsToSend = Array.fill(numPartitions)(new PrimitiveVector[(VertexId, (Int, Int))])
      msgs.foreach { msg =>
        val pos = index.getPos(msg._1)
        // println(s"vertices to edges: (${msg._1}, ${globalDegrees(pos)})")
        msgsToSend(msg._2) += (msg._1, globalDegrees(pos))
      }

      val sendMsgs = msgsToSend.zipWithIndex.map { case (attrs, pid) => (pid, attrs.trim().toArray) }.toIterator
      // println("Created send msgs")
      // sendMsgs.foreach { msgs => msgs._2.foreach(m => println(msgs._1 + " " + m)) }
      sendMsgs
    }
    /*
    println("attrMsgs")
    attrMsgs.foreachPartition { msgs =>
      val (pid, msgPart) = msgs.next()
      msgPart.foreach(msg => println(pid + " " + msg))
      println
    }
    */
    attrMsgs
  }

  def upgradeDegrees: SimpleEdgeRDD[ED] = {
    val acks = askForAttrs
    val updateMsgs = sendAttrs(acks).forcePartitionBy(new HashPartitioner(numPartitions))
    val newPartitionRDD = partitionRDD
      .zipPartitions(updateMsgs) { (edgePartIter, updateMsgsIter) =>
        val (pid, edgePart) = edgePartIter.next()
        val updateMsgs = updateMsgsIter.flatMap(_._2).toArray
        val vertexIndex = edgePart.vertices
        val globalDegrees = new Array[(Int, Int)](vertexIndex.capacity)
        updateMsgs.foreach { msg =>
          // println(s"In partition $pid, get vertex ${msg._1}")
          val pos = vertexIndex.getPos(msg._1)
          globalDegrees(pos) = msg._2
        }
        val newEdgePart = edgePart.withGlobalDegrees(globalDegrees)
        // newEdgePart.localInDegrees = edgePart.localInDegrees
        // newEdgePart.localOutDegrees = edgePart.localOutDegrees
        Iterator((pid, newEdgePart))
      }

    this.withPartitionRDD(newPartitionRDD)
  }

  def updateLocalDegrees: SimpleEdgeRDD[ED] = {
    this.withPartitionRDD(partitionRDD.mapPartitions { partIter =>
      val (pid, part) = partIter.next()
      part.updateLocalDegrees
      Iterator((pid, part))
    })
  }

  def getInComVertices: RDD[(PartitionID, Array[VertexId])] = {
    println("Get In Com vertices")
    partitionRDD.mapPartitions { partIter =>
      val (pid, part) = partIter.next()
      Iterator((pid, part.getInCompleteVertices))
    }
  }

  def getOutComVertices: RDD[(PartitionID, Array[VertexId])] = {
    partitionRDD.mapPartitions { partIter =>
      val (pid, part) = partIter.next()
      Iterator((pid, part.getOutCompleteVertices))
    }
  }

  def computeOutMoveMasters: RDD[(PartitionID, Array[VertexId])] = {
    partitionRDD.mapPartitions { partIter =>
      val (pid, part) = partIter.next()
      val outComVerts = part.getOutCompleteVertices
      val moveMasters = outComVerts.filter( v => partitioner.getPartition(v) != pid)
      val mastersToMove = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
      moveMasters.foreach { master =>
        val pid = partitioner.getPartition(master)
        mastersToMove(pid) += master
      }
      mastersToMove.zipWithIndex.map { case (attrs, pid) => (pid, attrs.trim().toArray) }.toIterator
    }
  }



  def withPartitionRDD(rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]): SimpleEdgeRDD[ED] =
    new SimpleEdgeRDD(rdd)
}

object SimpleEdgeRDD {
  def updateGlobalDegrees[ED: ClassTag](
      simpleGraph: RDD[(PartitionID, SimpleEdgePartition[ED])])
  : SimpleEdgeRDD[ED] = {
    simpleGraph.cache()
    val preAgg = simpleGraph.mapPartitions( _.flatMap {
      case (pid, graphPart) => graphPart.aggregateMsgs
    }).forcePartitionBy(new HashPartitioner(simpleGraph.getNumPartitions))
    println("updateGlobaldegrees simpleGraph numpartition: " + simpleGraph.getNumPartitions)

    val partitionRDD = simpleGraph.zipPartitions(preAgg) { (edgePartIter, msgs) =>
      val (pid, edgePart) = edgePartIter.next()
      // println(pid + " " + edgePart.edges.length)
      val newEdgePart = edgePart.updateMasters(msgs)
      Iterator((pid, newEdgePart))
      // edgePart.masterDegreeIterator
    }

    new SimpleEdgeRDD(partitionRDD)
  }

  // boolean: remove or add? true for remove
  def computeInMoveMasters[ED: ClassTag](simpleGraph: SimpleEdgeRDD[ED])
  : RDD[(PartitionID, (VertexId, Boolean))] = {
    val partitioner = new HashPartitioner(simpleGraph.numPartitions)

    /*
    println("Begin to change masters")
    simpleGraph.partitionRDD.foreach { part =>
      part._2.localDegreeIterator.foreach(v => println(part._1, v))
      println
    }

    println("Global degrees")
    simpleGraph.partitionRDD.foreach { part =>
      part._2.globalDegreeIterator.foreach(v => println(part._1, v))
      println
    }
    */

    simpleGraph.partitionRDD.mapPartitions { partIter =>
      val (pid, part) = partIter.next()
      /*
      println("Get complete vertex now")
      part.vertices.iterator.foreach(v => println("Vertices", pid, v))
      println
      part.globalDegreeIterator.foreach(v => println("Global", pid, v))
      println
      part.localDegreeIterator.foreach(v => println("local", pid, v))
      println
      */
      // get in complete vertices
      val inComVerts = part.getInCompleteVertices

      /*
      println("inComVerts: ")
      inComVerts.foreach(v => println("(" + pid + " " + v + ")"))
      println()
      */

      // get the in complete vertices that are not masters in this partition
      val moveMasters = inComVerts.filter( v => partitioner.getPartition(v) != pid)

      val newMasters = moveMasters.flatMap(master =>
        Iterator((partitioner.getPartition(master), (master, true)), (pid, (master, false))))
      // println("newMasters: " + newMasters.length)
      // println("newMasters")
      // newMasters.foreach(v => println(("New master msg", pid, v)))

      newMasters.toIterator
    }
  }

  def changeMasters[ED: ClassTag](
      simpleGraph: SimpleEdgeRDD[ED]): SimpleEdgeRDD[ED] = {
    // only in masters now
    // partitionRDD.cache()
    simpleGraph.partitionRDD.cache()

    val moveMasters = computeInMoveMasters(simpleGraph)
    val partitioner = new HashPartitioner(simpleGraph.numPartitions)
    println(simpleGraph.numPartitions)
    val masterChangeMsgs = moveMasters.forcePartitionBy(partitioner)
    // masterChangeMsgs.foreachPartition{ msg => msg.foreach(println); println }


    val newEdgePartitions = simpleGraph.partitionRDD
        .zipPartitions(masterChangeMsgs) { (edgePartIter, changingMasters) =>
      val (pid, edgePart) = edgePartIter.next()
      val masters = edgePart.masters
      val newMask = new BitSet(masters.capacity)
      val newMasters = new OpenHashSet[VertexId]
      // println("masters: " + pid + " " + masters.size)
      // masters.iterator.foreach(m => print((pid, m)))
      // println
      // val movingBit = masters.getBitSet
      changingMasters.foreach { m =>

        val remove = m._2._2
        val vid = m._2._1

        if (remove) {
          // println(("changingMasters", pid, m))
          newMask.set(masters.getPos(vid))
          // println(("Remove", pid, vid))
        } else {
          newMasters.add(vid)
          // println(("Add", pid, vid))
        }
      }
      masters.iterator.foreach { v =>
        if (!newMask.get(masters.getPos(v))) {
          newMasters.add(v)
        }
      }

      println("masters after changed: " + pid + " " + newMasters.size)
      // newMasters.iterator.foreach( v => print((pid, v)))
      // println

      // println("part2 length: " + edgePart.newMasters.length)

      Iterator((pid, edgePart.withMasters(newMasters)))
    }
    new SimpleEdgeRDD(newEdgePartitions)
  }

  // the main function to build SimpleEdgeRDD
  // 1. consider the partitioner
  // 2. edges with the vertices
  def buildSimpleEdgeRDD[ED: ClassTag](
      partitionStrategy: String,
      simpleGraph: RDD[(PartitionID, SimpleEdgePartition[ED])],
      dstMove: Boolean = true, srcMove: Boolean = false)
  : SimpleEdgeRDD[ED] = {
    // only have the edges array
    val edgeRDD = new SimpleEdgeRDD(simpleGraph)
    edgeRDD.partitionRDD.cache()
    // 1. repartition: one exchange
    val partitionedRDD = edgeRDD.partitionBy(partitionStrategy)
    // partitionedRDD.partitionRDD.foreach { iter => iter._2.edges.foreach(println); println }

    // 2. build vertex masters: one exchange
    // 3. global degrees compute: one exchange
    val finalEdgeRDD = buildEdgePartWithVertices(partitionedRDD)
    // finalEdgeRDD.masters.foreachPartition { vPart => vPart.foreach(v => print(v + " ")); println}
    /*
    println("Master degrees")
    finalEdgeRDD.partitionRDD.foreach { part =>
      part._2.masterDegreeIterator.foreach(println)
      println
    }
    */
    // println("vertices: " + finalEdgeRDD.vertices.count())
    // 4. upgrade degrees for each vertex in each edge partition:
    // two exchange

    finalEdgeRDD.partitionRDD.cache()
    val globalUpdatedRDD = finalEdgeRDD.upgradeDegrees

    val degreeUpdatedRDD = globalUpdatedRDD.updateLocalDegrees

    /*
    println("local degrees")
    degreeUpdatedRDD.partitionRDD.foreach { part =>
      part._2.localDegreeIterator.foreach(v => println(part._1, v))
      println
    }
    // println("vertices: " + localUpdatedRDD.vertices.count())
    */
    // val changedMastersRDD = changeMasters(degreeUpdatedRDD)

    // println("Changed number")
    // changedMastersRDD.masters.foreachPartition { vPart => vPart.foreach(v => print(v + " ")); println}

    degreeUpdatedRDD
  }

  def buildEdgePartWithVertices[ED: ClassTag](
      edges: SimpleEdgeRDD[ED])
  : SimpleEdgeRDD[ED] = {
    // 1. get the default vertex masters
    // get all the vertices, then partition them using a HashPartitioner
    val partitionRDD = edges.partitionRDD
    partitionRDD.cache()
    val vertices = partitionRDD.mapPartitions { _.flatMap {
      case (pid, edgePart) =>
        var i = 0
        val verts = new OpenHashSet[VertexId]
        while (i < edgePart.edges.length) {
          val srcId = edgePart.edges(i).srcId
          val dstId = edgePart.edges(i).dstId
          verts.add(srcId)
          verts.add(dstId)
          i += 1
        }
        // println("Vertex size: " + verts.iterator.size)
        verts.iterator.map((_, 0))
    }}.partitionBy(new HashPartitioner(edges.numPartitions))

    // 2. merge edges with its masters, they are currently seperated now
    // get all the vertex masters and edges for each simple edge partition
    val newPartitionRDD = partitionRDD.zipPartitions(vertices) { (edgePartIter, vertexPartIter) =>
      val (pid, edgePart) = edgePartIter.next()
      // println(vertexPartIter.size)
      val masterMap = new OpenHashSet[VertexId]
      val vertexMap = new OpenHashSet[VertexId]

      var i = 0
      while (i < edgePart.edges.length) {
        val srcId = edgePart.edges(i).srcId
        val dstId = edgePart.edges(i).dstId
        vertexMap.add(srcId)
        vertexMap.add(dstId)
        i += 1
      }

      vertexPartIter.foreach (vert => masterMap.add(vert._1))
      val simpleEdgePartition = new SimpleEdgePartition(
        masterMap, new Array[(Int, Int)](masterMap.capacity),
        vertexMap, new Array[(Int, Int)](vertexMap.capacity),
        edgePart.edges)
      // move localdegrees update here
      // simpleEdgePartition.updateLocalDegrees
      // println(masterMap.keySet.size + " " + masterMap._values.size)
      Iterator((pid, simpleEdgePartition))
    }
    // include global degrees compute in
    SimpleEdgeRDD.updateGlobalDegrees(newPartitionRDD)
  }
}
