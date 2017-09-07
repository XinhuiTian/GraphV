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

import scala.reflect.ClassTag

// scalastyle:off println

import org.apache.spark.HashPartitioner
import org.apache.spark.graphxpp.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.graphxpp.impl._
import org.apache.spark.graphxpp.impl.RoutingTablePartition._
import org.apache.spark.graphxpp.utils.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

/**
 * Created by XinhuiTian on 17/2/14.
 */
trait IngressEdgePartitioner extends Serializable {
  def numPartitions: Int
  def fromEdges[ED: ClassTag](
    rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgePartition[ED])]
}

case class IngressEdgePartition1D(partitions: Int = -1)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[ED : ClassTag](
    rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgePartition[ED])] = {
    println("Running EdgePartition1D here")
    // rdd.foreach{ part => part._2.edges.foreach(println); println}

    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    println("partnum: " + partNum)
    val newEdges = rdd.flatMap {part =>
      part._2.edges.map {e =>
        val part = PartitionStrategy.EdgePartition1D.getPartition (e.srcId, e.dstId, partNum)
        // println("edge: " + part + " " + e)
        (part, e)
      }
    }.partitionBy(new HashPartitioner(partNum)).map(_._2)
    // newEdges.foreachPartition { edges => edges.foreach(println); println}

    GraphImpl.buildSimpleFromEdges(newEdges)
  }
}

case class IngressEdgePartition2D(partitions: Int = -1)
  extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[ED : ClassTag](
    rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgePartition[ED])] = {
    println("Running EdgePartition2D here")
    // rdd.foreach{ part => part._2.edges.foreach(println); println}

    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    println("partnum: " + partNum)
    val newEdges = rdd.flatMap {part =>
      part._2.edges.map {e =>
        val pid = PartitionStrategy.EdgePartition2D.getPartition (e.srcId, e.dstId, partNum)
        // println("edge: " + part + " " + e)
        (pid, e)
      }
    }.partitionBy(new HashPartitioner(partNum)).map(_._2)
    // newEdges.foreachPartition { edges => edges.foreach(println); println}

    GraphImpl.buildSimpleFromEdges(newEdges)
  }
}

object AggregateDirection extends Enumeration {
  type AggregateDirection = Value
  val InOnly, OutOnly, Both = Value
}

case class IngressHybridPartition(
  partitions: Int = -1,
  aggDir: AggregateDirection.Value = AggregateDirection.Both,
  threshold: Int = 100
) extends IngressEdgePartitioner {
  require(threshold >= 0, s"Number of threshold ($threshold) cannot be negative.")
  def numPartitions: Int = partitions
  def fromEdges[ED : ClassTag](
    rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgePartition[ED])] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    val mixingPrime = 1125899906842597L % partNum
    aggDir match {
      case AggregateDirection.InOnly =>
        // partition edges based on dstId, edge => (pid, edge)
        val ecut_edges = rdd.flatMap {part =>
          part._2.edges.map {e => ((e.dstId * mixingPrime) % partNum, e)}
        }.partitionBy (new HashPartitioner (partNum))

        // compute indegree for each vertex,
        // for vertex with degrees greater than the threshold,
        // change the pid
        val newEdgeParts = ecut_edges.mapPartitions { iter =>
          val messages = iter.toArray
          // the inDegress can store all the vertices to this partition
          val inDegrees = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
          // val outDegrees =
          messages.foreach { message =>
            inDegrees.changeValue (message._2.dstId, 1, _ + 1)
          }

          val allMasters = new GraphXPrimitiveKeyOpenHashMap[VertexId, Byte]
          val highMasters = new GraphXPrimitiveKeyOpenHashMap[VertexId, BitSet]
          val newEdges = messages.map {message =>
            if (inDegrees (message._2.dstId) <= threshold) {
              allMasters.changeValue(message._2.dstId, 0x1, { b: Byte => b })
              message
            } else {
              val newPid = (message._2.srcId * mixingPrime) % partNum
              allMasters.changeValue(message._2.dstId, 0x0, { b: Byte => b })
              highMasters.changeValue (message._2.dstId, new BitSet(partNum),
                { bitset => bitset.set(newPid.toInt); bitset})
              (newPid, message._2)
            }
          }
          Iterator((newEdges, allMasters, highMasters))
        }
        val vertexParts = newEdgeParts.map( part => (part._2, part._3))

        println("InEdges")

        vertexParts.foreach { LaH =>
          println("Low Vertices")
          LaH._1.iterator.foreach(v => print(v + " "))
          println()
          println("High Vertices")
          LaH._2.iterator.foreach{ v => print(v._1 + " ");
            v._2.iterator.foreach( b => print(b + " ")) }
          println()
        }

        val edgeParts = newEdgeParts.flatMap(_._1)
          .partitionBy(new HashPartitioner(partNum)).map{ _._2 }
        GraphImpl.buildSimpleFromEdges(edgeParts)
      case AggregateDirection.OutOnly =>
        val ecut_edges = rdd.flatMap {part =>
          part._2.edges.map {e => ((e.srcId * mixingPrime) % partNum, e)}
        }.partitionBy (new HashPartitioner (partNum))

        // compute outdegree for each vertex,
        // for vertex with degrees greater than the threshold,
        // change the pid
        val newEdges = ecut_edges.mapPartitions { iter =>
          val messages = iter.toArray
          val outDegrees = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
          messages.foreach { message =>
            outDegrees.changeValue (message._2.srcId, 1, _ + 1)
          }
          messages.map {message =>
            if (outDegrees (message._2.srcId) <= threshold) {
              message
            } else {
              ((message._2.dstId * mixingPrime) % partNum, message._2)
            }
          }.toIterator
        }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
        GraphImpl.buildSimpleFromEdges(newEdges)
    }
  }
}

case class IngressBiDiPartition(
  partitions: Int = -1,
  threshold: Int = 100
) extends IngressEdgePartitioner {
  require(threshold >= 0, s"Number of threshold ($threshold) cannot be negative.")
  def numPartitions: Int = partitions
  def fromEdges[ED : ClassTag](
    rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgePartition[ED])] = {
    val in_edge_part = new IngressHybridPartition(partitions,
      AggregateDirection.InOnly, threshold).fromEdges(rdd)

    val out_edge_part = new IngressHybridPartition(partitions,
      AggregateDirection.OutOnly, threshold).fromEdges(rdd)

    // using a hashset to eliminate replicated edges
    val bi_edge_part = in_edge_part.zipPartitions(out_edge_part) {
      (in_iterator, out_iterator) =>
        in_iterator.map {in_part =>
          val pid = in_part._1
          var size = in_part._2.edges.length
          if (size == 0) {
            size = 64
          }
          val edgeSet = new OpenHashSet[Edge[ED]](size)
          in_part._2.edges.foreach { edge => edgeSet.add (edge) }
          out_iterator.flatMap (_._2.edges).foreach { edge => edgeSet.add (edge) }
          val newEdges = edgeSet.iterator.toArray
          (pid, SimpleEdgePartition(newEdges))
        }
    }
    bi_edge_part
  }

  /*
   * 1. vertices: get masters based on dstId, and get high degree vertices
   * 2. edges: in edges for low degree vertices, repartition in edges for high
   * degree vertices.
   * return:
   * 1. all masters with in low or in high bit
   * 2. high degree vertices with partition bit
   * 3. all edges for this partition
   */

  /*
  def buildInEdges[ED: ClassTag](rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgeWithVertexPartition[ED])] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    val mixingPrime = 1125899906842597L % partNum

    val ecut_in_edges = rdd.flatMap {part =>
      part._2.edges.map {e => ((e.dstId * mixingPrime) % partNum, e)}
    }.partitionBy (new HashPartitioner (partNum))

    // compute indegree for each vertex,
    // for vertex with degrees greater than the threshold,
    // change the pid
    val newEdgeParts = ecut_in_edges.mapPartitions { iter =>
      val messages = iter.toArray
      // the inDegress can store all the vertices to this partition
      val inDegrees = new PrimitiveKeyOpenHashMap[VertexId, Int]
      val inMasters = new PrimitiveKeyOpenHashMap[VertexId, BitSet]

      messages.foreach { message =>
        inDegrees.changeValue (message._2.dstId, 1, _ + 1)
      }

      val newEdges = messages.map {message =>
        if (inDegrees (message._2.dstId) <= threshold) {
          inMasters
          message
        } else {
          val newPid = (message._2.srcId * mixingPrime) % partNum
          allMasters.update(message._2.dstId, 0x0)
          inHighMasters.changeValue (message._2.dstId, new BitSet(partNum),
          { bitset => bitset.set(newPid.toInt); bitset})
          (newPid, message._2)
        }
      }
      Iterator((newEdges, allMasters, inHighMasters))
    }
    val vertexParts = newEdgeParts.map( part => (part._2, part._3))

    println("InEdges")

    vertexParts.foreach { LaH =>
      println("Low Vertices")
      LaH._1.iterator.foreach(v => print(v + " "))
      println()
      println("High Vertices")
      LaH._2.iterator.foreach{ v => print(v._1 + " ");
        v._2.iterator.foreach( b => print(b + " ")) }
      println()
    }

    // repartition the edges
    val edgeParts = newEdgeParts.flatMap(_._1)
      .partitionBy(new HashPartitioner(partNum)).map{ _._2 }

    //
    val graphParts = GraphImpl.buildSimpleFromEdges(edgeParts).zipPartitions(vertexParts) {
      (edgePartIter, vertexPartIter) =>
        val (pid, edgePart) = edgePartIter.next()
        val (allVerts, highVerts) = vertexPartIter.next()
        Iterator((pid, new SimpleEdgeWithVertexPartition(edgePart.edges,
          allVerts, highVerts, null)))
    }
    graphParts
  }

  // distribute out edges based on srcIds, compute outdegrees of each vertex
  // repartition high degree vertices
  def buildOutEdges[ED: ClassTag](rdd: RDD[(PartitionID, SimpleEdgeWithVertexPartition[ED])]):
  RDD[(PartitionID, SimpleEdgeWithVertexPartition[ED])] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    val mixingPrime = 1125899906842597L % partNum

    val ecut_out_edges = rdd.flatMap {part =>
      part._2.edges.map {e => ((e.srcId * mixingPrime) % partNum, e)}
    }.partitionBy (new HashPartitioner (partNum))

    val mastersPerPart = rdd.map(part => (part._1, (part._2.allVertices, part._2.inHighVertices)))

    // compute outdegree for each vertex,
    // for vertex with degrees greater than the threshold,
    // change the pid
    val newEdgeParts = ecut_out_edges
      .zipPartitions(mastersPerPart) {
      (outEdgesIter, masterPartIter) =>
        val (pid, masterPart) = masterPartIter.next()
        val outEdges = outEdgesIter.map(_._2)
        Iterator((pid, outEdges, masterPart))
    }
      .mapPartitions { iter =>
        val messages = iter.next()
        val pid = messages._1
        val outEdges = messages._2
        val allMasters = messages._3._1
        val inHighMasters = messages._3._2
        // the inDegress can store all the vertices to this partition
        val outDegrees = new PrimitiveKeyOpenHashMap[VertexId, Int]
        // val allMasters = new PrimitiveKeyOpenHashMap[VertexId, Byte]
        val outHighMasters = new PrimitiveKeyOpenHashMap[VertexId, BitSet]

        outEdges.foreach { edge =>
          outDegrees.changeValue (edge.srcId, 1, _ + 1)
        }

        val newEdges = outEdges.map { edge =>
          if (outDegrees (edge.srcId) <= threshold) {
            allMasters.changeValue(edge.srcId, 0x1, { b: Byte => (b | 0x1).toByte })
            (pid.toLong, edge)
          } else {
            val newPid = (edge.srcId * mixingPrime) % partNum
            allMasters.changeValue(edge.dstId, 0x0, { b: Byte => (b | 0x0).toByte})
            outHighMasters.changeValue (edge.dstId, new BitSet(partNum),
            { bitset => bitset.set(newPid.toInt); bitset})
            (newPid, edge)
          }
        }
        Iterator((newEdges, allMasters, inHighMasters, outHighMasters))
    }.cache()

    // repartition the edges
    val edgeParts = newEdgeParts.flatMap(_._1)
      .partitionBy(new HashPartitioner(partNum)).map{ _._2 }

    val vertexParts = newEdgeParts.map( part => (part._2, part._3, part._4))

    println("InEdges")

    vertexParts.foreach { LaH =>
      println("Low Vertices")
      LaH._1.iterator.foreach(v => print(v + " "))
      println()
      println("High In Vertices")
      LaH._2.iterator.foreach{ v => print(v._1 + " ")
        v._2.iterator.foreach( b => print(b + " ")) }
      println()

      println("High Out Vertices")
      LaH._3.iterator.foreach{ v => print(v._1 + " ");
        v._2.iterator.foreach( b => print(b + " ")) }
      println()
    }

    //
    val graphParts = GraphImpl.buildSimpleFromEdges(edgeParts).zipPartitions(vertexParts) {
      (edgePartIter, vertexPartIter) =>
        val (pid, edgePart) = edgePartIter.next()
        val (allVerts, inHighVerts, outHighVerts) = vertexPartIter.next()
        Iterator((pid, new SimpleEdgeWithVertexPartition(edgePart.edges,
          allVerts, inHighVerts, outHighVerts)))
    }
    graphParts

  }
  */

  /*
  // from edge partitions, build the edgeWithVertices partitions
  def buildInEdges[ED: ClassTag](rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgeWithVertexPartition[ED])] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    val mixingPrime = 1125899906842597L % partNum

    // first partition the edges based on dstId (in edges partitioning)
    // repartition edges
    val ecut_in_edges = rdd.flatMap { part =>
      part._2.edges.map { e => (((e.dstId * mixingPrime) % partNum).toInt, e) }
    }.partitionBy (new HashPartitioner (partNum))

    // compute in degrees for each vertex,
    // for vertex with degrees greater than the threshold,
    // change the pid
    // TXH: 3.13 remove the inMaster hashMap
    val newEdgeParts = ecut_in_edges.mapPartitionsWithIndex { (epid, iter) =>
      val messages = iter.toArray
      // the inDegress can store all the vertices to this partition
      val inDegrees = new PrimitiveKeyOpenHashMap[VertexId, (Int, Byte)]
      val inEdges = new PrimitiveVector[Edge[ED]]
      val routingMsgs = new PrimitiveVector[(Long, Int)]
      val highDegreeMirrors = new PrimitiveVector[(Int, Long)]
      // val inMasters = new PrimitiveKeyOpenHashMap[VertexId, Byte]

      // count the indegrees
      messages.foreach { message =>
        inDegrees.changeValue (message._2.dstId, (1, 0x0), { m => (m._1 + 1, m._2) })
      }

      // set the mirror parts of each vertex, if one vertex v has a mirror on pid p,
      // the pos p of v's bitset should be set to 1
      messages.foreach { message =>
        // case of in low degree vertices
        val inDegree = inDegrees(message._2.dstId)._1
        if (inDegree <= threshold) {
          inDegrees.changeValue(message._2.dstId, (inDegree, 0x2),
            { m => (m._1, (m._2 | 0x2).toByte) })
          // inMasters.changeValue(message._2.srcId, )
          // (message._1, message._2, message._2.srcId)
          inEdges += message._2
          routingMsgs += (message._2.srcId, message._1)
        } else {
          // will be handled in buildOutEdges
          // should record this as high degree vertex in srcId's partition
          inDegrees.changeValue(message._2.dstId, (0, 0x0), { m => (0, 0x0) })
          val srcPid = ((message._2.srcId * mixingPrime) % partNum).toInt
          // (srcPid, null.asInstanceOf[Edge[ED]], message._2.dstId)
          highDegreeMirrors += (srcPid, message._2.dstId)
        }
      }

      // need to be repartitioned and merged to srcId's masterMap
      // val highDegreeMirrors = newEdges.filter(_._2 == null).map(m => (m._1, m._3))

      Iterator((newEdges, inDegrees))
    }.cache()

    val vertexParts = newEdgeParts
      .mapPartitionsWithIndex((pid, iter) => Iterator((pid, iter.next._2)))

    // the high degree vertices and where they act as in edge mirrors
    // these in edge mirrors should be compute after all in low degree
    // vertices have been computed
    val highDegreeVertices = newEdgeParts
      .flatMap(_._1.filter(_._2 == null)).map(record => (record._1, record._3))
      .partitionBy(new HashPartitioner(partNum))

    val newVertexParts = vertexParts.zipPartitions(highDegreeVertices) {
      (vertexPartIter, highDegreeIter) =>
        val (pid, vertexPart) = vertexPartIter.next()
        highDegreeIter.foreach(v => vertexPart.update(v._2, 0x4))
        Iterator((pid, vertexPart))
    }

    val routingTableParts = newEdgeParts
      .flatMap(_._1.filter(_._2 != null).map(edge => (edge._3, edge._1)))
      .partitionBy(new HashPartitioner(partNum))
      .mapPartitionsWithIndex { (vpid, iter) =>
        val messages = iter.toArray
        val pid2vid = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
        val masterPid = vpid

        for (msg <- iter) {
          val vid = msg._1
          val pid = msg._2
          pid2vid (pid) += vid
          // println(s"pid: $vpid, vid: $vid")
        }

        val p2v = pid2vid.map { vids => vids.trim ().array}
        // p2v.foreach{ vid => print("mirrors: ");
        // vid.iterator.foreach(m => print(s"$m ")); println}
        // p2v.update (masterPid, masters.iterator.toArray)
        Iterator((vpid, p2v))
    }

    println("InEdges")

    /*
    vertexParts.foreach { LaH =>
      println("Low Vertices")
      LaH._2.iterator.foreach{ v => print( v._1 + "  " )
          println }
    }
      println
*/
    // repartition the edges
    val edgeParts = newEdgeParts
      .flatMap(_._1.filter(_._1 != -1).map(edge => (edge._1, edge._2)))
      // .partitionBy(new HashPartitioner(partNum)) // no need to repartition here
      .map{ _._2 }

    // TODO: how to decrease the overhead here.
    val graphParts = GraphImpl.buildSimpleFromEdges(edgeParts)
      .zipPartitions(vertexParts) {
      (edgePartIter, vertexPartIter) =>
        val (pid, edgePart) = edgePartIter.next()
        val (_, inMasters) = vertexPartIter.next()
        val edgeSet = new OpenHashSet[Edge[ED]]
        edgePart.edges.foreach (edge => edgeSet.add(edge))
        Iterator((pid, edgeSet, inMasters))
    }
      .zipPartitions(routingTableParts) {
      (edgePartIter, routingTablePartIter) =>
        val (pid, edgePart, vertexPart) = edgePartIter.next()
        val (_, routingTable) = routingTablePartIter.next()
        Iterator((pid, new SimpleEdgeWithVertexPartition(edgePart, vertexPart, routingTable, null)))
    }
    graphParts
  }

  // distribute out edges based on srcIds, compute outdegrees of each vertex
  // repartition high degree vertices
  def buildOutEdges[ED: ClassTag](rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgeWithVertexPartition[ED])] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    val mixingPrime = 1125899906842597L % partNum

    val ecut_out_edges = rdd.flatMap {part =>
      part._2.edges.map {e => (((e.srcId * mixingPrime) % partNum).toInt, e)}
    }.forcePartitionBy (new HashPartitioner (partNum))

    // compute outdegree for each vertex,
    // for vertex with degrees greater than the threshold,
    // change the pid
    val newEdgeParts = ecut_out_edges
      .mapPartitions { iter =>
        val messages = iter.toArray

        // the outDegress can store all the vertices to this partition
        val outDegrees = new PrimitiveKeyOpenHashMap[VertexId, Int]
        // val allMasters = new PrimitiveKeyOpenHashMap[VertexId, Byte]
        val outMasters = new PrimitiveKeyOpenHashMap[VertexId, Byte]

        messages.foreach { message =>
          outDegrees.changeValue (message._2.srcId, 1, _ + 1)
        }

        val newEdges = messages.map { message =>
          if (outDegrees (message._2.srcId) <= threshold) {
            outMasters.changeValue(message._2.srcId, 0x1, {b: Byte => (b | 0x1).toByte})
            // inMasters.changeValue(message._2.srcId, )
            (message._1, message._2, message._2.srcId)

            // outMasters.changeValue(message._2.srcId, newBitSet,
            // {b: BitSet => b.set(message._1); b.set(partNum); b})
          } else {
            outMasters.changeValue(message._2.srcId, 0x0, {b: Byte => b })
            (-1, Edge(-1, -1, null.asInstanceOf[ED]), -1L)
          }
        }
        Iterator((newEdges, outMasters))
      }.cache()

    // repartition the edges
    val edgeParts = newEdgeParts
      .flatMap(_._1.filter(_._1 != -1).map(edge => (edge._1, edge._2)))
      // .partitionBy(new HashPartitioner(partNum)) // no need to repartition here
      .map{ _._2 }

    val vertexParts = newEdgeParts
      .mapPartitionsWithIndex((pid, iter) => Iterator((pid, iter.next._2)))

    // generate inRoutingTable
    // repartition vertices
    val routingTableParts = newEdgeParts
      .flatMap(_._1.filter(_._1 != -1).map(edge => (edge._3, edge._1)))
      .partitionBy(new HashPartitioner(partNum))
      .mapPartitionsWithIndex { (vpid, iter) =>
        val messages = iter.toArray
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
        // p2v.foreach{ vid => print("mirrors: ");
        // vid.iterator.foreach(m => print(s"$m ")); println}
        p2v.update (masterPid, masters.iterator.toArray)
        Iterator((vpid, p2v))
      }

    println("OutEdges")

    /*
    vertexParts.foreach { LaH =>
      println("High Vertices")
      LaH._2.iterator.foreach{ v => print( v + "  ")
        v._2.iterator.foreach{ b => print(b + " ")
          println
        }
      }
      println
    }
    */

    // too ugly here...
    val inEdges = rdd.map(part => (part._1, part._2.edges))
    //
    val graphParts = GraphImpl.buildSimpleFromEdges(edgeParts)
      .zipPartitions(vertexParts) {
      (edgePartIter, vertexPartIter) =>
        val (pid, edgePart) = edgePartIter.next()
        val (_, outMasters) = vertexPartIter.next()
        val edgeSet = new OpenHashSet[Edge[ED]]
        edgePart.edges.foreach (edge => edgeSet.add(edge))
        Iterator((pid, edgeSet, outMasters))
    }
      .zipPartitions(routingTableParts) {
        (edgePartIter, routingTableIter) =>
          val (pid, edgePart, vertexPart) = edgePartIter.next()
          val (_, routingTable) = routingTableIter.next()
          Iterator((pid, new SimpleEdgeWithVertexPartition(edgePart,
            vertexPart, null, routingTable)))
      }
    graphParts

  } */

  def buildAllEdges[ED : ClassTag](rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgeWithVertexPartition[ED])] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    val partitioner = new HashPartitioner(partNum)

    // first partition the edges based on dstId (in edges partitioning)
    // repartition edges
    val ecut_edges = rdd.flatMap { part =>
      part._2.edges.flatMap { e =>
        Iterator((partitioner.getPartition(e.dstId), (e, 0x0)), // for dstIds
          (partitioner.getPartition(e.srcId), (e, 0x1))) } // for srcIds
    }.partitionBy (new HashPartitioner (partNum))

    /*
    println("partitioned_edges")
    ecut_edges.foreachPartition { edges => edges.foreach(println); println }
    println
    */

    val tmp_edges = ecut_edges.mapPartitions { iter =>
      // get all the edges assigned for this partition
      val messages = iter.toArray
      // store the in and out degree of each src and dst
      val degrees = new PrimitiveKeyOpenHashMap[VertexId, (Int, Int)]
      // store the edges in this partition, may have duplicated edges
      // vertex types:
      // 0x1: out high degree
      // 0x2: in high degree
      // 0x4: out low degree
      // 0x8: in low degree
      // 0x10: src high mirror
      // 0x20: dst high mirror
      // 0x40: src as master
      // 0x80: dst as master
      val edges = Array.fill(numPartitions)(new PrimitiveKeyOpenHashMap[Edge[ED], Byte])
      // routing msgs for mirrors in this edge partition
      // val routingMsgs = new PrimitiveVector[(Long, Int)]
      // val masters = new PrimitiveKeyOpenHashMap[VertexId, Byte]

      messages.foreach { msg =>
        if (msg._2._2 == 0x0) {
          degrees.changeValue (msg._2._1.dstId, (1, 0), {m => (m._1 + 1, m._2)})
        } else {
          degrees.changeValue (msg._2._1.srcId, (0, 1), {m => (m._1, m._2 + 1)})
        }
      }

      // println("Compute Degrees")
      // degrees.foreach(println)

      val inEdges = messages.filter(_._2._2 == 0x0).map(_._2._1)

      /*
      println("in_edges")
      inEdges.foreach{ edge => println(edge) }
      println
      */

      inEdges.foreach { edge =>
        val inDegree = degrees (edge.dstId)._1
        // val outDegree = degrees (edge.dstId)._2
        if (inDegree <= threshold) {
          val pid = partitioner.getPartition(edge.dstId)
          edges(pid).update(edge, 0x88.toByte)
          // routingMsgs +=(msg._2._1.srcId, msg._1)
        } else {
          val pid = partitioner.getPartition(edge.srcId)
          edges(pid).update(edge, 0x60.toByte)
        }
      }

      val outEdges = messages.filter(_._2._2 == 0x1).map(_._2._1)

      /*
      println("out_edges")
      outEdges.foreach{ edge => println(edge) }
      println
      */

      outEdges.foreach { edge =>
        // val inDegree = degrees (edge.dstId)._1
        // println("outEdge foreach: edge: " + edge)

        val outDegree = degrees (edge.srcId)._2
        if (outDegree <= threshold) {
          val pid = partitioner.getPartition(edge.srcId)
          edges(pid).changeValue(edge, 0x44.toByte, b => (b | 0x44.toByte).toByte)
        } else {
          val pid = partitioner.getPartition(edge.dstId)
          edges(pid).changeValue(edge, 0x90.toByte, b => (b | 0x90.toByte).toByte)
        }
      }

      val allEdges = edges.zipWithIndex.flatMap {edgeMap =>
        edgeMap._1.map(edge => (edgeMap._2, edge))
      }

      allEdges.toIterator
    }

    println("tmp_edges")
    // tmp_edges.foreach{ edge => printf("%d (%s, %h)\n", edge._1, edge._2._1, edge._2._2)}

    val all_edges = tmp_edges.forcePartitionBy(new HashPartitioner(partNum))
      .mapPartitionsWithIndex { (pid, iter) =>
      val edgeMsgs = iter.map(_._2).toArray
      val edgeMap = new PrimitiveKeyOpenHashMap[Edge[ED], Byte]
      val highMasters = new OpenHashSet[VertexId]
      edgeMsgs.foreach { msg =>
        edgeMap.changeValue(msg._1, msg._2, b => (b | msg._2).toByte)
      }

      // filter high-high out edges
      val filteredEdges = edgeMap.iterator.filter(_._2 != 0x60.toByte).toArray

      Iterator((pid, filteredEdges))
    }.cache()

    println("All_edges")
    // all_edges.foreach{ edges => edges._2.foreach(e => printf("%s %h\n", e._1, e._2)); println }

    // compute the routing msgs for each vertex
    // possible cases:
    // 1. 0x44: src as low degree master, dst is low:   src: 0x1, dst: 0x8
    // 2. 0x64: src as low degree master, dst is high:  src: 0x1, dst: 0x80
    // 3. 0x88: dst as low degree master, src is low:   src: 0x2, dst: 0x10
    // 4. 0x98: dst as low degree master, src is high:  src: 0x8, dst: 0x10
    // 5. 0x90: dst as high degree master, src is high: src: 0x8, dst: 0x40
    // for each vertex, the type attrs are :
    // position, degree, master or mirror
    // 0x1: src & low & master
    // 0x2: src & low & mirror
    // 0x4: dst & low & master
    // 0x8: dst & low & mirror
    // 0x10: src & high & master
    // 0x20: src & high & mirror
    // 0x40: dst & high & master
    // 0x80: dst & high & mirror
    // a vertex in one partition can be both master and mirror
    val tmp_vertices = all_edges.mapPartitions { iter =>
      val edges = iter.toArray
      edges.flatMap { edgePart =>
        // val pid = (edgePart._1 & 0x3FFFFFFF)
        val pid = edgePart._1
        val vertices = new PrimitiveKeyOpenHashMap[VertexId, Byte]
        edgePart._2.foreach { edge =>
          edge._2 match {
            case 0x44 => vertices.changeValue(edge._1.srcId, 0x1.toByte,
              (b: Byte) => (b | 0x1.toByte).toByte)
              vertices.changeValue(edge._1.dstId, 0x20.toByte,
                (b: Byte) => (b | 0x20.toByte).toByte)
            case 0x64 => vertices.changeValue(edge._1.srcId, 0x1.toByte,
              (b: Byte) => (b | 0x1.toByte).toByte)
              vertices.changeValue(edge._1.dstId, 0x80.toByte,
                (b: Byte) => (b | 0x80.toByte).toByte)
            case 0x88 => vertices.changeValue(edge._1.srcId, 0x2.toByte,
              (b: Byte) => (b | 0x2.toByte).toByte)
              vertices.changeValue(edge._1.dstId, 0x10.toByte,
                (b: Byte) => (b | 0x10.toByte).toByte)
            case 0x98 => vertices.changeValue(edge._1.srcId, 0x8.toByte,
              (b: Byte) => (b | 0x8.toByte).toByte)
              vertices.changeValue(edge._1.dstId, 0x10.toByte,
                (b: Byte) => (b | 0x10.toByte).toByte)
            case 0x90 => vertices.changeValue(edge._1.srcId, 0x8.toByte,
              (b: Byte) => (b | 0x8.toByte).toByte)
              vertices.changeValue(edge._1.dstId, 0x40.toByte,
                (b: Byte) => (b | 0x40.toByte).toByte)
            case _ => new NotImplementedError()
          }
        }

        vertices.iterator.map(v => (v._1, (pid, v._2)))
      }.iterator
    }.forcePartitionBy(new HashPartitioner(partNum))

    val vertices = tmp_vertices.mapPartitionsWithIndex { (pid, iter) =>
      val routingTable = fromMsgs(partNum, iter)
      val masters = new PrimitiveKeyOpenHashMap[VertexId, Byte]
      routingTable.iterator.foreach { vpos =>
        masters.changeValue(vpos._1, vpos._2, (b: Byte) => (b | vpos._2).toByte)
      }
      Iterator((pid, (masters.iterator.toArray, routingTable)))
    }

    println("routing table")

    // vertices.foreach { iter => iter._2._1.foreach(v => printf("%s %x\n", v._1, v._2)); println }

    val newGraph = all_edges.zipPartitions(vertices) {
      (edgePartIter, vertexPartIter) =>
        val (pid, edgePart) = edgePartIter.next()
        val (_, vertexPart) = vertexPartIter.next()
        Iterator((pid, new SimpleEdgeWithVertexPartition(edgePart, vertexPart._1, vertexPart._2)))
    }

    println("edges")
    // newGraph.foreach { graph => graph._2.edges.foreach(println); println}
    println("masters")
    // newGraph.foreach { graph => graph._2.masters.foreach(println); println}

    newGraph
  }

  def fromEdgesWithVertices[ED : ClassTag](
    rdd: RDD[(PartitionID, SimpleEdgePartition[ED])]):
  RDD[(PartitionID, SimpleEdgeWithVertexPartition[ED])] = {
    buildAllEdges(rdd)
  }
}


