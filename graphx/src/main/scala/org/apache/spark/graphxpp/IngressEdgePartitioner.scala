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

import org.apache.spark.graphxpp.utils.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.OpenHashSet

import scala.reflect.ClassTag

import org.apache.spark.HashPartitioner
import org.apache.spark.graphxpp.impl.{GraphImpl, SimpleEdgePartition}
import org.apache.spark.rdd.RDD

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

        // compute ingress for each vertex,
        // for vertex with degrees greater than the threshold,
        // change the pid
        val newEdges = ecut_edges.mapPartitions { iter =>
          val messages = iter.toArray
          val inDegrees = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
          messages.foreach { message =>
            inDegrees.changeValue (message._2.dstId, 1, _ + 1)
          }
          messages.map {message =>
            if (inDegrees (message._2.dstId) <= threshold) {
              message
            } else {
              ((message._2.srcId * mixingPrime) % partNum, message._2)
            }
          }.toIterator
        }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
        GraphImpl.buildSimpleFromEdges(newEdges)
      case AggregateDirection.OutOnly =>
        val ecut_edges = rdd.flatMap {part =>
          part._2.edges.map {e => ((e.srcId * mixingPrime) % partNum, e)}
        }.partitionBy (new HashPartitioner (partNum))

        // compute ingress for each vertex,
        // for vertex with degrees greater than the threshold,
        // change the pid
        val newEdges = ecut_edges.mapPartitions { iter =>
          val messages = iter.toArray
          val outDegrees = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
          messages.foreach { message =>
            outDegrees.changeValue (message._2.dstId, 1, _ + 1)
          }
          messages.map {message =>
            if (outDegrees (message._2.dstId) <= threshold) {
              message
            } else {
              ((message._2.srcId * mixingPrime) % partNum, message._2)
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
          val edgeSet = new OpenHashSet[Edge[ED]](in_part._2.edges.length)
          in_part._2.edges.foreach { edge => edgeSet.add (edge) }
          out_iterator.flatMap (_._2.edges).foreach { edge => edgeSet.add (edge) }
          val newEdges = edgeSet.iterator.toArray
          (pid, SimpleEdgePartition(newEdges))
        }
    }
    bi_edge_part
  }
}


