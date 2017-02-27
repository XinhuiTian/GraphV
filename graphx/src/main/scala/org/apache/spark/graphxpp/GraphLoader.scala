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

// scalastyle:off println

import org.apache.spark.SparkContext
import org.apache.spark.graphxpp.impl.GraphImpl
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

/**
 * Created by XinhuiTian on 16/11/28.
 * Instead of directly creating edge partition,
 * leave the work to specific ingress partitioner
 */
object GraphLoader extends Logging {
  def edgeListFile(
    sc: SparkContext,
    path: String,
    canonicalOrientation: Boolean = false,
    numEdgePartitions: Int = -1,
    edgePartitioner: String = "",
    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    // partitionStrategy: PartitionStrategy = PartitionStrategy.EdgePartition1D)
  : GraphImpl[Int, Int] =
   //: Unit =
  {
    val startTime = System.currentTimeMillis

    // Parse the edge data table directly into edge partitions
    val lines =
      if (numEdgePartitions > 0) {
        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
      } else {
        sc.textFile(path)
      }

    // println("Lines")
    // lines.foreach(println)

    // new RDD[edge]
    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[Int]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line)
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          builder.add(Edge(srcId, dstId, 1))
          // srcId
          // new Edge(srcId, dstId, 1)
        }
        // else new Edge(0, 0, 1)
      }
      Iterator((pid, builder.toEdgePartition))
    }.cache()

    edges.count()

    println("It took %d ms to load the edges".format(System.currentTimeMillis - startTime))

    // edges.foreach(println)
    // edges.foreach{ part => part._2.edgeArray.foreach(println); println}
    // edges.foreach(part => part._2..foreach(println))
    var finalEdges = edges
    if (edgePartitioner != "") {
      finalEdges = GraphImpl.partitionSimplePartitions(finalEdges,
        numEdgePartitions, edgePartitioner)
    }

    finalEdges.foreach{ part => part._2.edges.foreach(println); println}


    GraphImpl.fromEdgesSimple(finalEdges, numEdgePartitions,
      defaultVertexAttr = 1, edgeStorageLevel, vertexStorageLevel)
  }

}
