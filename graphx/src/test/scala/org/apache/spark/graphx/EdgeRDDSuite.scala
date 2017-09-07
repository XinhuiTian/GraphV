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

package org.apache.spark.graphx

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite

import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

class EdgeRDDSuite extends SparkFunSuite with LocalSparkContext {

  test("cache, getStorageLevel") {
    // test to see if getStorageLevel returns correct value after caching
    withSpark { sc =>
      val verts = sc.parallelize(List((0L, 0), (1L, 1), (1L, 2), (2L, 3), (2L, 3), (2L, 3)))
      val edges = EdgeRDD.fromEdges(sc.parallelize(List.empty[Edge[Int]]))
      assert(edges.getStorageLevel == StorageLevel.NONE)
      edges.cache()
      assert(edges.getStorageLevel == StorageLevel.MEMORY_ONLY)
    }
  }

  /*
  test("count degrees") {
    withSpark { sc =>
      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) writer.write(s"$i 0\n")
      writer.close()
      try {
        // val graph = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, numEdgePartitions = 10)
        val startTime = System.currentTimeMillis()
        val graph = GraphLoader.edgeListFile(sc, "/Users/XinhuiTian/Downloads/roadNet-CA.txt", numEdgePartitions = 10)
          // .partitionBy(PartitionStrategy.EdgePartition2D)
        // val graph = GraphLoader.edgeListFile(sc, "/Users/XinhuiTian/Downloads/wiki-Vote.txt", numEdgePartitions = 100).partitionBy(PartitionStrategy.EdgePartition2D)
        // val graph = GraphLoader.edgeListFile(sc, "/Users/XinhuiTian/Downloads/soc-Epinions1.txt", numEdgePartitions = 1000).partitionBy(PartitionStrategy.EdgePartition2D)
        val period = System.currentTimeMillis() - startTime
        println("Loading Time: " + period)
        val startDTime = System.currentTimeMillis()
        val inDegrees = graph.inDegrees
        inDegrees.count
        val endDTime = System.currentTimeMillis()
        println("Compute in degree, time: " + (endDTime - startDTime))
        val inDegreesFromEdges = graph.edges.maxInDegreeCounts
        println("real partition number: " + graph.edges.getNumPartitions)
        val sameInCounts = inDegrees.zipPartitions(inDegreesFromEdges) { (globalIter, localIter) =>
          val globalArray = globalIter.toArray
          var sameCount = 0
          localIter.foreach { local =>
            globalArray.foreach{ global =>
              if (global._1 == local._1) {
                if (global._2 == local._2)
                  sameCount += 1
              }
            }
          }
          Iterator(sameCount)
        }.sum()
        println(sameInCounts)


        val outDegrees = graph.outDegrees
        val outDegreesFromEdges = graph.edges.maxOutDegreeCounts
        // outDegreesFromEdges.foreachPartition{ part => part.foreach(println); println}
        val sameOutCounts = outDegrees.zipPartitions(outDegreesFromEdges) { (globalIter, localIter) =>
          val globalArray = globalIter.toArray
          var sameCount = 0
          localIter.foreach { local =>
            globalArray.foreach{ global =>
              if (global._1 == local._1) {
                if (global._2 == local._2)
                  // println(global._1)
                  sameCount += 1
              }
            }
          }
          Iterator(sameCount)
        }.sum()
        println(sameOutCounts)
        println("Total Vertices: " + graph.numVertices)
        println("Total Edges: "+ graph.numEdges)

        /*
        val neighborAttrSums = graph.aggregateMessages[Int](
          ctx => ctx.sendToDst(ctx.srcAttr),
          _ + _)
        assert(neighborAttrSums.collect.toSet === Set((0: VertexId, 100)))*/
      } finally {
        // Utils.deleteRecursively(tmpDir)
      }
    }
  }*/

}
