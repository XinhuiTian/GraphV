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

import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class GraphLoaderSuite extends SparkFunSuite with LocalSparkContext {

  test("GraphLoader.edgeListFile") {
    withSpark { sc =>
      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) writer.write(s"$i 0\n")
      // writer.write("3 1\n")
      // writer.write("6 1\n")
      // writer.write("6 4\n")
      /* writer.write("1 6\n")
      writer.write("4 1\n")
      writer.write("4 2\n")
      writer.write("2 1\n")
      writer.write("5 2\n")
      writer.write("5 1\n")
      */

      writer.close()
      try {
         val graph = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, numEdgePartitions = 10)
           .partitionBy(PartitionStrategy.EdgePartition2D)
        val startTime = System.currentTimeMillis()
        // val graph = GraphLoader.edgeListFile(sc, "/Users/XinhuiTian/Downloads/soc-Epinions1.txt", numEdgePartitions = 100)
        //   .partitionBy(PartitionStrategy.EdgePartition2D)
        val period = System.currentTimeMillis() - startTime
        println("Loading Time: " + period)

        println(graph.outComRatio)
        println(graph.inComRatio)

        /*
        val neighborAttrSums = graph.aggregateMessages[Int](
          ctx => ctx.sendToDst(ctx.srcAttr),
          _ + _)
        assert(neighborAttrSums.collect.toSet === Set((0: VertexId, 100)))*/
      } finally {
        // Utils.deleteRecursively(tmpDir)
      }
    }
  }
}
