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

package org.apache.spark.graphloca

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite

import org.apache.spark.graphx.LocalSparkContext
import org.apache.spark.graphxpp.utils.PGStyle
import org.apache.spark.util.Utils

/**
 * Created by XinhuiTian on 16/12/22.
 */
class GraphLoadSuite extends SparkFunSuite with LocalSparkContext {

  test("GraphLoader.edgeListFile") {
    withSpark { sc =>

      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      // println(tmpDir.getAbsolutePath)
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) {
        writer.write(s"$i 0\n")
      }
      /*
      for (i <- (1 until 101)) {
        writer.write(s"0 $i\n")
      }
      */

      writer.close()

      try {
        val startTime = System.currentTimeMillis()
        // val graph = GraphLoader.edgeListFile(sc,
        //  "/Users/XinhuiTian/Downloads/roadNet-CA.txt", false, 10,
        //   edgePartitioner = "EdgePartition2D").cache()

        // val graph = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, false, 10)
        val graph = GraphLoader.edgeListFile(sc, "/Users/XinhuiTian/Downloads/roadNet-CA.txt", false, 500,
             edgePartitioner = "ObliviousVertexCut")
        graph.triplets.count()

        // println("Mirrors")
        // graph.edges.partitionsRDD.foreach{ part => part._2.getMirrors.foreach(println); println }

        //graph.edges.partitionsRDD.foreach{ part => part._2.changeVertexAttrs(i => i + 1).foreach(println); println}
        // graph.edges.partitionsRDD
        //graph.subGraphs.collect()
        //graph.subGraphs.partitionsRDD.foreach { iter => print(iter._1);
        //  iter._2.masterIterator.foreach(print); println }
        //graph.subGraphs.partitionsRDD.foreach { iter => print(iter._1);
        //  iter._2.mirrorIterator.foreach(print); println }
        //graph.vertices.partitionsRDD.foreach { iter => iter._2.iterator.foreach(println); println }
        // graph.vertices.partitionsRDD.foreach { iter => iter._2.shipVertexAttributes(10).foreach{ vb => print(vb._1); vb._2.iterator.foreach(print)}; println}

      } finally {
        Utils.deleteRecursively(tmpDir)
      }
    }
  }

  /*

  test("Ingress test") {
    withSpark { sc =>

      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      // println(tmpDir.getAbsolutePath)
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) {
        writer.write(s"$i 0\n")
      }
      /*
      writer.write("0 1\n")
      for (i <- (2 until 11)) {
        writer.write(s"$i 1\n")
      }

      for (i <- (2 until 10)) {
        writer.write(s"$i ${i + 1}\n")
      }

      for (i <- (2 until 10)) {
        writer.write(s"${i + 1} $i\n")
      } */

      writer.close()

      try {
        val startTime = System.currentTimeMillis()
        // val graph = GraphLoader.edgeListFile(sc,
        //   "/Users/XinhuiTian/Downloads/roadNet-CA.txt", false, 48,
        //   edgePartitioner = "EdgePartition2D").cache()
         val graph = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, false, 10,
          edgePartitioner = "BiEdgePartition", threshold = 10).cache()

        graph.edges.partitionsRDD.foreach{ part =>
          println("Masters")
          println(part._2.getMasters.size)
          println("Mirror size")
          println(part._2.getMirrors.size)
          // println("Mirrors")
          // part._2.getMirrors.foreach(println)
          println
        }
        val period = System.currentTimeMillis() - startTime
        println("Loading Time: " + period)
        println("Replications: " + GraphImpl.getVertRNum(graph))

        // println("Mirrors")
        // graph.edges.partitionsRDD.foreach{ part => part._2.getMirrors.foreach(println); println }

        //graph.edges.partitionsRDD.foreach{ part => part._2.changeVertexAttrs(i => i + 1).foreach(println); println}
        // graph.edges.partitionsRDD
        //graph.subGraphs.collect()
        //graph.subGraphs.partitionsRDD.foreach { iter => print(iter._1);
        //  iter._2.masterIterator.foreach(print); println }
        //graph.subGraphs.partitionsRDD.foreach { iter => print(iter._1);
        //  iter._2.mirrorIterator.foreach(print); println }
        //graph.vertices.partitionsRDD.foreach { iter => iter._2.iterator.foreach(println); println }
        // graph.vertices.partitionsRDD.foreach { iter => iter._2.shipVertexAttributes(10).foreach{ vb => print(vb._1); vb._2.iterator.foreach(print)}; println}

      } finally {
        Utils.deleteRecursively(tmpDir)
      }
    }
  } */

  test("PGStyle Test") {
    withSpark {sc =>
      val srcIds = Array(2, 1, 0, 1, 2, 3)
      val dstIds = (1 until 7).toArray

      println(srcIds)
      println(dstIds.length)

      val ((finalSrcIds, permute), finalDstIds) = PGStyle.realEdgeSort(srcIds, dstIds)

      println("src dst permute")
      for (i <- 0 until finalSrcIds.size) {
        println(s"${finalSrcIds(i)} ${finalDstIds(i)} ${permute(i)}")
      }
    }

  }

}
