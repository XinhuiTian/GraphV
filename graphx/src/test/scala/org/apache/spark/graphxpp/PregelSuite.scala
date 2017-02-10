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

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx.LocalSparkContext

/**
 * Created by XinhuiTian on 17/1/6.
 */
class PregelSuite extends SparkFunSuite with LocalSparkContext {

  test("1 iteration") {
    withSpark {sc =>
      val n = 5
      val starEdges = (1 to n).map(x => (0: VertexId, x: VertexId))
      val star = Graph.fromEdgeTuples(sc.parallelize(starEdges, 3), "v").cache()
      val result = Pregel(star, 0)(
        (vid, attr, msg) => attr,
        et => Iterator.empty,
        (a: Int, b: Int) => throw new Exception("mergeMsg run unexpectedly"))
      assert(result.verticesWithAttrs.collect.toSet === star.verticesWithAttrs.collect.toSet)
    }
  }

  test("chain propagation") {
    withSpark { sc =>
      val n = 50
      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until n).map(x => (x: VertexId, x + 1: VertexId)), 10),
        0).cache()
      chain.edges.partitionsRDD.foreach { part => part._2.getMasters.foreach(println); println }
      assert(chain.verticesWithAttrs.collect.toSet === (1 to n).map(x => (x: VertexId, 0)).toSet)
      chain.edges.partitionsRDD.foreach { part =>
        val table = part._2.getRoutingTable
        for (i <- 0 until table.length) {
          print(i + ": ")
          for (j <- 0 until table(i).length) {
            print(table(i)(j) + " ")
          }
          println
        }
        println
      }

      println("Check 1")
      val chainWithSeed = chain.mapVertices { (vid, attr) => if (vid <= 1) 1 else 0 }
      // assert(chainWithSeed.verticesWithAttrs.collect.toSet ===
      //  Set((1: VertexId, 1)) ++ (2 to n).map(x => (x: VertexId, 0)).toSet)
      println("Check 2")
      val result = Pregel(chainWithSeed, 0)(
        (vid, attr, msg) => math.max(msg, attr),
        et => if (et.dstAttr != et.srcAttr) Iterator((et.dstId, et.srcAttr)) else Iterator.empty,
        (a: Int, b: Int) => math.max(a, b))

      // result.edges.partitionsRDD.foreach()
      println(result.verticesWithAttrs.collect.toSet)

      //assert(result.verticesWithAttrs.collect.toSet ===
      //  chain.mapVertices { (vid, attr) => attr + 1 }.verticesWithAttrs.collect.toSet)
    }
  }
}
