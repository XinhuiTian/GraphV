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
import org.apache.spark.graphxpp.impl.EdgeInfos

// scalastyle:off println
/**
 * Created by XinhuiTian on 17/3/28.
 */
class CollectionSuite extends SparkFunSuite with LocalSparkContext {
  test("PGCsrMap") {
    withSpark { sc =>
      val srcIds =    Array(1, 1, 1, 2, 2, 3, 3, 0)
      val dstIds =    Array(2, 3, 0, 0, 3, 2, 0, 3)
      val edgeAttrs = Array(0, 1, 2, 3, 4, 5, 6, 7)

      val edges = EdgeInfos.edgeSort(srcIds, dstIds, edgeAttrs)

      println("In Edges")
      for (i <- 0 until 4) {
        edges.getInEdgeAttrs(i).foreach(e => println("("+ e._1 + "," + i + ") " + e._2))
      }

      println("Out Edges")
      for (i <- 0 until 5) {
        edges.getOutEdgeAttrs(i).foreach(e => println("("+ i + "," + e._1 + ") " + e._2))
      }
    }
  }

}
