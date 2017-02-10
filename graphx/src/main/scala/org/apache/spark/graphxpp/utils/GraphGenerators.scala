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

package org.apache.spark.graphxpp.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphxpp.{VertexId, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by XinhuiTian on 17/1/3.
 */
object GraphGenerators {
  def starGraph(sc: SparkContext, nverts: Int): Graph[Int, Int] = {
    val edges: RDD[(VertexId, VertexId)] = sc.parallelize(1 until nverts)
      .map(vid => (vid: VertexId, 0: VertexId))
    Graph.fromEdgeTuples(edges, 1, None, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
  }
}
