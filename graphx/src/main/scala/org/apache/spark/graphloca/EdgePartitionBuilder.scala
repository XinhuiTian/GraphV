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

import scala.reflect.ClassTag

import org.apache.spark.graphloca.impl.SimpleEdgePartition
import org.apache.spark.util.collection.PrimitiveVector

/**
 * Created by XinhuiTian on 17/3/29.
 */
class EdgePartitionBuilder[ED: ClassTag]
(size: Int = 64) {
  val edges = new PrimitiveVector[Edge[ED]](size)
  def edgeArray: Iterator[Edge[ED]] = edges.trim.array.toIterator
  def add(edge: Edge[ED]) {
    edges += edge
  }

  def add(src: Int, dst: Int, v: ED) = {
    edges += Edge(src, dst, v)
  }

  def toSimpleEdgePartition: SimpleEdgePartition[ED] = {
    // val vertices = new PrimitiveKeyOpenHashMap[VertexId, Int]
    new SimpleEdgePartition(edges.trim.array)
  }
}
