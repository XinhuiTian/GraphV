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

import org.apache.spark.graphxpp.impl.SimpleEdgePartition
import org.apache.spark.util.collection.PrimitiveVector

import scala.reflect.ClassTag

/**
 * Created by XinhuiTian on 16/12/25.
 */
class EdgePartitionBuilder[ED: ClassTag]
  (size: Int = 64) {
  val edges = new PrimitiveVector[Edge[ED]](size)
  def edgeArray: Iterator[Edge[ED]] = edges.trim.array.toIterator
  def add(edge: Edge[ED]) {
    edges += edge
  }

  def add(src: Int, dst: Int, v: ED): Unit = {
    edges += Edge(src, dst, v)
  }

  def toEdgePartition: SimpleEdgePartition[ED] = {
    new SimpleEdgePartition(edges.iterator)
  }
}
