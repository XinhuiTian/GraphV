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
package org.apache.spark.graphxpp.impl

import org.apache.spark.graphxpp._
import org.apache.spark.util.collection.{OpenHashSet, PrimitiveVector}

/**
 * Created by XinhuiTian on 17/3/17.
 */

object RoutingTablePartition {

  type RoutingTableMessage = (VertexId, (Int, Byte))
  private def vidFromMessage(msg: RoutingTableMessage): VertexId = msg._1
  private def pidFromMessage(msg: RoutingTableMessage): PartitionID = msg._2._1
  private def positionFromMessage(msg: RoutingTableMessage): Byte = msg._2._2

  def fromMsgs(numEdgePartitions: Int, iter: Iterator[RoutingTableMessage])
  : RoutingTablePartition = {
    val pid2vid = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId])
    val pid2byte = Array.fill(numEdgePartitions)(new PrimitiveVector[Byte])
    for (msg <- iter) {
      val vid = vidFromMessage(msg)
      val pid = pidFromMessage(msg)
      val position = positionFromMessage(msg)
      pid2vid(pid) += vid
      pid2byte(pid) += position
    }

    new RoutingTablePartition(pid2vid.zipWithIndex.map {
      case (vids, pid) => (vids.trim().array, pid2byte(pid).trim().array)
    })
  }
}

class RoutingTablePartition(
  private val routingTable: Array[(Array[VertexId], Array[Byte])]) extends Serializable {
  def iterator: Iterator[(VertexId, Byte)] = routingTable.flatMap { table =>
    table._1.zip(table._2) }.toIterator
  def vertices: Iterator[VertexId] = {
    val vertexSet = new OpenHashSet[VertexId]
    this.iterator.foreach(v => vertexSet.add(v._1))
    vertexSet.iterator
  }
}

