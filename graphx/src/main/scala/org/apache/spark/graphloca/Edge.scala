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

import org.apache.spark.util.collection.SortDataFormat

/**
 * Created by XinhuiTian on 17/3/29.
 */
case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
  var srcId: VertexId = 0,
  var dstId: VertexId = 0,
  var attr: ED = null.asInstanceOf[ED])
  extends Serializable {
}

case class LocalEdge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
  var srcId: Int = 0,
  var dstId: Int = 0,
  var attr: ED = null.asInstanceOf[ED])
  extends Serializable {
}

object LocalEdge {
  private[graphloca] def lexicographicOrdering[ED] = new Ordering[LocalEdge[ED]] {
    override def compare(a: LocalEdge[ED], b: LocalEdge[ED]): Int = {
      if (a.srcId == b.srcId) {
        if (a.dstId == b.dstId) 0
        else if (a.dstId < b.dstId) -1
        else 1
      } else if (a.srcId < b.srcId) -1
      else 1
    }
  }

  private[graphloca] def edgeArraySortDataFormat[ED] = new SortDataFormat[LocalEdge[ED], Array[LocalEdge[ED]]] {
    override def getKey(data: Array[LocalEdge[ED]], pos: Int): LocalEdge[ED] = {
      data(pos)
    }

    override def swap(data: Array[LocalEdge[ED]], pos0: Int, pos1: Int): Unit = {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    override def copyElement(
      src: Array[LocalEdge[ED]], srcPos: Int,
      dst: Array[LocalEdge[ED]], dstPos: Int) {
      dst(dstPos) = src(srcPos)
    }

    override def copyRange(
      src: Array[LocalEdge[ED]], srcPos: Int,
      dst: Array[LocalEdge[ED]], dstPos: Int, length: Int) {
      System.arraycopy(src, srcPos, dst, dstPos, length)
    }

    override def allocate(length: Int): Array[LocalEdge[ED]] = {
      new Array[LocalEdge[ED]](length)
    }
  }
}

/**
 * The direction of a directed edge relative to a vertex.
 */
class EdgeDirection private (private val name: String) extends Serializable {
  /**
   * Reverse the direction of an edge.  An in becomes out,
   * out becomes in and both and either remain the same.
   */
  def reverse: EdgeDirection = this match {
    case EdgeDirection.In => EdgeDirection.Out
    case EdgeDirection.Out => EdgeDirection.In
    case EdgeDirection.Either => EdgeDirection.Either
    case EdgeDirection.Both => EdgeDirection.Both
  }

  override def toString: String = "EdgeDirection." + name

  override def equals(o: Any): Boolean = o match {
    case other: EdgeDirection => other.name == name
    case _ => false
  }

  override def hashCode: Int = name.hashCode
}


/**
 * A set of [[EdgeDirection]]s.
 */
object EdgeDirection {
  /** Edges arriving at a vertex. */
  final val In: EdgeDirection = new EdgeDirection("In")

  /** Edges originating from a vertex. */
  final val Out: EdgeDirection = new EdgeDirection("Out")

  /** Edges originating from *or* arriving at a vertex of interest. */
  final val Either: EdgeDirection = new EdgeDirection("Either")

  /** Edges originating from *and* arriving at a vertex of interest. */
  final val Both: EdgeDirection = new EdgeDirection("Both")
}

/**
 * Criteria for filtering edges based on activeness. For internal use only.
 */
object EdgeActiveness extends Enumeration {
  type EdgeActiveness = Value
  val Neither, SrcOnly, DstOnly, Both, Either = Value
}
