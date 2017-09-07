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

package org.apache.spark.graphxp.impl

import scala.reflect.ClassTag

import org.apache.spark.graphxp

import org.apache.spark.graphxp._
import org.apache.spark.graphxp.util.collection
import org.apache.spark.graphxp.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

/** Stores vertex attributes to ship to an edge partition. */
// changed by TXH, change the routingtable to a localroutingtable structure
private[graphxp]
class VertexAttributeBlock[VD: ClassTag](val localIds: Array[Int], val attrs: Array[VD])
  extends Serializable {
  def iterator: Iterator[(Int, VD)] =
    (0 until localIds.length).iterator.map { i => (localIds(i), attrs(i)) }
}

private[graphxp]
object ShippableVertexPartition {
  /** Construct a `ShippableVertexPartition` from the given vertices without any routing table. */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)]): ShippableVertexPartition[VD] =
    apply(iter, LocalRoutingTablePartition.empty, null.asInstanceOf[VD], (a, b) => a)

  /**
   * Construct a `ShippableVertexPartition` from the given vertices with the specified routing
   * table, filling in missing vertices mentioned in the routing table using `defaultVal`.
   */
  def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: LocalRoutingTablePartition, defaultVal: VD)
    : ShippableVertexPartition[VD] =
    apply(iter, routingTable, defaultVal, (a, b) => a)

  /**
   * Construct a `ShippableVertexPartition` from the given vertices with the specified routing
   * table, filling in missing vertices mentioned in the routing table using `defaultVal`,
   * and merging duplicate vertex attribute with mergeFunc.
   */

  def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: LocalRoutingTablePartition, defaultVal: VD,
      mergeFunc: (VD, VD) => VD): ShippableVertexPartition[VD] = {
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    // Merge the given vertices using mergeFunc
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    // Fill in missing vertices mentioned in the routing table
    routingTable.iterator.foreach { vid =>
      map.changeValue(vid, defaultVal, identity)
    }

    new ShippableVertexPartition(map.keySet, map._values, map.keySet.getBitSet, routingTable)
  }
  /*
  def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: LocalRoutingTablePartition, defaultVal: VD,
      mergeFunc: (VD, VD) => VD): ShippableVertexPartition[VD] = {
    // val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    var currentVertexId = -1
    val map = new PrimitiveVector[VertexId]
    val values = new PrimitiveVector[VD]
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]

    // Merge the given vertices using mergeFunc
    iter.foreach { pair =>
      global2local.changeValue(pair._1,
        { currentVertexId += 1; map += pair._1; values += pair._2; currentVertexId}, identity)
    }
    // Fill in missing vertices mentioned in the routing table
    routingTable.iterator.foreach { vid =>
      // map.changeValue(vid, defaultVal, identity)
      global2local.changeValue(vid,
      { currentVertexId += 1; map += vid; values += defaultVal; currentVertexId}, identity)
    }

    val mask = new BitSet(currentVertexId + 1)
    mask.setUntil(currentVertexId + 1)

    val l2lRoutingTable = routingTable.toL2LRoutingTable(global2local)

    new ShippableVertexPartition(map.trim().toArray, values.trim.toArray, mask, l2lRoutingTable)
  }
  */



  import scala.language.implicitConversions

  /**
   * Implicit conversion to allow invoking `VertexPartitionBase` operations directly on a
   * `ShippableVertexPartition`.
   */
  implicit def shippablePartitionToOps[VD: ClassTag](partition: ShippableVertexPartition[VD])
    : ShippableVertexPartitionOps[VD] = new ShippableVertexPartitionOps(partition)

  /**
   * Implicit evidence that `ShippableVertexPartition` is a member of the
   * `VertexPartitionBaseOpsConstructor` typeclass. This enables invoking `VertexPartitionBase`
   * operations on a `ShippableVertexPartition` via an evidence parameter, as in
   * [[VertexPartitionBaseOps]].
   */
  implicit object ShippableVertexPartitionOpsConstructor
    extends VertexPartitionBaseOpsConstructor[ShippableVertexPartition] {
    def toOps[VD: ClassTag](partition: ShippableVertexPartition[VD])
      : VertexPartitionBaseOps[VD, ShippableVertexPartition] = shippablePartitionToOps(partition)
  }
}

/**
 * A map from vertex id to vertex attribute that additionally stores edge partition join sites for
 * each vertex attribute, enabling joining with an [[graphxp.EdgeRDD]].
 *
 * changed by TXH, change the openHashSet to a local array
 */
private[graphxp]
class ShippableVertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    // val index: Array[VertexId],
    val values: Array[VD],
    val mask: BitSet,
    val routingTable: LocalRoutingTablePartition)
  extends VertexPartitionBase[VD] {

  // var localIndex: VertexIdToIndexMap = null
  // var localValues: Array[VD] = Array.empty[VD]

  /** Return a new ShippableVertexPartition with the specified routing table. */
  def withRoutingTable(_routingTable: LocalRoutingTablePartition): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, values, mask, _routingTable)
  }

  /**
   * Generate a `VertexAttributeBlock` for each edge partition keyed on the edge partition ID. The
   * `VertexAttributeBlock` contains the vertex attributes from the current partition that are
   * referenced in the specified positions in the edge partition.
   */
  def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): Iterator[(PartitionID, VertexAttributeBlock[VD])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val initialSize = if (shipSrc && shipDst) routingTable.partitionSize(pid) else 64
      val localIds = new PrimitiveVector[Int](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, shipSrc, shipDst) { vid =>
        if (isDefined(vid._1)) {
          localIds += vid._2
          attrs += this(vid._1)
        }
        i += 1
      }
      (pid, new VertexAttributeBlock(localIds.trim().array, attrs.trim().array))
    }
  }

  /**
   * Generate a `VertexId` array for each edge partition keyed on the edge partition ID. The array
   * contains the visible vertex ids from the current partition that are referenced in the edge
   * partition.
   */
  // changed by TXH
  def shipVertexIds(): Iterator[(PartitionID, Array[Int])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val localVids = new PrimitiveVector[Int](routingTable.partitionSize(pid))
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, true, true) { vid =>
        if (isDefined(vid._1)) {
          localVids += vid._2
        }
        i += 1
      }
      (pid, localVids.trim().array)
    }
  }
}

private[graphxp] class ShippableVertexPartitionOps[VD: ClassTag](self: ShippableVertexPartition[VD])
  extends VertexPartitionBaseOps[VD, ShippableVertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, self.values, self.mask, self.routingTable)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): ShippableVertexPartition[VD2] = {
    new ShippableVertexPartition(self.index, values, self.mask, self.routingTable)
  }

  def withMask(mask: BitSet): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(self.index, self.values, mask, self.routingTable)
  }
}
