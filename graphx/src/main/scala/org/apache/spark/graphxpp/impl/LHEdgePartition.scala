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

import scala.reflect.ClassTag

import org.apache.spark.graphxpp._
import org.apache.spark.graphxpp.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.BitSet
// scalastyle:off println
/**
 * Created by XinhuiTian on 17/3/10.
 */
class LHEdgePartition[
@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassTag, VD: ClassTag](
  localSrcIds: Array[Int],
  localDstIds: Array[Int],
  data: Array[ED],
  srcIndex: PrimitiveKeyOpenHashMap[Int, Int],
  dstIndex: PrimitiveKeyOpenHashMap[Int, Int],
  // masters: Array[Iterable[PartitionID]],
  outRoutingTable: Array[Array[Int]],
  inRoutingTable: Array[Array[Int]],
  global2local: PrimitiveKeyOpenHashMap[VertexId, Int],
  local2global: Array[VertexId],
  masterAttrs: Array[VD],
  mirrorAttrs: Array[VD],
  masterMask: BitSet,
  numPartitions: Int,
  activeSet: Option[VertexSet]) {




}
