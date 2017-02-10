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

import org.apache.spark.graphxpp.impl.{EdgePartition, EdgeRDDImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition, Dependency, SparkContext}

import scala.reflect.ClassTag

/**
 * Created by XinhuiTian on 16/11/27.
 */
abstract class EdgeRDD[ED](
  sc: SparkContext,
  deps: Seq[Dependency[_]]) extends RDD[Edge[ED]](sc, deps) {

  // scalastyle:off structural.type
  def partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])] forSome { type VD }
  // scalastyle:on structural.type

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val p = firstParent[(PartitionID, EdgePartition[ED, _])].iterator(part, context)
    if (p.hasNext) {
      // TODO: why copy?
      p.next()._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }
}

object EdgeRDD {

  /**
   * Creates an EdgeRDD from already-constructed edge partitions.
   *
   * @tparam ED the edge attribute type
   * @tparam VD the type of the vertex attributes that may be joined with the returned EdgeRDD
   */
  def fromEdgePartitions[ED: ClassTag, VD: ClassTag](
    edgePartitions: RDD[(PartitionID, EdgePartition[ED, VD])]): EdgeRDDImpl[ED, VD] = {
    new EdgeRDDImpl(edgePartitions)
  }
}
