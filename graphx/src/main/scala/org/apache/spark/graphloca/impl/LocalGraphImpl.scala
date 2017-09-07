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
package org.apache.spark.graphloca.impl

import scala.reflect.ClassTag

import org.apache.spark.{HashPartitioner, OneToOneDependency}
import org.apache.spark.graphloca.{LocalGraph, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by XinhuiTian on 17/3/30.
 */
class LocalGraphImpl[VD: ClassTag, ED: ClassTag](
  @transient override val partitionsRDD: RDD[(PartitionID, LocalGraphPartition[VD, ED])],
  val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends LocalGraph[VD, ED](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  val vertices = partitionsRDD.flatMap(part => part._2.getMasters)

  override val partitioner =
    partitionsRDD.partitioner.orElse(Some(new HashPartitioner(partitions.length)))

  def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2): LocalGraphImpl[VD2, ED] = {
    this.withPartitionsRDD(this.partitionsRDD.mapPartitions(_.flatMap {part =>
      Iterator((part._1, part._2.mapVertices(f)))
    }))
  }

  private[graphloca] def withPartitionsRDD[VD2: ClassTag, ED2: ClassTag](
    partitionsRDD: RDD[(PartitionID, LocalGraphPartition[VD2, ED2])]): LocalGraphImpl[VD2, ED2] = {
    new LocalGraphImpl (partitionsRDD, this.targetStorageLevel)
  }

}
