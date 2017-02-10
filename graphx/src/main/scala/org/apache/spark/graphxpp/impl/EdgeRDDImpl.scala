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

import org.apache.spark.util.collection.PrimitiveVector
import org.apache.spark.{HashPartitioner, OneToOneDependency}
import org.apache.spark.graphxpp._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Created by XinhuiTian on 16/12/22.
 */
class EdgeRDDImpl[ED: ClassTag, VD: ClassTag] (
  @transient override val partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])],
  val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends EdgeRDD[ED](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  val vertices = partitionsRDD.flatMap(part => part._2.getMasters)

  def verticesWithAttrs = partitionsRDD.flatMap(part => part._2.getMastersWithAttr)

  def mastersWithAttrs: RDD[(PartitionID, Iterator[(VertexId, VD)])] = partitionsRDD.map(part =>
    (part._1, part._2.getMastersWithAttr.toIterator))

  def localMastersWithAttrs: RDD[(PartitionID, Array[VD])] = partitionsRDD.map(part =>
    (part._1, part._2.getLocalMastersWithAttr))

  /**
   * If `partitionsRDD` already has a partitioner, use it. Otherwise assume that the
   * [[PartitionID]]s in `partitionsRDD` correspond to the actual partitions and create a new
   * partitioner that allows co-partitioning with `partitionsRDD`.
   */
  override val partitioner =
    partitionsRDD.partitioner.orElse(Some(new HashPartitioner(partitions.length)))

  /** Persists the vertex partitions at `targetStorageLevel`, which defaults to MEMORY_ONLY. */
  override def cache(): this.type = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  def upgrade: EdgeRDDImpl[ED, VD] = {
    val msgs = shipMasters.forcePartitionBy(new HashPartitioner(getNumPartitions))
    println("upgrade")
    // msgs.collect.foreach { msg => println(msg._1); msg._2.iterator.foreach(println)}
    syncMirrors(msgs)
  }

  // update mirrors using the master mask
  def updateVertices: EdgeRDDImpl[ED, VD] = {
    val shippedMsgs = this.partitionsRDD.mapPartitions(_.flatMap {
      part => part._2.shipMasterVertexAttrs
    }).forcePartitionBy(new HashPartitioner(getNumPartitions))

    this.withPartitionsRDD(this.partitionsRDD.zipPartitions(shippedMsgs) {
      (edgePartIter, msgPartIter) => edgePartIter.map {
        case (pid, edgePart) =>
          (pid, edgePart.updateVertices(msgPartIter.flatMap(_._2.iterator)))
      }
    })
  }

  def updateVertices(msgs: RDD[(PartitionID, Iterator[(Int, VD)])]): EdgeRDDImpl[ED, VD] = {

    val shippedMsgs = this.partitionsRDD.zipPartitions(msgs) {
      (edgePartIter, msgPartIter) => edgePartIter.flatMap {
        case (pid, edgePart) =>
          edgePart.shipVertexAttrs(msgPartIter.flatMap(_._2))
      }
    }.forcePartitionBy(new HashPartitioner(getNumPartitions))

    this.withPartitionsRDD(this.partitionsRDD.zipPartitions(shippedMsgs) {
      (edgePartIter, msgPartIter) => edgePartIter.map {
        case (pid, edgePart) =>
          (pid, edgePart.updateVertices(msgPartIter.flatMap(_._2)))
      }
    })
  }

  def updateActiveSet: EdgeRDDImpl[ED, VD] = {
    partitionsRDD.cache()

    val shippedActiveMsgs = partitionsRDD
      .mapPartitions(_.flatMap(part => part._2.shipActiveSet))
      .reduceByKey(_ ++ _).cache()

    // println("updateActiveSet")
    // shippedActiveMsgs.foreach { iter => println(iter._1); iter._2.foreach(println); println }


    val newEdgePartitions = partitionsRDD.zipPartitions(shippedActiveMsgs) {
      (ePartIter, activesIter) => ePartIter.map {
        case (pid, edgePart) =>
          (pid, edgePart.syncActiveSet(activesIter.flatMap(_._2)))
      }
    }

    println("counting activeSets")
    // newEdgePartitions.collect.foreach(part => println(part._2.getActiveSet.get.size))
    // newEdgePartitions.foreachPartition(part => println(part.next._2.getActiveSet.get.size))

    this.withPartitionsRDD(newEdgePartitions)
  }

  def diff(others: RDD[(PartitionID, Array[VD])]):
    EdgeRDDImpl[ED, VD] = {
    this.withPartitionsRDD(this.partitionsRDD.zipPartitions(others) {
      (edgePartIter, otherIter) =>
        edgePartIter.map {
          case (pid, edgePart) =>
            val otherPart = otherIter.flatMap (_._2)
            (pid, edgePart.diff(otherPart.toArray))
        }
    })
  }

  def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2): EdgeRDDImpl[ED, VD2] = {
    this.withPartitionsRDD(this.partitionsRDD.mapPartitions(_.flatMap {part =>
      Iterator((part._1, part._2.mapVertices(f)))
    }))
  }

  def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag](
    f: (PartitionID, EdgePartition[ED, VD]) => EdgePartition[ED2, VD2]): EdgeRDDImpl[ED2, VD2] = {
    this.withPartitionsRDD[ED2, VD2](partitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {
        val (pid, ep) = iter.next()
        Iterator(Tuple2(pid, f(pid, ep)))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true))
  }

  def shipMasters: RDD[(PartitionID, ShippedMsg[VD])] = {
    this.partitionsRDD.mapPartitions(_.flatMap { part =>
      part._2.shipMasterVertexAttrs
    })
  }

  def syncMirrors(msgs: RDD[(PartitionID, ShippedMsg[VD])]): EdgeRDDImpl[ED, VD] = {
    this.withPartitionsRDD(
      partitionsRDD.zipPartitions(msgs) { (thisIter, msgIter) =>
         thisIter.map {
           case (pid, edgePartition) =>
             val msgPart = msgIter.flatMap (_._2.iterator)
             // msgPart.foreach(println)
             (pid, edgePartition.syncMirrors (msgPart))
         }
      }
    )
  }

  def leftJoin[VD2: ClassTag, A: ClassTag](other: RDD[(PartitionID, VertexAttrBlock[A])],
      withActives: Boolean = true)
    (f: (VertexId, VD, Option[A]) => VD2): EdgeRDDImpl[ED, VD2] = {
    // println("Edges.leftJoin")
    this.withPartitionsRDD(
      partitionsRDD.zipPartitions(other, preservesPartitioning = true) {
        (thisIter, otherIter) =>
          val (pid, thisPart) = thisIter.next()
          val (_, otherPart) = otherIter.next()
          Iterator ((pid, thisPart.leftJoin (otherPart, withActives)(f)))
      })
  }

  def withActiveSet[A: ClassTag]
  (actives: RDD[(PartitionID, VertexAttrBlock[A])]): EdgeRDDImpl[ED, VD] = {

    val activeVertices = actives.map(part => (part._1, part._2.clone().msgs.map(_._1)))
        .setName("EdgeWithActiveSet")
        .cache()
    println("Actives here")
    // activeVertices.collect.foreach(a => println(a._1 + " " + a._2.isEmpty))

    // using collect here, actives are kept?
    activeVertices.collect.foreach(a => a._2.foreach(println))

    // activeVertices.getNarrowAncestors.foreach(println)

    // activeVertices.foreachPartition(part => println(part.next._2.length))


    // println(actives.partitioner == partitionsRDD.partitioner)
    // actives.map(iter => iter._2.msgs ++ Iterator((1, 0)))

    /*
    val newEdgePartitions = partitionsRDD.zipPartitions(activeVertices, preservesPartitioning = true) {
      (ePartIter, activesIter) =>
        println("Enter activeSet")
        // ePartIter.foreach(println)
        // activesIter.foreach(println)
        val (pid, edgePartition) = ePartIter.next
        val (_, acts) = activesIter.next
        println("pid " + pid + " acts " + acts.isEmpty)

        Iterator ((pid, edgePartition.withActiveSet (acts)))
    }.cache()
    */
    val partialEdgePartitions = partitionsRDD.zipPartitions(activeVertices, preservesPartitioning = true) {
      (ePartIter, activesIter) => ePartIter.map {
        case (pid, edgePart) =>
          // activesIter.foreach(part => part._2.foreach(println))
          // activesIter.foreach(part => println(part._2.isEmpty))
          (pid, edgePart.withActiveSet(activesIter.flatMap(_._2)))
      }
        // Iterator ((pid, edgePartition.withActiveSet (acts)))
    }.cache()

    val shippedActiveMsgs = partialEdgePartitions
      .mapPartitions(_.flatMap(part => part._2.shipActiveSet))
      .reduceByKey(_ ++ _)

    val newEdgePartitions = partialEdgePartitions.zipPartitions(shippedActiveMsgs) {
      (ePartIter, activesIter) => ePartIter.map {
        case (pid, edgePart) =>
          (pid, edgePart.syncActiveSet(activesIter.flatMap(_._2)))
      }
    }

    println("counting activeSets")
    // newEdgePartitions.collect.foreach(part => println(part._2.getActiveSet.get.size))
    // newEdgePartitions.foreachPartition(part => println(part.next._2.getActiveSet.get.size))

    this.withPartitionsRDD(partialEdgePartitions)
  }

  def getActiveSet: RDD[Option[VertexSet]] = partitionsRDD.map(_._2.getActiveSet)

  private[graphxpp] def withPartitionsRDD[ED2: ClassTag, VD2: ClassTag](
    partitionsRDD: RDD[(PartitionID, EdgePartition[ED2, VD2])]): EdgeRDDImpl[ED2, VD2] = {
    new EdgeRDDImpl(partitionsRDD, this.targetStorageLevel)
  }

  private[graphxpp] def withTargetStorageLevel(
    targetStorageLevel: StorageLevel): EdgeRDDImpl[ED, VD] = {
    new EdgeRDDImpl(this.partitionsRDD, targetStorageLevel)
  }
}


/*
object EdgeRDDImpl {

}

*/