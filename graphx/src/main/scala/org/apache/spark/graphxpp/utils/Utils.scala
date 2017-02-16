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

import scala.reflect.ClassTag

import org.apache.spark.graphxpp._
import org.apache.spark.graphxpp.impl.{GraphImpl, VertexAttrBlock}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

/**
 * Created by XinhuiTian on 17/1/1.
 */
object GraphUtils {

  private[graphxpp] def toBitSet(flags: Array[Int]): BitSet = {
    val bitset = new BitSet(flags.size)
    var i = 0
    while (i < flags.size) {
      bitset.set(flags(i))
      i += 1
    }
    bitset
  }

  private[graphxpp] def mapReduceTriplets[VD: ClassTag, ED: ClassTag, A: ClassTag](
    g: Graph[ED, VD],
    mapFunc: EdgeTriplet[ED, VD] => Iterator[(VertexId, A)],
    reduceFunc: (A, A) => A,
    activeSetOpt: Option[(RDD[(PartitionID, VertexAttrBlock[A])], EdgeDirection)] = None):
  RDD[(PartitionID, VertexAttrBlock[A])] = {
    def sendMsg(ctx: EdgeContext[VD, ED, A]) {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }

    g.aggregateMessagesWithActiveSet(
      sendMsg, reduceFunc, TripletFields.All, activeSetOpt)
  }

}
