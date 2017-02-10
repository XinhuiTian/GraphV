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

import org.apache.spark.graphxpp.impl.{VertexAttrBlock, GraphImpl}
import org.apache.spark.graphxpp.utils.GraphUtils
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by XinhuiTian on 16/12/31.
 */
object Pregel {

  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: GraphImpl[ED, VD],
    initialMsg: A,
    maxIterations: Int = Int.MaxValue,
    activeDirection: EdgeDirection = EdgeDirection.Either)
    (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[ED, VD] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
  : GraphImpl[ED, VD] = {
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()

    // g.triplets.foreachPartition{ t =>
      // t.foreach(ti => println(ti.srcId + " " + ti.srcAttr + " " + ti.dstId + " " + ti.dstAttr)); println }

    var messages = GraphUtils.mapReduceTriplets(g, sendMsg, mergeMsg).cache()

    // a little complex here to compute the num of active messages
    // val oldMessages = messages.clone()

    // var activeMessages = 0

    /*
    for (msg <- messages) yield {
        activeMessages += msg._2.msgs.length
    }
    */
    // var checkMsgs: RDD[(PartitionID, VertexAttrBlock[A])] = null

    // checkMsgs = messages.filter(!_._2.msgs.isEmpty)

    var activeMessages = messages.filter(!_._2.msgs.isEmpty).count
    // messages.filter(!_._2.msgs.isEmpty).count
    // var activeMessages = 1.0

    println("Messages")

    // checkMsgs.collect().foreach(_._2.msgs.foreach(println))
    println(activeMessages)

    var prevG: GraphImpl[ED, VD] = null
    var currentG: GraphImpl[ED, VD] = g
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages and update the vertices.
      println("Enter the iteration")

      // messages.cache()
      // messages.collect().foreach(_._2.msgs.foreach(println))
      prevG = g

      g = g.joinMsgs(messages)(vprog).cache()
      g = g.withEdgeRDD(g.edges.updateActiveSet)
      println("Messages After Join: ")
      // messages.collect().foreach(_._2.msgs.foreach(println))

      // g.edges.count()
      currentG = g
      currentG.cache()

      // adding this can empty the old messages?
      println("g after join")
      // currentG.verticesWithAttrs.collect().foreach(println)

      // currentG.triplets.foreachPartition{ t =>
        // t.foreach(ti => println(ti.srcId + " " + ti.srcAttr + " " + ti.dstId + " " + ti.dstAttr)); println }
      val oldMessages = messages
      // messages.unpersist(blocking = false)
      // oldMessages.cache()
      // println("OldMessages")
      // oldMessages.collect().foreach(_._2.msgs.foreach(println))

      messages = GraphUtils.mapReduceTriplets(
         g, sendMsg, mergeMsg, Some((oldMessages, activeDirection))).cache()

      // messages = GraphUtils.mapReduceTriplets(
      //   g, sendMsg, mergeMsg).cache()
      // messages.count
      activeMessages = messages.filter(!_._2.msgs.isEmpty).count
      // g = currentG
      // messages.count
      // messages.collect().foreach(_._2.msgs.foreach(println))

      println("activeMsgs")
      println(activeMessages)
      // messages = newMessages
      // newMessages.collect().foreach(_._2.msgs.foreach(println))

      oldMessages.unpersist(blocking = false)
      // prevG.(blocking = false)
      prevG.edges.unpersist(blocking = false)
      i += 1
    }
    messages.unpersist(blocking = false)
    g
  }
}
