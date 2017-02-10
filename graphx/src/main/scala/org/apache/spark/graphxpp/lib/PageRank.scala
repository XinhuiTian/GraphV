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

package org.apache.spark.graphxpp.lib

import org.apache.spark.graphxpp.utils.GraphUtils
import org.apache.spark.graphxpp.{TripletFields, VertexId, Graph}

import scala.reflect.ClassTag

/**
 * Created by XinhuiTian on 17/1/8.
 */
object PageRank {
  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def run[ED: ClassTag, VD: ClassTag](graph: Graph[ED, VD], numIter: Int,
    resetProb: Double = 0.15): Graph[Double, Double] =
  {
    runWithOptions(graph, numIter, resetProb)
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   */
  def runWithOptions[ED: ClassTag, VD: ClassTag](
    graph: Graph[ED, VD], numIter: Int, resetProb: Double = 0.15,
    srcId: Option[VertexId] = None): Graph[Double, Double] =
  {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute resetProb.
    // When running personalized pagerank, only the source vertex
    // has an attribute resetProb. All others are set to 0.
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinMasters(graph.outDegrees, false) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) =>
      if (!(id != src && personalized)) resetProb else 0.0
    }

    // rankGraph.triplets.collect.foreach(println)

    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      val startTime = System.currentTimeMillis()
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      /*
      val rankUpdates = GraphUtils.mapReduceTriplets[Double, Double, Double](
        rankGraph,
        et => Iterator((et.dstId, et.srcAttr * et.attr)),
        (a: Double, b: Double) => a) */

      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.srcAttr * ctx.attr),
        _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      val rPrb = if (personalized) {
        (src: VertexId, id: VertexId) => resetProb * delta(src, id)
      } else {
        (src: VertexId, id: VertexId) => resetProb
      }

      rankGraph = rankGraph.joinMsgs(rankUpdates, false) {
        (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - resetProb) * msgSum
      }.cache()


      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      val endTime = System.currentTimeMillis()
      println(s"PageRank finished iteration $iteration, time: ${endTime - startTime}")
      // prevRankGraph.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    rankGraph
  }
}
