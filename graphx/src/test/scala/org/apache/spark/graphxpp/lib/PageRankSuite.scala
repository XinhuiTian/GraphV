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

/**
 * Created by XinhuiTian on 17/1/9.
 */
import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx.LocalSparkContext
import org.apache.spark.graphxpp.{GraphLoader, TripletFields, Graph, VertexId}
import org.apache.spark.graphxpp.lib.{ PageRank => PR}

import scala.reflect.ClassTag

/**
 * Created by XinhuiTian on 17/1/3.
 */
object GridPageRank {
  def apply(nRows: Int, nCols: Int, nIter: Int, resetProb: Double): Seq[(VertexId, Double)] = {
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outDegree = Array.fill(nRows * nCols)(0)
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r, c)
      if (r + 1 < nRows) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r + 1, c)) += ind
      }
      if (c + 1 < nCols) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r, c + 1)) += ind
      }
    }
    // compute the pagerank
    var pr = Array.fill(nRows * nCols)(resetProb)
    for (iter <- 0 until nIter) {
      val oldPr = pr
      pr = new Array[Double](nRows * nCols)
      for (ind <- 0 until (nRows * nCols)) {
        pr(ind) = resetProb + (1.0 - resetProb) *
          inNbrs(ind).map( nbr => oldPr(nbr) / outDegree(nbr)).sum
      }
    }
    (0L until (nRows * nCols)).zip(pr)
  }

}

class PageRankSuite extends SparkFunSuite with LocalSparkContext {
  def runWithOptions[ED: ClassTag, VD: ClassTag](
    graph: Graph[ED, VD], numIter: Int, resetProb: Double = 0.15,
    srcId: Option[VertexId] = None): Graph[Double, Double] = {
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
      .outerJoinMasters(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
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

      rankGraph = rankGraph.joinMsgs(rankUpdates) {
        (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - resetProb) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      println(s"PageRank finished iteration $iteration.")
      // prevRankGraph.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    rankGraph
  }

  def run[ED: ClassTag, VD: ClassTag](graph: Graph[ED, VD], numIter: Int,
    resetProb: Double = 0.15): Graph[Double, Double] =
  {
    runWithOptions(graph, numIter, resetProb)
  }

  test ("Chain PageRank") {
    withSpark {sc =>
      val chain1 = (0 until 9).map (x => (x, x + 1))
      val rawEdges = sc.parallelize (chain1, 1).map {case (s, d) => (s.toLong, d.toLong)}
      // val chain = Graph.fromEdgeTuples (rawEdges, 1.0).cache ()
      val graph = GraphLoader.edgeListFile(sc, "/Users/XinhuiTian/Downloads/wiki-Vote.txt", false, 20)
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 20
      val errorTol = 1.0e-5

      val staticRanks = graph.staticPageRank (numIter, resetProb)
      // val staticRanks = PR.run(chain, numIter).edges.count

      val srcId = None
      val personalized = srcId.isDefined
      val src: VertexId = srcId.getOrElse (-1L)
      // val staticRanks = run (chain, 10)

    }
  }
}