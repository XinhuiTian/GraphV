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
// scalastyle:off println
import org.apache.spark.graphxpp.utils.GraphUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.graphx.LocalSparkContext

/**
 * Created by XinhuiTian on 17/1/3.
 */
class GraphSuite extends SparkFunSuite with LocalSparkContext {
  def starGraph(sc: SparkContext, n: Int): Graph[Int, String] = {
    Graph.fromEdgeTuples (sc.parallelize ((1 to n).map (x => (0: VertexId, x: VertexId)), 3), "v")
  }

  test ("Graph.fromEdgeTuples") {
    withSpark {sc =>
      val ring = (0L to 100L).zip ((1L to 99L) :+ 0L)
      val doubleRing = ring ++ ring
      val graph = Graph.fromEdgeTuples (sc.parallelize (doubleRing, 3), 1)
      assert (graph.edges.count () == doubleRing.size)
      assert (graph.edges.collect ().forall (e => e.attr == 1))
    }
  }

  test ("Graph.fromEdges") {
    withSpark {sc =>
      val ring = (0L to 100L).zip ((1L to 99L) :+ 0L).map {case (a, b) => Edge (a, b, 1)}
      val graph = Graph.fromEdges (sc.parallelize (ring), 1.0F)
      assert (graph.edges.count () === ring.size)
    }
  }

  test ("Graph.apply") {
    withSpark {sc =>
      val rawEdges = (0L to 98L).zip ((1L to 99L) :+ 0L)
      val edges: RDD[Edge[Int]] = sc.parallelize (rawEdges, 10).map {case (s, t) => Edge (s, t, 1)}
      val vertices: RDD[(VertexId, Boolean)] = sc.parallelize ((0L until 10L).map (id => (id, true)))
      var graph = Graph.fromEdges (edges, false)
      graph = graph.mapVertices ((vid, vd) => if (vid < 10) true else false)
      assert (graph.edges.count () === rawEdges.size)
      // Vertices not explicitly provided but referenced by edges should be created automatically
      assert (graph.vertices.count () === 100)

      /*
      graph.edges.partitionsRDD.collect.foreach { part =>
        println("Mirror Size: " + part._2.getMirrors.size)
        println("Master Size: " + part._2.mastersSize)
        part._2.getMasters.foreach(println)
        part._2.getMirrors.foreach(println)
      }
      */



      graph.triplets.collect ().foreach {et =>
        assert ((et.srcId < 10 && et.srcAttr) || (et.srcId >= 10 && !et.srcAttr))
        assert ((et.dstId < 10 && et.dstAttr) || (et.dstId >= 10 && !et.dstAttr))
        // println(et.srcId + " " + et.dstId + " " + et.srcAttr + " " + et.dstAttr)
      }

    }
  }

  test ("triplets") {
    withSpark {sc =>
      val n = 5
      val star = starGraph (sc, n)
      assert (star.triplets.map (et => (et.srcId, et.dstId, et.srcAttr, et.dstAttr)).collect ().toSet
                === (1 to n).map (x => (0: VertexId, x: VertexId, "v", "v")).toSet)
    }
  }

  test ("mapVertices") {
    withSpark {sc =>
      val n = 5
      val star = starGraph (sc, n)
      // mapVertices preserving type
      val mappedVAttrs = star.mapVertices ((vid, attr) => attr + "2")
      assert (mappedVAttrs.verticesWithAttrs.collect ().toSet
                === (0 to n).map (x => (x: VertexId, "v2")).toSet)
      // mapVertices changing type
      val mappedVAttrs2 = star.mapVertices ((vid, attr) => attr.length)
      assert (mappedVAttrs2.verticesWithAttrs.collect ().toSet
                === (0 to n).map (x => (x: VertexId, 1)).toSet)
    }
  }

  test ("upgrade") {
    withSpark {sc =>
      val ring = (0L to 100L).zip ((1L to 99L) :+ 0L).map { e => Edge(e._1, e._2, 1) }
      val graph = Graph.fromEdges (sc.parallelize(ring, 10).asInstanceOf[RDD[Edge[Int]]], 1.0F)


      // val newGraph = graph.mapVertices((vid, v) => 2.0F)

      graph.upgrade

      graph.edges.partitionsRDD.foreach { part =>
        println(part._1)
        part._2.getMirrorsWithAttr.foreach(println)
        println
      }


      // newGraph.upgrade
      // assert (newGraph.triplets.map (et => (et.srcAttr, et.dstAttr)).collect().toSet
      //            === (0L to 100L).map (x => (2.0F, 2.0F)).toSet)

    }
  }

  test ("aggregateMessages") {
    withSpark {sc =>
      val n = 5
      val graph = starGraph (sc, n)
      println("Masters")
      graph.verticesWithAttrs.foreach(println)

      println("Mirrors")
      graph.edges.partitionsRDD.foreach { part =>
        part._2.getMirrorsWithAttr.foreach(println)
      }
      println("Master Size")
      graph.edges.partitionsRDD.foreach { part =>
        println(part._2.getMasters.size)
      }
      println("Mirror Size")
      graph.edges.partitionsRDD.foreach { part =>
        println(part._2.getMirrors.size)
      }

      val agg = graph.aggregateMessages [String](
        ctx => {
          if (ctx.dstAttr != null) {
            throw new Exception (
              "expected ctx.dstAttr to be " +
                "null" +
                " due to TripletFields, but" +
                " it" +
                " was " + ctx.dstAttr)
          }
          ctx.sendToDst (ctx.srcAttr)
        }, _ + _, TripletFields.Src)

      agg.collect().foreach{ part => println(part._1); part._2.msgs.foreach(println)}

      // agg.flatMap(_._2.msgs).collect.foreach(println)

      // assert (agg.flatMap(_._2.msgs).collect.toSet === (1 to n).map (x => (x: VertexId, "v")).toSet)
    }
  }

  test("outDegrees") {
    withSpark { sc =>
      val n = 5
      val reverseStar = starGraph(sc, n).cache()
      val outMsgs = reverseStar.outDegrees

      val newGraph = reverseStar.outerJoinMasters(outMsgs) {
        (vid, attr, op) => op.getOrElse(0)
      }

      val outds = newGraph.verticesWithAttrs.collect().toSet

      assert(outds === Set((0: VertexId, n)) ++ (1 to n).map(x => (x: VertexId, 0)))

    }
  }

  test("InPartitionAggregateMessages") {
    withSpark { sc =>
      val n = 5
      val star = starGraph(sc, n).cache()
      val newStar = star.mapVertices((vid, v) => if (vid == 0) 1 else 0)

      newStar.upgrade
      newStar.edges.partitionsRDD.foreach{ iter => iter._2.getMastersWithAttr.foreach(println); println}

      newStar.edges.partitionsRDD.foreach{ iter => iter._2.getMirrorsWithAttr.foreach(println); println}

      /*
      val sums = GraphUtils.mapReduceTriplets[Int, Int, Int](
        newStar,
        et => Iterator((et.srcId, et.dstAttr), (et.dstId, et.srcAttr)),
        (a: Int, b: Int) => a + b).flatMap(_._2.msgs).collect().toSet

      println(sums)
      */
    }
  }

  test("OutJoinWithAggregateMessages") {
    withSpark { sc =>
      val n = 5
      val reverseStar = Graph.fromEdgeTuples (sc.parallelize ((0 to n).map (x => (x: VertexId, x+1: VertexId)), 3), "v").cache()
      // val reverseStar = starGraph(sc, n).cache()
      // outerJoinVertices changing type
      // vertex with its own out degree number
      // val msgs = reverseStar.outDegrees.cache()
      val msgs = reverseStar.outDegrees.cache()
      msgs.count
      val starDegrees = reverseStar.outerJoinMasters(msgs) {
        (vid, a, bOpt) => bOpt.getOrElse(10)
      }
      val oldMsgs = msgs
      oldMsgs.collect.foreach(p => p._2.msgs.foreach(println))


      // starDegrees.upgrade
      // reverseStarDegrees.triplets.foreachPartition {iter => iter.foreach(t => println(t.srcId + " " + t.dstId)); println }
      // reverseStarDegrees.verticesWithAttrs.foreach(println)
      starDegrees.edges.partitionsRDD.foreach{ iter => iter._2.getMastersWithAttr.foreach(println); println}

      // starDegrees.edges.partitionsRDD.foreach{ iter => iter._2.getMirrorsWithAttr.foreach(println); println}

      // val shippedMsgs = starDegrees.edges.shipMasters.reduceByKey(_ ++ _)

      // shippedMsgs.foreach{ part => println(part._1); part._2.foreach(println)}
      // val newEdges = starDegrees.edges
      //    .syncMirrors(shippedMsgs)
      // starDegrees.upgrade
      // starDegrees.edges.partitionsRDD.foreach{ iter => iter._2.getMastersWithAttr.foreach(println); println}
      // newEdges.partitionsRDD.foreach{ iter => iter._2.getMastersWithAttr.foreach(println); println}

      // starDegrees.edges.partitionsRDD.foreach{ iter => iter._2.getMirrorsWithAttr.foreach(println); println}
      // newEdges.partitionsRDD.foreach{ iter => iter._2.getMirrorsWithAttr.foreach(println); println}

      /*
      val newStar = starDegrees.mapVertices((vid, v) => if (vid % 2 == 0) 1 else 0)

      newStar.upgrade
      newStar.edges.partitionsRDD.foreach{ iter => iter._2.getMastersWithAttr.foreach(println); println}

      newStar.edges.partitionsRDD.foreach{ iter => iter._2.getMirrorsWithAttr.foreach(println); println}*/
      /*
      val neighborDegreeSums = GraphUtils.mapReduceTriplets[Int, Int, Int](
        starDegrees,
        et => Iterator((et.srcId, et.dstAttr), (et.dstId, et.srcAttr)),
        (a: Int, b: Int) => a + b).flatMap(_._2.msgs).collect().toSet
      assert(neighborDegreeSums === Set((0: VertexId, n)) ++ (1 to n).map(x => (x: VertexId, 0)))*/
      // outerJoinVertices preserving type
      /*
      val messages = reverseStar.mapVertices { (vid, attr) => vid.toString }
      val newReverseStar =
        reverseStar.outerJoinMasters(messages) { (vid, a, bOpt) => a + bOpt.getOrElse("") }
      assert(newReverseStar.vertices.map(_._2).collect().toSet ===
        (0 to n).map(x => "v%d".format(x)).toSet)
        */
    }
  }

  test("outerJoinVertices") {
    withSpark { sc =>
      val n = 5
      val reverseStar = starGraph(sc, n).cache()
      // outerJoinVertices changing type
      // vertex with its own out degree number
      val reverseStarDegrees = reverseStar.outerJoinMasters(reverseStar.outDegrees) {
        (vid, a, bOpt) => bOpt.getOrElse(0)
      }.cache

      // reverseStarDegrees.upgrade
      reverseStarDegrees.triplets.foreachPartition {iter => iter.foreach(t => println(t.srcId + " " + t.srcAttr + " " + t.dstId + " " + t.dstAttr)); println }
      // reverseStarDegrees.verticesWithAttrs.foreach(println)

      val neighborDegreeSums = GraphUtils.mapReduceTriplets[Int, Int, Int](
        reverseStarDegrees,
        et => Iterator((et.srcId, et.dstAttr), (et.dstId, et.srcAttr)),
        (a: Int, b: Int) => a + b).flatMap(_._2.msgs).collect().toSet
      assert(neighborDegreeSums === Set((0: VertexId, 0)) ++ (1 to n).map(x => (x: VertexId, n)))
      // outerJoinVertices preserving type
      /*
      val messages = reverseStar.mapVertices { (vid, attr) => vid.toString }
      val newReverseStar =
        reverseStar.outerJoinMasters(messages) { (vid, a, bOpt) => a + bOpt.getOrElse("") }
      assert(newReverseStar.vertices.map(_._2).collect().toSet ===
        (0 to n).map(x => "v%d".format(x)).toSet)
        */
    }
  }
}
