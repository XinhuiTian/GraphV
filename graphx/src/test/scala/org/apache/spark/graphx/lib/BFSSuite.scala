
package org.apache.spark.graphx.lib

import org.apache.spark.graphx._
import org.apache.spark.SparkFunSuite

class BFSSuite extends SparkFunSuite with LocalSparkContext {

  test("BFS Computations") {
    withSpark { sc =>
      val graph = GraphLoader.edgeListFile(sc,
        "/Users/XinhuiTian/Downloads/facebook-links.txt", false, 8)
        .cache()

      var g = graph.mapVertices { (vid, attr) =>
        if (vid == 1L) 0 else Integer.MAX_VALUE
      }.cache()

      val initialMessage = Integer.MAX_VALUE

      def vertexProgram(id: VertexId, attr: Int, msg: Int): Int = {
        if (attr == Integer.MAX_VALUE) msg
        else attr
      }

      def sendMessage(edge: EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
        // only visited vertices can be touched here?

        if (edge.srcAttr != Integer.MAX_VALUE) {
          val newAttr = edge.srcAttr + 1
          if (edge.dstAttr == Integer.MAX_VALUE) Iterator((edge.dstId, newAttr))
          else Iterator.empty
        }
        // only send msg to no visit vertices

        else Iterator.empty
      }

      def mergeFunc(a: Int, b: Int): Int = Math.min(a, b)

      val results = Pregel(g, initialMessage,
        activeDirection = EdgeDirection.Out)(
        vertexProgram, sendMessage, mergeFunc)
      println(results.vertices.map(_._2).sum())
    }
  }

  test("SSSP Computations") {
    withSpark { sc =>
      val graph = GraphLoader.edgeListFile(sc,
        "/Users/XinhuiTian/Downloads/soc-Epinions1.txt", false, 8)
        .cache()

      var g = graph.mapVertices { (vid, attr) =>
        if (vid == 61L) 0 else Double.PositiveInfinity
      }.cache()

      val initialMessage = Double.PositiveInfinity

      def vertexProgram(id: VertexId, attr: Double, msg: Double)
      : Double = math.min(attr, msg)

      def sendMessage(edge: EdgeTriplet[Double, _]): Iterator[(VertexId, Double)] = {
        // only visited vertices can be touched here?

        if (edge.srcAttr != Double.PositiveInfinity) {
          val newAttr = edge.srcAttr + 1.0
          if (edge.dstAttr > newAttr) Iterator((edge.dstId, newAttr))
          else Iterator.empty
        }
        // only send msg to no visit vertices

        else Iterator.empty
      }

      def mergeFunc(a: Double, b: Double): Double = Math.min(a, b)

      val results = Pregel(g, initialMessage,
        activeDirection = EdgeDirection.Out)(
        vertexProgram, sendMessage, mergeFunc)
      println(results.vertices.map(_._2).sum())
    }
  }

}
