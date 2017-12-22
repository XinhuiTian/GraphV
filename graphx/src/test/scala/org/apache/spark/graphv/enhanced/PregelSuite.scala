
package org.apache.spark.graphv.enhanced

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import org.apache.spark.graphx.LocalSparkContext
import org.apache.spark.graphv._
import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils


class PregelSuite extends SparkFunSuite with LocalSparkContext {

  test ("PageRank") {
    withSpark { sc =>

      val tmpDir = Utils.createTempDir ()
      val graphFile = new File (tmpDir.getAbsolutePath, "graph.txt")
      // println(tmpDir.getAbsolutePath)
      val writer = new OutputStreamWriter (new FileOutputStream (graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) {
        writer.write (s"0 $i\n")
      }

      for (i <- 1 until 10) {
        writer.write (s"$i 0\n")
      }

      writer.close ()

      try {

        val myStartTime = System.currentTimeMillis
        val workGraph = GraphLoader.edgeListFile (sc, "/Users/XinhuiTian/Downloads/soc-Epinions1.txt", false, 8).cache ()
        // val workGraph = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, false, 10).cache()

        println ("outDegrees")
        // workGraph.outDegrees.foreach(println)
        // workGraph.count()
        // workGraph.vertices.foreach(println)
        // workGraph.vertices.count()

        val iniGraph: Graph[Double, Double] = workGraph
          .localOuterJoin (workGraph.localDegreeRDD (false), false){(vid, attr, degree) => degree}
          .mapTriplets (e => 1.0 / e.srcAttr, TripletFields.SrcWithEdge)
          .mapVertices ((id, attr) => 1.0).cache ()

        // iniGraph.vertices.foreach(println)

        // iniGraph.edges.foreach(println)

        iniGraph.vertices.count
        println ("It took %d ms loadGraph".format (System.currentTimeMillis - myStartTime))
        /*
        iniGraph.vertices.partitionsRDD.foreach { v =>
          v.foreachEdgePartition((vid, e) => println(vid + " " + e))
        }
        */
        println ("Active masters: " + iniGraph.getActiveNums)
        // iniGraph.count()

        val newGraph = iniGraph.activateAllMasters
        newGraph.cache ()
        newGraph.vertices.count ()
        // println("edges")
        // workGraph.vertices.partitionsRDD.foreach { vertexPart => vertexPart.edges.foreach(println); println }
        // iniGraph.vertices.foreach(println)


        val initialMessage = 1.0

        def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = {
          val newRank = 0.15 + (1.0 - 0.15) * msgSum
          newRank
        }

        def sendMessage(edge: GraphVEdgeTriplet[Double, Double]) = {
          Iterator ((edge.dstId, edge.srcAttr * edge.attr))
        }

        def mergeMessage(a: Double, b: Double): Double = a + b

        val resultGraph = Pregel (newGraph, initialMessage,
          activeDirection = EdgeDirection.Out, maxIterations = 10)(
          (id, attr) => 1.0, vertexProgram, sendMessage, mergeMessage)

        println (resultGraph.vertices.map (_._2).sum ())
        // resultGraph.vertices.values.foreach(println)
        println ("total vertices: " + resultGraph.vertices.map (_._1).count)
        println ("My pregel " + (System.currentTimeMillis - myStartTime))
      } finally {
        Utils.deleteRecursively (tmpDir)
      }
    }


  }

  test ("BFS") {
    withSpark { sc =>
      val myStartTime = System.currentTimeMillis

      val graph = GraphLoader.edgeListFile (sc,
        "/Users/XinhuiTian/Downloads/facebook-links.txt", false, 8, true, false).cache ()
      // tmpDir.getCanonicalPath, false, 8).cache ()

      println ("Vertices: " + graph.vertices.count ())
      println ("Edges: " + graph.edges.count (), graph.edgeSize)

      var g = graph.mapVertices ((vid, attr) => Integer.MAX_VALUE, false).cache ()

      val initialMessage = Integer.MAX_VALUE

      def initialProgram(id: VertexId, attr: Int): Int = {
        if (id == 1L) 0 else attr
      }

      def vertexProgram(id: VertexId, attr: Int, msg: Int): Int = {
        if (attr == Integer.MAX_VALUE) msg
        else attr
      }

      def sendMessage(edge: GraphVEdgeTriplet[Int, _]): Iterator[(VertexId, Int)] = {
        // only visited vertices can be touched here?

        if (edge.srcAttr < Int.MaxValue) {
          Iterator ((edge.dstId, edge.srcAttr + 1))
        } else {
          Iterator.empty
        }
      }

      def mergeFunc(a: Int, b: Int): Int = Math.min (a, b)

      val results = Pregel (g, initialMessage, needActive = true)(
        initialProgram, vertexProgram, sendMessage, mergeFunc)
      println (results.vertices.map (_._2).sum ())
      println ("My pregel " + (System.currentTimeMillis - myStartTime))
    }
  }

  test("DeltaPageRank") {
    withSpark{sc =>
      val tmpDir = Utils.createTempDir ()
      val graphFile = new File (tmpDir.getAbsolutePath, "graph.txt")
      // println(tmpDir.getAbsolutePath)
      val writer = new OutputStreamWriter (new FileOutputStream (graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) {
        writer.write (s"0 $i\n")
      }

      for (i <- 1 until 101) {
        writer.write(s"$i 0\n")
      }


      writer.close ()

      try {

        val myStartTime = System.currentTimeMillis

        val graph = GraphLoader.edgeListFile (sc,
           "/Users/XinhuiTian/Downloads/soc-Epinions1.txt", false, 8).cache ()
        //tmpDir.getCanonicalPath, false, 8).cache ()

        graph.vertices.count ()

        val resetProb = 0.15
        println ("It took %d ms loadGraph".format (System.currentTimeMillis - myStartTime))
        val iniGraph: Graph[(Double, Double), Double] = graph
          .localOuterJoin(graph.localOutDegrees, false) { (vid, attr, degree) => degree }
          .mapTriplets (e => 1.0 / e.srcAttr, TripletFields.SrcWithEdge)
          .mapVertices ((id, attr) => (0.0, 0.0)).cache ()

        // iniGraph.vertices.count

        // workGraph.outDegrees.foreach(println)

        // iniGraph.vertices.foreach(println)

        val initialMessage = 1.0
        val tol = 0.0001

        def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double)
        : (Double, Double) = {
          val (oldPR, _) = attr
          val newPR = oldPR + (1.0 - resetProb) * msgSum
          (newPR, newPR - oldPR)
        }

        def sendMessage(edge: GraphVEdgeTriplet[(Double, Double), Double])
        : Iterator[(VertexId, Double)] = {
          if (edge.srcAttr._2 > tol) {
            Iterator ((edge.dstId, edge.srcAttr._2 * edge.attr))
          } else {
            Iterator.empty
          }
        }

        def mergeMessage(a: Double, b: Double): Double = a + b

        val resultGraph = Pregel (iniGraph, initialMessage,
          activeDirection = EdgeDirection.Out, needActive = true)(
          (id, attr) => (0.85, 0.85), vertexProgram, sendMessage, mergeMessage)

        println (resultGraph.vertices.values.count ())
        //resultGraph.vertices.values.foreach(println)
        println ("total vertices: " + resultGraph.vertices.map (_._2._1).sum ())
        println ("My pregel " + (System.currentTimeMillis - myStartTime))
      } finally {
        Utils.deleteRecursively (tmpDir)
      }
    }
  }

}