
package org.apache.spark.examples.graphv.enhanced

import scala.collection.mutable

import org.apache.spark.graphv.{EdgeDirection, VertexId}
import org.apache.spark.graphv.enhanced._
import org.apache.spark.{SparkConf, SparkContext}

object DeltaPageRank {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println (
        "Usage: GraphLoader <file> --numEPart=<num_edge_partitions> [other options]")
      System.exit (1)
    }

    val fname = args (0)
    val optionsList = args.drop (2).map{arg =>
      arg.dropWhile (_ == '-').split ('=') match {
        case Array (opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException ("Invalid argument: " + arg)
      }
    }

    val options = mutable.Map (optionsList: _*)

    val numEPart = options.remove ("numEPart").map (_.toInt).getOrElse{
      println ("Set the number of edge partitions using --numEPart.")
      sys.exit (1)
    }

    val iterations = options.remove ("numIter").map (_.toInt).getOrElse{
      println ("Set the number of iterations using --numIter.")
      sys.exit (1)
    }

    val conf = new SparkConf ()

    val sc = new SparkContext (conf.setAppName ("GraphLoad(" + fname + ")"))

    val myStartTime = System.currentTimeMillis
    val graph = GraphLoader.edgeListFile (sc, args (0), false, numEPart).cache ()

    graph.vertices.count ()
    println ("It took %d ms loadGraph".format (System.currentTimeMillis - myStartTime))

    val iniGraph: Graph[(Double, Double), Double] = graph
      .localOuterJoin(graph.localOutDegrees, false) { (vid, attr, degree) => degree }
      .mapTriplets (e => 1.0 / e.srcAttr, TripletFields.SrcWithEdge)
      .mapVertices ((id, attr) => (0.0, 0.0)).cache ()

    // iniGraph.vertices.count

    // workGraph.outDegrees.foreach(println)

    // iniGraph.vertices.foreach(println)

    val resetProb = 0.15
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


    // resultGraph.cache ()
    println (resultGraph.vertices.values.count ())
    //resultGraph.vertices.values.foreach(println)
    println ("total vertices: " + resultGraph.vertices.map (_._2._1).sum ())
    println ("My pregel " + (System.currentTimeMillis - myStartTime))
  }

}
