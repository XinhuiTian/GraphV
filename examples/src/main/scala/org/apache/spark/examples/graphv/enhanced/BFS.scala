
package org.apache.spark.examples.graphv.enhanced

import scala.collection.mutable

import org.apache.spark.graphv.VertexId
import org.apache.spark.graphv.enhanced.{GraphLoader, GraphVEdgeTriplet, Pregel}
import org.apache.spark.{SparkConf, SparkContext}

object BFS {
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

    val iterations = options.remove("numIter").map(_.toInt).getOrElse {
      println ("Set the number of iterations using --numIter.")
      sys.exit (1)
    }

    val conf = new SparkConf ()

    val sc = new SparkContext (conf.setAppName ("GraphLoad(" + fname + ")"))

    val myStartTime = System.currentTimeMillis
    val graph = GraphLoader.edgeListFile (sc, args (0), false, numEPart).cache()

    graph.vertices.count ()
    println ("It took %d ms loadGraph".format (System.currentTimeMillis - myStartTime))

    // val landmark = graph.vertices.map(_._1).take(0)(0)

    // val landmark: Long = 61

    var g = graph.mapVertices ((vid, attr) => Integer.MAX_VALUE, false).cache ()

    val initialMessage = Integer.MAX_VALUE

    def initialProgram(id: VertexId, attr: Int): Int = {
      if (id == 61L) 0 else attr
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
