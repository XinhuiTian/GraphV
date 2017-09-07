package org.apache.spark.examples.graphloca

// scalastyle:off println
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphloca.GraphLoader
/**
 * Created by XinhuiTian on 17/5/9.
 */
object GraphLoad {
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

    val conf = new SparkConf ()

    val sc = new SparkContext (conf.setAppName ("GraphLoad(" + fname + ")"))

    val graph = GraphLoader.edgeListFile(sc, args (0), false, numEPart,
      edgePartitioner = "EdgePartition2D").cache ()
  }
}
