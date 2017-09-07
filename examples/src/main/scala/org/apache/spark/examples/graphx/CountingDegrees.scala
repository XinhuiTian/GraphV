package org.apache.spark.examples.graphx

import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * Created by XinhuiTian on 17/6/26.
 */
object CountingDegrees {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        "Usage: CountingDegrees <file> -numEPart <partNum> -partStrategy <strategy>")
      System.exit(1)
    }

    val fname = args(0)

    val optionsList = args.drop(1).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)
    // $example on$
    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    val edgePartitioner: Option[IngressEdgePartitioner] = options.remove("partStrategy")
      .map(IngressEdgePartitioner.fromString(_, numEPart))
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)

    val sc = new SparkContext(conf.setAppName("CountingDegrees(" + fname + ")"))

    val graph = GraphLoader.edgeListFile(sc, fname,
      numEdgePartitions = numEPart,
      edgeStorageLevel = edgeStorageLevel,
      vertexStorageLevel = vertexStorageLevel,
      partitioner = edgePartitioner).cache()

    println("GRAPHX: Number of vertices " + graph.vertices.count)
    println("GRAPHX: Number of edges " + graph.edges.count)

    // 1. get the in degrees for each vertex, and sync to the edges
    // 2.


    println("Number of in complete vertices: " + graph.inComRatio)
    println("Number of out complete vertices: " + graph.outComRatio)
  }

}
