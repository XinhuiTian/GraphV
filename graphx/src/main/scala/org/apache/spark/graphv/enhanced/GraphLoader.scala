
package org.apache.spark.graphv.enhanced

import org.apache.spark.graphv._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

object GraphLoader {

  def edgeListFile(
      sc: SparkContext,
      path: String,
      reverse: Boolean = false,
      numVertexPartitions: Int = -1,
      useSrcMirror: Boolean = true,
      useDstMirror: Boolean = true,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): GraphImpl[Int, Int] = {

    val startTime = System.currentTimeMillis
    // Parse the edge data table directly into edge partitions
    val lines =
      if (numVertexPartitions > 0) {
        sc.textFile (path, numVertexPartitions).coalesce (numVertexPartitions)
      } else {
        sc.textFile (path)
      }

    // using cache cause longer time?
    val filteredLines = lines.filter(line => !line.isEmpty && line(0) != '#')


    val mid_data = filteredLines.flatMap (line => {
      val parts = line.split ("\\s+")
      // srcId must have an edge, do not need a -1 flag to build vertex
      Iterator((parts(0).toLong, parts(1).toLong), (parts(1).toLong, -1L))
    })

    // mid_data.foreach(println)

    // groupby the master vids
    val links = mid_data.groupByKey (new HashPartitioner (numVertexPartitions))
    // println ("It took %d ms to group".format (System.currentTimeMillis - startTime))

    val graph = GraphImpl.fromEdgeList(links, 1,
      useSrcMirror, useDstMirror,
      edgeStorageLevel, vertexStorageLevel, true).cache()
    graph.vertices.count()
    println ("It took %d ms to group".format (System.currentTimeMillis - startTime))
    // val links = mid_data.partitionBy(new HashPartitioner(numVertexPartitions)).cache()
    graph
  }
}
