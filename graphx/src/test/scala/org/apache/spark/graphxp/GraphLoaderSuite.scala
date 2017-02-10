
package org.apache.spark.graphxp

import java.io.{FileOutputStream, OutputStreamWriter, File}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx.LocalSparkContext
import org.apache.spark.util.Utils

/**
 * Created by XinhuiTian on 16/9/27.
 */
class GraphLoaderSuite extends SparkFunSuite with LocalSparkContext {

  test("GraphLoader.edgeListFile") {
    withSpark { sc =>
      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) writer.write(s"$i 0\n")
      writer.close()
      try {
        val graph = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, false, 10)
        //graph.subGraphs.collect()
        //graph.subGraphs.partitionsRDD.foreach { iter => print(iter._1);
        //  iter._2.masterIterator.foreach(print); println }
        //graph.subGraphs.partitionsRDD.foreach { iter => print(iter._1);
        //  iter._2.mirrorIterator.foreach(print); println }
        //graph.vertices.partitionsRDD.foreach { iter => iter._2.iterator.foreach(println); println }
        // graph.vertices.partitionsRDD.foreach { iter => iter._2.shipVertexAttributes(10).foreach{ vb => print(vb._1); vb._2.iterator.foreach(print)}; println}

        /*
        graph.aggregateMessages(
          ctx => ctx.sendToDst(ctx.srcId),
          (id1: VertexId, id2: VertexId) => if (id1 > id2) id1 else id2)


        graph.vertices.partitionsRDD.foreach { iter => iter._2.iterator.foreach{ vb => println(vb._2);}; println}
        */
      } finally {
        Utils.deleteRecursively(tmpDir)
      }
    }
  }
}