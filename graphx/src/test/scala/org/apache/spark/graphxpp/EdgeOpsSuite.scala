
package org.apache.spark.graphxpp


import java.io.{FileOutputStream, OutputStreamWriter, File}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx.LocalSparkContext
import org.apache.spark.util.Utils

/**
 * Created by XinhuiTian on 16/12/30.
 */
class EdgeOpsSuite extends SparkFunSuite with LocalSparkContext {

  test("MapVertices") {
    withSpark { sc =>
      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      // println(tmpDir.getAbsolutePath)
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) {
        writer.write(s"$i 0\n")
      }
      writer.close()
      try {
        var g = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, false, 10)
        g = g.mapVertices((vid, i) => 10)

        g.edges.partitionsRDD.foreach { iter =>
          val part = iter._2
          for (i <- 0 until part.getMasters.size) {
            println(part.vertexAttr(i))
          }
        }

        g = g.mapVertices((vid, i) => i * 2)

        g.edges.partitionsRDD.foreach { iter =>
          val part = iter._2
          for (i <- 0 until part.getMasters.size) {
            println(part.vertexAttr(i))
          }
        }

      } finally {
        Utils.deleteRecursively(tmpDir)
      }
    }
  }

  test("shipMasters") {
    withSpark { sc =>
      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      // println(tmpDir.getAbsolutePath)
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) {
        val j = i - 1
        writer.write(s"$i $j\n")
      }
      writer.close()
      try {
        var g = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, false, 10)
        g = g.mapVertices((vid, i) => 10)

        g.edges.partitionsRDD.foreach {iter =>
          val part = iter._2
          part.shipMasterVertexAttrs.foreach { iter =>
            println(iter._1)
            iter._2.iterator.foreach(println)
            println
          }
        }
      } finally {
        Utils.deleteRecursively(tmpDir)
      }
    }
  }

  test("upgrade") {
    withSpark { sc =>
      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      // println(tmpDir.getAbsolutePath)
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) {
        val j = i - 1
        writer.write(s"$i $j\n")
      }
      writer.close()
      try {
        var g = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, false, 10)
        g = g.mapVertices((vid, i) => 10)
        g.upgrade

        g.edges.partitionsRDD.foreach {iter =>
          val part = iter._2
          println(iter._1)
          for (i <- 0 until part.getMirrors.size) {
            println(part.vertexAttr(i + part.getMasters.size))
          }
        }
      } finally {
        Utils.deleteRecursively(tmpDir)
      }
    }
  }

  test("upgrade") {
    withSpark { sc =>
      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      // println(tmpDir.getAbsolutePath)
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) {
        val j = i - 1
        writer.write(s"$i $j\n")
      }
      writer.close()
      try {
        var g = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, false, 10)
        g = g.mapVertices((vid, i) => 10)
        g.upgrade

        g.edges.partitionsRDD.foreach {iter =>
          val part = iter._2
          println(iter._1)
          for (i <- 0 until part.getMirrors.size) {
            println(part.vertexAttr(i + part.getMasters.size))
          }
        }
      } finally {
        Utils.deleteRecursively(tmpDir)
      }
    }
  }

  test("upgrade") {
    withSpark { sc =>
      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      // println(tmpDir.getAbsolutePath)
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) {
        val j = i - 1
        writer.write(s"$i $j\n")
      }
      writer.close()
      try {
        var g = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath, false, 10)
        g = g.mapVertices((vid, i) => 10)
        g.upgrade

        g.edges.partitionsRDD.foreach {iter =>
          val part = iter._2
          println(iter._1)
          for (i <- 0 until part.getMirrors.size) {
            println(part.vertexAttr(i + part.getMasters.size))
          }
        }
      } finally {
        Utils.deleteRecursively(tmpDir)
      }
    }
  }


}
