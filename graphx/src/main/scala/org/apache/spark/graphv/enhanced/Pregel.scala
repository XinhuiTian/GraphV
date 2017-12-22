
package org.apache.spark.graphv.enhanced

import scala.reflect.ClassTag

import org.apache.spark.graphv.{EdgeDirection, VertexId}
import org.apache.spark.internal.Logging

object Pregel extends Logging {

  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag](
      graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Out,
      triplets: TripletFields = TripletFields.BothSidesWithEdge,
      needActive: Boolean = false)
    (initialFunc: (VertexId, VD) => VD,
        vFunc: (VertexId, VD, A) => VD,
      sendMsg: GraphVEdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A): Graph[VD, ED] = {

    // init values and the active states
    var g = graph.mapVertices((vid, v) => initialFunc(vid, v), needActive).cache()

    var activeNums = g.getActiveNums
    println("activeNums: " + activeNums)

    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeNums > 0 && (needActive || i < maxIterations)) {
      val jobStartTime = System.currentTimeMillis
      println("activeNums: " + activeNums)
      prevG = g

      g = prevG.mapReduceTriplets(sendMsg, mergeMsg, vFunc,
        activeDirection, triplets, needActive)
        .cache()

      // g.vertices.foreach(println)

      g.vertices.count()

      println ("iteration  " + i + "  It took %d ms count message"
        .format (System.currentTimeMillis - jobStartTime))
      activeNums = g.getActiveNums
      prevG.unpersist(blocking = false)
      i += 1
    }
    g
  }
}
