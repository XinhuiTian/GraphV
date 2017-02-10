/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.graphxpp

import org.apache.spark.graphxpp.impl.VertexAttrBlock
import org.apache.spark.graphxpp.lib.PageRank
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by XinhuiTian on 17/1/5.
 */
class GraphOps[ED: ClassTag, VD: ClassTag](val graph: Graph[ED, VD]) extends Serializable {

  @transient lazy val outDegrees: RDD[(PartitionID, VertexAttrBlock[Int])] =
    degreesRDD(EdgeDirection.Out).setName("GraphOps.outDegrees")

  @transient lazy val degrees: RDD[(PartitionID, VertexAttrBlock[Int])] =
    degreesRDD(EdgeDirection.Either).setName("GraphOps.degrees")

  private def degreesRDD(edgeDirection: EdgeDirection): RDD[(PartitionID, VertexAttrBlock[Int])] = {
    if (edgeDirection == EdgeDirection.In) {
      graph.aggregateMessages(_.sendToDst(1), _ + _, TripletFields.None)
    } else if (edgeDirection == EdgeDirection.Out) {
      graph.aggregateMessages(_.sendToSrc(1), _ + _, TripletFields.None)
    } else { // EdgeDirection.Either
      graph.aggregateMessages(ctx => { ctx.sendToSrc(1); ctx.sendToDst(1) }, _ + _,
        TripletFields.None)
    }
  }

  def staticPageRank(numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] = {
    PageRank.run(graph, numIter, resetProb)
  }

}
