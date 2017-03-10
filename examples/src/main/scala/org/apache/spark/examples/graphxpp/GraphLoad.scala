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

package org.apache.spark.examples.graphxpp

// scalastyle:off println
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphxpp.GraphLoader

/**
 * Created by XinhuiTian on 17/3/9.
 */
object GraphLoad {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println (
        "Usage: GraphLoad <file> --numEPart=<num_edge_partitions> [other options]")
      System.exit (1)
    }

    val fname = args(0)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    val options = mutable.Map(optionsList: _*)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }

    val conf = new SparkConf()

    val sc = new SparkContext(conf.setAppName("GraphLoad(" + fname + ")"))

    val graph = GraphLoader.edgeListFile(sc, args(0), false, numEPart,
      edgePartitioner = "BiEdgePartition").cache()


  }
}
