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
package org.apache.spark.graphxpp.utils

// scalastyle:off println
import org.apache.spark.graphxpp.utils.collection.CstStorage

/**
 * Created by XinhuiTian on 17/2/28.
 */

object PGStyle {
  // counting sort for edge order, method from PowerGraph
  def countingSort(values: Array[Int]): (Array[Int], Array[Int]) = {
    // require(values.size > 0)
    val size = values.size
    val max = values.max
    val countingArray = new Array[Int](max + 1)
    val permuteIndex = new Array[Int](size)
    for (i <- 0 until size) {
      countingArray(values(i)) += 1
    }

    for (i <- 1 until max + 1) {
      countingArray(i) += countingArray(i - 1)
    }

    for (i <- 0 until size) {
      val index = values(i)
      countingArray(index) -= 1
      permuteIndex(countingArray(index)) = i
    }

    (permuteIndex, countingArray)
  }

  def realEdgeSort(localSrdIds: Array[Int], localDstIds: Array[Int]):
    ((Array[Int], Array[Int]), Array[Int]) = {
    val (permuteIndex, srcPrefix) = countingSort(localSrdIds)

    var swap_src = 0
    var swap_dst = 0

    for (i <- 0 until permuteIndex.size) {
      if (i != permuteIndex(i)) {
        var j = i
        swap_src = localSrdIds(i)
        swap_dst = localDstIds(i)

        var flag = true
        while (j != permuteIndex(i) && flag == true) {
          val next = permuteIndex(j)
          if (next != i) {
            localSrdIds(j) = localSrdIds(next)
            localDstIds(j) = localDstIds(next)
            permuteIndex(j) = j
            j = next
          } else {
            localSrdIds(j) = swap_src
            localDstIds(j) = swap_dst
            permuteIndex(j) = j
            flag = false
          }
        }
      }
    }

    localSrdIds.foreach(println)

    val (permuteDstIndex, dstPrefix) = countingSort(localDstIds)
    val newSrcIds = new Array[Int](localSrdIds.size)

    for (i <- 0 until permuteDstIndex.size) {
      // println(permuteDstIndex(i))
      newSrcIds(i) = localSrdIds(permuteDstIndex(i))
    }

    val finalSrcIds = (newSrcIds, permuteDstIndex)
    val finalDstIds = localDstIds

    val csrStorage = new CstStorage(srcPrefix, localDstIds)

    for (i <- 0 until srcPrefix.size) {
      csrStorage.valueRange(i).foreach{ e => print(s"($i, $e) ")}
      println
    }

    val srcIds = newSrcIds.zip(permuteDstIndex)

    val cscStorage = new CstStorage(dstPrefix, srcIds)

    for (i <- 0 until dstPrefix.size) {
      cscStorage.valueRange(i).foreach { e => print(s"(${e._1}, $i) ")}
      println
    }

    println(srcIds.slice(1, 1).length)

    (finalSrcIds, finalDstIds)
  }

  def scanEdges(localSrcIds: Array[Int], localDstIds: Array[Int]): Unit = {
    val ((finalSrcIds, permuteDstIndex), finalDstIds) = realEdgeSort(localSrcIds, localDstIds)

  }
}
