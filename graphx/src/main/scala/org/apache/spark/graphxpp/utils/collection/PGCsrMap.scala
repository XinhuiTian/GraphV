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
package org.apache.spark.graphxpp.utils.collection

/**
 * Created by XinhuiTian on 17/3/24.
 */
class PGCsrMap(var ptrs: Array[Int], var values: Array[Int]) {
  def this() = this(Array.empty[Int], Array.empty[Int])

  def wrap(newPtrs: Array[Int], newValues: Array[Int]): Unit = {
    this.ptrs = newPtrs
    this.values = newValues
  }

  @inline def numKeys(): Int = ptrs.length
  @inline def numValues(): Int = values.length

  @inline def begin(id: Int): Iterator[Int] = {
    if (id > numKeys) {
      Iterator.empty
    } else {
      values.slice(ptrs(id), values.length).toIterator
    }
  }

  @inline def ptr(id: Int): Int = {
    if (id >= numKeys) {
      numKeys() - 1
    } else {
      ptrs(id)
    }
  }

  // get keys based on the given id, the keys
  @inline def keyRange(id: Int): Iterator[Int] = {
    if (id >= numKeys) {
      Iterator.empty
    } else if (id + 1 >= numKeys) {
      Array.range(ptrs(id), values.length).toIterator
    } else {
      Array.range(ptrs(id), ptrs(id + 1)).toIterator
    }
  }

  @inline def valueRange(id: Int): Iterator[Int] = {
    if (id >= numKeys) {
      Iterator.empty
    } else if (id + 1 >= numKeys) {
      values.slice(ptrs(id), values.length).toIterator
    } else {
      values.slice(ptrs(id), ptrs(id + 1)).toIterator
    }
  }
}

object PGCsrMap {
  // input: original values
  // output: permuteIndex and ptrs
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

}
