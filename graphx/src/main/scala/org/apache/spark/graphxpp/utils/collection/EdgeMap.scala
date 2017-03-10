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

import scala.reflect.ClassTag

/**
 * Created by XinhuiTian on 17/3/1.
 * store the map from localSrcId/localDstId to the range of OutEdges/InEdges
 */
class EdgeMap[VD: ClassTag](var ptrs: Array[Int], var values: Array[VD]) {
  def this() = this(Array.empty[Int], Array.empty[VD])

  def wrap(newPtrs: Array[Int], newValues: Array[VD]): Unit = {
    this.ptrs = newPtrs
    this.values = newValues
  }

  @inline def numPtrs(): Int = ptrs.length
  @inline def numValues(): Int = values.length

  @inline def ptr(id: Int): Int = {
    if (id >= numPtrs) {
      numPtrs() - 1
    } else {
      ptrs(id)
    }
  }

  @inline def begin(id: Int): Int = ptr(id)
  @inline def end(id: Int): Int = ptr(id + 1)

  @inline def valueRange(id: Int): Iterator[VD] = {
    if (id >= numPtrs) {
      Iterator.empty
    } else if (id + 1 >= numPtrs) {
      values.slice(ptrs(id), values.length).toIterator
    } else {
      values.slice(ptrs(id), ptrs(id + 1)).toIterator
    }
  }

}
