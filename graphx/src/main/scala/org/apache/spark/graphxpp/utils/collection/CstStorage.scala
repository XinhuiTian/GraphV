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
 * Borrow from PowerGraph
 * A compact key-value(s) data structure
 * The core operation of is querying the list of values associated with the query key *
 * returns the begin and end iterators via begin(id) and end(id)
 */
class CstStorage[VD: ClassTag](var ptrs: Array[Int], var values: Array[VD]) {
  def this() = this(Array.empty[Int], Array.empty[VD])

  def wrap(newPtrs: Array[Int], newValues: Array[VD]): Unit = {
    this.ptrs = newPtrs
    this.values = newValues
  }

  @inline def numKeys(): Int = ptrs.length
  @inline def numValues(): Int = values.length

  @inline def begin(id: Int): Iterator[VD] = {
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

  @inline def valueRange(id: Int): Iterator[VD] = {
    if (id >= numKeys) {
      Iterator.empty
    } else if (id + 1 >= numKeys) {
      values.slice(ptrs(id), values.length).toIterator
    } else {
      values.slice(ptrs(id), ptrs(id + 1)).toIterator
    }
  }
}
