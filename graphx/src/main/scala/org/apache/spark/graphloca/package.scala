package org.apache.spark

import org.apache.spark.util.collection.OpenHashSet

/**
 * Created by XinhuiTian on 17/3/29.
 */
package object graphloca {
  type VertexId = Long

  /** Integer identifier of a graph partition. Must be less than 2^30. */
  // TODO: Consider using Char.
  type PartitionID = Int

  private[graphloca] type VertexSet = OpenHashSet[VertexId]
}
