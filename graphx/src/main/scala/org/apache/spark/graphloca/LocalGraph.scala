package org.apache.spark.graphloca

import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import org.apache.spark.graphloca.impl.LocalGraphPartition
import org.apache.spark.rdd.RDD

/**
 * Created by XinhuiTian on 17/3/29.
 */
abstract class LocalGraph[VD, ED](
  sc: SparkContext,
  deps: Seq[Dependency[_]]
) extends RDD[Edge[ED]](sc, deps) {

  def partitionsRDD: RDD[(PartitionID, LocalGraphPartition[VD, ED])]

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val p = firstParent[(PartitionID, LocalGraphPartition[ED, _])].iterator(part, context)
    if (p.hasNext) {
      // TODO: why copy?
      Iterator.empty
      //p.next()._2.edgeIterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }
}

object LocalGraph {



}
