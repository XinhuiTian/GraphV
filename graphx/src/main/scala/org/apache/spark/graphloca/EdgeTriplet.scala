package org.apache.spark.graphloca

/**
 * Created by XinhuiTian on 17/3/30.
 */
class EdgeTriplet[VD, ED] extends Edge[ED] {

  /**
   * The source vertex attribute
   */
  var srcAttr: VD = _ // nullValue[VD]

  /**
   * The destination vertex attribute
   */
  var dstAttr: VD = _ // nullValue[VD]

  /**
   * Set the edge properties of this triplet.
   */
  protected[spark] def set(other: Edge[ED]): EdgeTriplet[VD, ED] = {
    srcId = other.srcId
    dstId = other.dstId
    attr = other.attr
    this
  }

  /**
   * Given one vertex in the edge return the other vertex.
   *
   * @param vid the id one of the two vertices on the edge
   * @return the attribute for the other vertex on the edge
   */
  def otherVertexAttr(vid: VertexId): VD =
    if (srcId == vid) dstAttr else { assert(dstId == vid); srcAttr }

  /**
   * Get the vertex object for the given vertex in the edge.
   *
   * @param vid the id of one of the two vertices on the edge
   * @return the attr for the vertex with that id
   */
  def vertexAttr(vid: VertexId): VD =
    if (srcId == vid) srcAttr else { assert(dstId == vid); dstAttr }

  override def toString: String = ((srcId, srcAttr), (dstId, dstAttr), attr).toString()

  def toTuple: ((VertexId, VD), (VertexId, VD), ED) = ((srcId, srcAttr), (dstId, dstAttr), attr)
}

class TripletFields(val useSrc: Boolean,
  val useDst: Boolean, val useEdge: Boolean) extends Serializable {

  def this() = this(true, true, true)
}

object TripletFields {
  val None = new TripletFields(false, false, false)

  val EdgeOnly = new TripletFields(false, false, true)

  val Src = new TripletFields(true, false, true)

  val Dst = new TripletFields(false, true, true)

  val All = new TripletFields(true, true, true)
}
