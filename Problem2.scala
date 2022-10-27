package org.example

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Problem2 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)

    /* Load file, map edges and create graph structure. Since we want to find the number of vertexes
    that are reachable from a given vertex, the edges of the graph are reversed.*/
    val edges = sc.textFile(args(0))
    val edgelist = edges.map(x => x.split(" ")).map(x => Edge(x(2).toLong, x(1).toLong, true))
    val graph = Graph.fromEdges[Set[VertexId], Boolean](edgelist, Set[VertexId]())

    // Functions for pregel operator. Messages received are combined and added to a vertex's current set.
    def vprog(vertexId: VertexId, value: Set[VertexId], message: Set[VertexId]): Set[VertexId] = {
      value ++ message
    }

    /* Messages contain a set of vertexIds, that includes the vertex's own id and other ids that
     have been previously sent to them (the vertex's current attribute value).*/
    def sendMsg(triplet: EdgeTriplet[Set[VertexId], Boolean]): Iterator[(VertexId, Set[VertexId])] = {
      Iterator((triplet.dstId, triplet.srcAttr.+(triplet.srcId)))
    }

    def mergeMsg(msg1: Set[VertexId], msg2: Set[VertexId]): Set[VertexId] = msg1 ++ msg2

    val minGraph = graph.pregel(Set[VertexId](),
      (graph.numVertices + 1).toInt,
      EdgeDirection.Out)(
      vprog,
      sendMsg,
      mergeMsg)

    // Collect and parses results and save to file.
    val output = minGraph.vertices.collect.map { case (vertexId, (value)) => (vertexId.toInt, value.size.toString) }.sorted.map(x => x._1.toString + ":" + x._2)
    sc.parallelize(output).saveAsTextFile(args(1))

    sc.stop()
  }
}
