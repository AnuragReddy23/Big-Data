
import org.apache.spark.graphx.{Graph => Graph, VertexId, Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object ConnectedComponents {
  def main ( args: Array[String] ) {
    val config = new SparkConf().setAppName("GraphX")
    val con_sc = new SparkContext(config)


    val G_edges: RDD[ Edge[Long]]=con_sc.textFile(args(0)).map(line => { val (vertex, adjacent) = line.split(",").splitAt(1)
      (vertex(0).toLong,adjacent.toList.map(_.toLong))}).flatMap(e => e._2.map(v => (e._1,v))).map(node =>  Edge(node._1,node._2,node._1))
    
    val grph: Graph[Long, Long]=Graph.fromEdges(G_edges, "defValue").mapVertices((vid, _) => vid)
    
    
    val pregelFunction = grph.pregel(Long.MaxValue,5)(
    (id, val_old, val_new) => math.min(val_old, val_new),
    triple => {
      if (triple.attr < triple.dstAttr)
      {
        Iterator((triple.dstId,triple.attr))
      }
      else if (triple.srcAttr < triple.attr)
      {
        Iterator((triple.dstId,triple.srcAttr))
      }
      else
      {
        Iterator.empty
      }
    },
    (a,b) => math.min(a,b)
    )
   
    val out = pregelFunction.vertices.map(g => (g._2,1)).reduceByKey(_ + _).sortByKey().map(x => x._1.toString() + " " + x._2.toString())
    out.collect().foreach(println)
    
  }
}
