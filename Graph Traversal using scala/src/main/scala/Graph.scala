import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

case class ver(node: Long, grp: Long, adjacent123: List[Long])

object Graph {
  def main(args: Array[String]) {

    val con = new SparkConf().setAppName("Connected Components of Graph")
    con.setMaster("local[2]")
    val config = new SparkContext(con)

    // A graph is a dataset of vertices, where each vertex is a triple
    //   (grp,id,adj) where id is the vertex id, grp is the grp id
    //   (initially equal to id), and adj is the list of outgoing neighbors
    var grph = config.textFile(args(0)).map(line => {

      val in = line.split(",")

      (in(0).toLong, in(0).toLong, in.slice(1, in.length).toList.map(_.toLong))
    })

    for (loop <- 1 to 5) {

        // For each vertex (grp,id,adj) generate the candidate (id,grp)
       //    and for each y1 in adj generate the candidate (y1,grp).
       // Then for each vertex, its new grp number is the minimum candidate

      grph = grph.flatMap(v1 => v1._3.flatMap(in => Seq((in, v1._2))) ++ Seq((v1._1, v1._2)))
        .reduceByKey((y1, z1) => y1 min z1)
        .join(grph.map(v1 => (v1._1, v1)))
        .map { case (node, (min, vertex)) => (node, min, vertex._3) }
    }

        // print the grp sizes
    val output = grph.map(v1 => (v1._2, 1)).reduceByKey(_+_).sortBy(_._1)
    output.map(y1 => {
      y1._1 + "\t" + y1._2
    }).collect().foreach(println)
    config.stop()
  }
}
