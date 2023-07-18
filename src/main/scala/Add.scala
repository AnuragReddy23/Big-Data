import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Add {
  val rows = 100
  val columns = 100

  case class Block ( data: Array[Double] ) {
    override
    def toString (): String = {
      var s = "\n"
      for ( i <- 0 until rows ) {
        for ( j <- 0 until columns )
          s += "\t%.3f".format(data(i*rows+j))
        s += "\n"
      }
      s
    }
  }

  /* Convert a list of triples (i,j,v) into a Block */
  def toBlock ( triples: List[(Int,Int,Double)] ): Block = {
 var triple_array:Array[Double] = new Array[Double](rows*columns)
    triples.foreach(triple=> {
      triple_array(triple._1*rows+triple._2) = triple._3
    })

    new Block(triple_array)

  }
  def blockAdd ( m_mat: Block, n_mat: Block ): Block = {
 var triple_array:Array[Double] = new Array[Double](rows*columns)
    for ( i <- 0 until rows ) {
      for ( j <- 0 until columns )
        triple_array(i*rows+j) = m_mat.data(i*rows+j) + n_mat.data(i*rows+j)
    }
    new Block(triple_array)

  }
  /* Read a sparse matrix from a file and convert it to a block matrix */
  def createBlockMatrix ( scan: SparkContext, file: String ): RDD[((Int,Int),Block)] = {
    /* ... */
      scan.textFile(file).map( line => { val a = line.split(",")
      ((a(0).toInt/rows, a(1).toInt/columns), (a(0).toInt%rows, a(1).toInt%columns, a(2).toDouble)) } )
      .groupByKey().map{ case (key,value) => (key, toBlock(value.toList)) }
  }

 def main ( args: Array[String] ) {
    /* ... */
  val con = new SparkConf().setAppName("Spark_Add")
    val conf = new SparkContext(con)

    val M_mat= createBlockMatrix(conf , args(0))
    val N_mat=  createBlockMatrix(conf , args(1))

    val output = M_mat.join(N_mat).map{ case(key, (m_mat, n_mat)) => (key, blockAdd(m_mat, n_mat)) }
    output.saveAsTextFile(args(2))
    conf.stop()
 }
}


