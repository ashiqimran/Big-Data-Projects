package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ShortestPath {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Shortest Distance")
//    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    var m = sc.textFile(args(0))
      .map(line => {
        val a = line.split(",")
        (a(0).toInt, a(1).toInt, a(2).toInt)
      })

    var RDD = m.groupBy(_._3)
 
    var adj = m.map(m => (m._1, 1000000, m._2, m._3))
    var result = RDD.map(RDD => if (RDD._1 == 0) (RDD._1, 0) else (RDD._1, 1000000))
    for (i <- 1 to 4) {
      adj = adj.map(adj => (adj._4, adj)).join(result.map(res => (res._1, res)))
        .map { case (dest, (adj, res)) => (adj._1, res._2, adj._3, adj._4) }
      
      result = adj.map(adj => (adj._1, adj)).join(result.map(res => (res._1, res)))
        .map { case (dest, (adj, res)) => (adj._4, Math.min(res._2 + adj._3, adj._2)) }
        .reduceByKey(_ min _)

    }
    result = result.sortBy(_._1)
    result = result.filter(res => (res._2 != 1000000))

    result.collect.foreach(println)
//    result.saveAsTextFile(args(1))
    sc.stop()
  }
}
