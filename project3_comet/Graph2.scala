package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph2 {
  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("Graph")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    var m = sc.textFile(args(0))
              .map( line => { val a = line.split(",").toList
                              (a.head.toLong,a.head.toLong,a.tail.map(_.toLong))
                            } )
                         m.collect.foreach(println)
    for(i <- 1 to 5)
        m = m.flatMap{ case (group,id,adj) => (id,group)::adj.map((_,group)) }
      //  m.collect.foreach(println)
             .reduceByKey(_ min _)
             .join(m.map{ case (_,id,adj) => (id,adj) })
             .map{ case (id,(group,adj)) => (group,id,adj) }
             
             m.collect.foreach(println)
    m.map{ case (group,id,adj) => (group,id) }
             println("GROUP ID") 
             m.collect.foreach(println)
    // .countByKey.foreach(println)
    sc.stop()
  }
}