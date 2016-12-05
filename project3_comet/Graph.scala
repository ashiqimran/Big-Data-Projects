package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer


/*
 * Author: Ashiq Imran
 */

object Graph {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Connected Component of Graph")
    //conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val graph = sc.textFile(args(0)).map(line => {
      val a = line.split(",")
      val adjacent = new ListBuffer[Int]()
      for (i <- 1 to a.length - 1) {
        adjacent += a(i).toInt
      }
      (a(0).toInt, a(0).toInt, adjacent) // group, id, adjacent
    })

  // for(i <- 0 to 5) {
    val adj = graph.flatMap(graph => for (i <- 0 to graph._3.length - 1) yield { (graph._3(i).toInt, (graph._1.toInt)) }) // group, id

    val res = graph.map( graph => (graph._1,graph) ).join(adj.map( adj => (adj._2,adj) ))
                .map { case (k,(graph,adj)) => ((graph._1.toInt),(adj._1.toInt))}
                .reduceByKey(_ min _)


    val minVal = res.map( res =>  (if (res._1 <= res._2) (res._1.toInt, res._1.toInt) else (res._1.toInt, res._2.toInt)))

    var graph2 = graph.map(graph => (graph._2,graph)).join(minVal.map(minVal => (minVal._1,minVal)))
               .map { case (k, (graph,minVal)) => (minVal._2.toInt,graph._2.toInt,graph._3) }




////    /////////////******************** 2nd Iteration**************************////////////////



     val adj2 = graph2.flatMap(graph2 => for (i <- 0 to graph2._3.length - 1) yield { (graph2._3(i).toInt, (graph2._1.toInt)) }) // group, id


     val res2 = adj2.map(adj2 => (adj2._1.toInt,adj2._2.toInt)).reduceByKey(_ min _)


     val graph3 = graph2.map(graph2 => (graph2._2,graph2)).join(res2.map(res2 => (res2._1,res2)))
               .map { case (k, (graph2,res2)) => (res2._2.toInt,graph2._2.toInt,graph2._3) }



//      //    /////////////******************** 3rd Iteration**************************////////////////
    val adj3 = graph3.flatMap(graph3 => for (i <- 0 to graph3._3.length - 1) yield { (graph3._3(i).toInt, (graph3._1.toInt)) }) // group, id


    val res3 = adj3.map(adj3 => (adj3._1.toInt,adj3._2.toInt)).reduceByKey(_ min _)


    val graph4 = graph3.map(graph3 => (graph3._2,graph3)).join(res3.map(res3 => (res3._1,res3)))
               .map { case (k, (graph3,res3)) => (res3._2.toInt,graph3._2.toInt,graph3._3) }


////      //    /////////////******************** 4th Iteration**************************////////////////
    val adj4 = graph4.flatMap(graph4 => for (i <- 0 to graph4._3.length - 1) yield { (graph4._3(i).toInt, (graph4._1.toInt)) }) // group, id

    val res4 = adj4.map(adj4 => (adj4._1.toInt,adj4._2.toInt)).reduceByKey(_ min _)


    val graph5 = graph4.map(graph4 => (graph4._2,graph4)).join(res4.map(res4 => (res4._1,res4)))
               .map { case (k, (graph4,res4)) => (res4._2.toInt,graph4._2.toInt,graph4._3) }

////      //    /////////////******************** 5th Iteration**************************////////////////
    val adj5 = graph5.flatMap(graph5 => for (i <- 0 to graph5._3.length - 1) yield { (graph5._3(i).toInt, (graph5._1.toInt)) }) // group, id

    val res5 = adj5.map(adj5 => (adj5._1.toInt,adj5._2.toInt)).reduceByKey(_ min _)


    val graph6 = graph5.map(graph5 => (graph5._2,graph5)).join(res5.map(res5 => (res5._1,res5)))
               .map { case (k, (graph5,res5)) => (res5._2.toInt,graph5._2.toInt,graph5._3) }


////      //    /////////////******************** Counting**************************////////////////
      val result = graph6.map(graph6 => (graph6._1,graph6._2)).groupByKey()
      result.map(result => (result._1.toInt,result._2.size)).collect().foreach(println)


    sc.stop()
  }
}
