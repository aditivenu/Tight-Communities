// Databricks notebook source
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphLoader

// Load the the text file with edges as graphs
val graph = GraphLoader.edgeListFile(sc,"/FileStore/tables/hq7qo29w1490991974254/facebook_combined.txt",true) .partitionBy(PartitionStrategy.RandomVertexCut)

// Find the connected components without any modification on the edges file
val cc = graph.connectedComponents().vertices

// Print the connected components
println(cc.collect().mkString("\n"))

// COMMAND ----------

// Find the triangle count for each vertex
val triCounts = graph.triangleCount().vertices
// Print the triangle count result
println(triCounts.collect().mkString("\n"))

// COMMAND ----------

val Data= sc.textFile("/FileStore/tables/hq7qo29w1490991974254/facebook_combined.txt")
val rawGraph = Data.map(line => line.split(' ')).map(line =>(line(0).toInt,line(1).toInt))
val degree = graph.degrees
// find the clustering coefficient for all the nodes in the graph
val clustCoeff = triCounts.join(degree).map(row => (row._1,(2*row._2._1.toDouble)/(row._2._2.toDouble*(row._2._2-1).toDouble)))
//Displaying the node and its coefficient after sorting on the coefficient value
println(clustCoeff.sortBy(line => -line._2).collect().mkString("\n"))

// COMMAND ----------

//Filter the nodes with coefficient greater than a threshold value
val filtrd = clustCoeff.filter(line=>(line._2 >0.3)).map(line=>(line._1.toInt,line._2.toInt))
//Merge the filtered nodes with raw graph file
val result = rawGraph.join(filtrd).map(line=>(line._2._1,line._1)).join(filtrd).map(line=>(line._2._1,line._1))
val edgeRDD = result.map(line => Edge(line._1,line._2,""))
//Create a  graph with the filtered edges
val graphFiltered  = Graph.fromEdges(edgeRDD, defaultValue = 1) 
//Find connected components of the filtered graph
val ccfiltrd = graphFiltered.connectedComponents.vertices
val components = ccfiltrd.map(line=>line._2).distinct().collect()
val components_count = ccfiltrd.map(line =>(line._2,1)).reduceByKey((x,y) => (x+y)).toDF("Component centre","Count of Nodes")
//print the component centre and number of nodes in it, ordered by the number of nodes
display(components_count.orderBy($"Count of Nodes".desc))
var compCount = components_count.count()
println("Number  of components ="+compCount)
