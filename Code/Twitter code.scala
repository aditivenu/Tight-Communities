// Databricks notebook source
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

// Load the the text file with edges as graphs
val graph = GraphLoader.edgeListFile(sc, "/FileStore/tables/ynydmu691493567594669/twitter_combined.txt")
// Find the connected components without any modification on the edges file
val cc = graph.connectedComponents().vertices
// Find the strongly connected components without any modification on the edges file
val scc = graph.stronglyConnectedComponents(numIter = 2).vertices

// Print the connected components
println(cc.collect().mkString("\n"))

// COMMAND ----------

// print the strongly connected components
println(scc.collect().mkString("\n"))

// COMMAND ----------

// Load the edges in canonical order and partition the graph for triangle count
val graphPart = graph.partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex
val triCounts = graph.triangleCount().vertices

// Print the triangle count result
println(triCounts.collect().mkString("\n"))


// COMMAND ----------

val degree = graph.degrees
// find the clustering coefficient for all the nodes in the graph
val clustCoeff = triCounts.join(degree).map(row => (row._1,(2*row._2._1.toDouble)/(row._2._2*(row._2._2-1).toDouble)))

//Displaying the node and its coefficient after sorting on the coefficient value
println(clustCoeff.sortBy(line => -line._2).collect().mkString("\n"))


// COMMAND ----------

//Filter the nodes with coefficient greater than a threshold value
val filtrd = clustCoeff.filter(line=>(line._2 >0.4)).map(line=>(line._1.toInt,line._2.toInt))
val Data = sc.textFile("/FileStore/tables/ynydmu691493567594669/twitter_combined.txt");
val rawGraph = Data.map(line => line.split(' ')).map(line =>(line(0).toInt,line(1).toInt))

//Merge the filtered nodes with raw graph file
val result = rawGraph.join(filtrd).map(line=>(line._2._1,line._1)).join(filtrd).map(line=>(line._2._1,line._1))
val edgeRDD = result.map(line => Edge(line._1,line._2,""))

//Create a  graph with the filtered edges
val graphFiltered  = Graph.fromEdges(edgeRDD, defaultValue = 1) 

//Find connected components of the filtered graph
val ccfiltrd = graphFiltered.connectedComponents().vertices
val components = ccfiltrd.map(line=>line._2).distinct().collect()
val components_count = ccfiltrd.map(line =>(line._2,1)).reduceByKey((x,y) => (x+y)).toDF("Component centre","Count of Nodes")

//print the component centre and number of nodes in it, ordered by the number of nodes
display(components_count.orderBy($"Count of Nodes".desc))
println("Number of connected components "+components_count.count)

// COMMAND ----------

// find strongly connected components on the filtered graph
val sccfiltrd = graphFiltered.stronglyConnectedComponents(numIter = 2).vertices
val strongComponents = sccfiltrd.map(line=>line._2).distinct().collect()
val strongComponents_count = sccfiltrd.map(line =>(line._2,1)).reduceByKey((x,y) => (x+y)).toDF("Strong component centre","Count of Nodes")

//print the strongly connected component centre and number of nodes in it, ordered by the number of nodes
display(strongComponents_count.orderBy($"Count of Nodes".desc))

println("Number of Strongly connected components "+strongComponents_count.count)
