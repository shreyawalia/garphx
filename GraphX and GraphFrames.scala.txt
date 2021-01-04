// Databricks notebook source
// importing packages required for GraphX
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
// importing packages required for GraphFrames
import org.graphframes._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//Creating case class shcema for our data
case class Data_flight(trip_id:Int, day_of_month:String, day_of_week:String, carrier_code:String, tail_num:String, flight_num:String, origin_id:Long, origin:String, dest_id:Long, dest:String, dest_state:String, scheduled_dept_time:String, actual_dept_time:String, dep_delay_mins:String, scheduled_arr_time:String, actual_arr_time:String, arr_delay_mins:String,elapsed_time_mins:String, total_flights:Long, distance:Int, city:String)

// COMMAND ----------

// Creating parse_flight_data function for parsing the file, whenever we load it
def parse_flight_data(str: String): Data_flight = {
  val line = str.split(",")
  Data_flight(line(0).toInt, line(1), line(2), line(3), line(4), line(5), line(6).toLong, line(7), line(8).toLong, line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16), line(17), line(18).toLong, line(19).toInt, line(20).toString)
}

// COMMAND ----------

//loading csv file of our data in data_RDD
val data_RDD = sc.textFile("/FileStore/tables/data3.csv")
//View of our data 
data_RDD.toDF().show(10,false)

//parsing the input file from data_RDD using parse_flight_data function and saving it in flights_RDD
val flights_RDD = data_RDD.map(parse_flight_data).cache()



// COMMAND ----------

//View of how our data looks like
flights_RDD.toDF().show(5)

// COMMAND ----------

//Working with GraphX

// COMMAND ----------

//Creating RDD for airports as vertices, using origin airport ID and airports
val vertices_airports = flights_RDD.map(flight => (flight.origin_id, flight.origin)).distinct    
    vertices_airports.take(5)



// COMMAND ----------

// Defining a default vertex called default_vertex
val default_vertex = "default_vertex"

//Creating RDD, routes_RDD for origin_id, destination_id of airports, and distance
val routes_RDD = flights_RDD.map(flight => ((flight.origin_id.toLong, flight.dest_id.toLong), flight.distance.toInt)).distinct
routes_RDD.cache
routes_RDD.take(5)

// COMMAND ----------

// Since origin_id is numerical - Mapping origin_id to the 3-letter airport code
val airport_map = vertices_airports.map { case ((origin_id), name) => (origin_id -> name) }.collect.toList.toMap



// COMMAND ----------

// Defining the routes_RDD as edges for our graph
val edges = routes_RDD.map { case ((origin_id, dest_id), distance) => Edge(origin_id.toLong, dest_id.toLong, distance) }

edges.take(5)

// COMMAND ----------

// defining the graph using vertices_airports, edges, and default_vertex
val graph = Graph(vertices_airports, edges, default_vertex)  

// graph's vertices
graph.vertices.take(5)



// COMMAND ----------

//graph's edges
graph.edges.take(5)



// COMMAND ----------

// How many airports?
val total_airports = graph.numVertices



// COMMAND ----------

// How many routes?
val total_routes = graph.numEdges



// COMMAND ----------

// origin_id, dest_id, and distance of the routes greater than 4500 miles distance?
graph.edges.filter { case ( Edge(origin_id, dest_id, distance))=> distance > 4500}.take(5)



// COMMAND ----------

//The edge class with the source and destination properties, respectively.
graph.triplets.take(5).foreach(println)



// COMMAND ----------

//Longest routes (distance) in our data with source and destination airports. Also sorting it in descending order
graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
     "Total distance is " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(5).foreach(println)



// COMMAND ----------

// Computing the highest degree vertex by using a reduce operation
def maximum_function(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
 if (a._2 > b._2) a else b
}

//Fetching airport_id with maximum incoming edges or flights 
val maximum_incoming: (VertexId, Int) = graph.inDegrees.reduce(maximum_function)



// COMMAND ----------

//Fetching airport_id with maximum outcoming edges or flights 
val maximum_outgoing: (VertexId, Int) = graph.outDegrees.reduce(maximum_function)



// COMMAND ----------

//Fetching airport_id with both maximum incoming and outcoming edges or flights 
val maximum_degrees: (VertexId, Int) = graph.degrees.reduce(maximum_function)



// COMMAND ----------

airport_map(11298)

// COMMAND ----------

// Fetching top 5 airport codes with maximum incoming flights:
val max_incoming_flights = graph.inDegrees.collect.sortWith(_._2 > _._2).map(x => (airport_map(x._1), x._2)).take(5)  

max_incoming_flights.foreach(println)



// COMMAND ----------

//Fetching top 5 airport codes (vertices) with maximum outgoing flights:
val max_outcoming_flights= graph.outDegrees.join(vertices_airports).sortBy(_._2._1, ascending=false).take(5)  

max_outcoming_flights.foreach(println)



// COMMAND ----------

// using pageRank function with 0.001 tolerance to rank airports based on maximum number of vertices (routes)
val define_rank = graph.pageRank(0.001).vertices



// COMMAND ----------

// joining the ranks of airports with the map of airport id to name
val airport_rank= define_rank.join(vertices_airports)  

airport_rank.take(10).foreach(println)



// COMMAND ----------

// sorting the airports by ranks
val sort_airport_rank = airport_rank.sortBy(_._2._1, false)

sort_airport_rank.take(10)

// COMMAND ----------

// Getting the airport code of the top 10 airports with highest rank
val important_10_airports =sort_airport_rank.map(_._2._2)
important_10_airports.take(10)

// COMMAND ----------

//Pregel to find flights with cheapest airfare from DFW, 11298 airport
// defining starting vertex ID as 11298 which is for 'DFW' airport
val source_Id: VertexId = 11298
// Transforming edges (distance) into flight fare to compute the cost and defining it in another graph
val new_graph = graph.mapEdges(e => 50.toDouble + e.attr.toDouble/0.11 )

// initializing new graph. Setting distance of all vertices except source, infinity
val initialize_graph = new_graph.mapVertices((id, _) => if (id == source_Id) 0.0 else Double.PositiveInfinity)

// calling pregel on initialize_graph
val call_pregel = initialize_graph.pregel(Double.PositiveInfinity)(
 // Defining vertex program to preserve the minimum distance of actual distance (airfare cost) and new distance (airfare cost)
 (id, dist, newDist) => math.min(dist, newDist),
  //Triplet sends off message to neighboring vertices and increment occurs
 triplet => {
  if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
   Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
  } else {
   Iterator.empty
  }
 },
  //Merge messages
 // Defining reduce operation to preserve minimum distance (airfare cost) incase multiple messages are received for the same vertex
 (a,b) => math.min(a,b)
)  

// COMMAND ----------

//Shows airport_ids (source, destination) of routes with lowest airfare price
println(call_pregel.edges.take(5).mkString("\n"))



// COMMAND ----------

//Shows airport codes (source, destination) of routes with lowest airfare price in sorted ascending order
call_pregel.edges.map{ case ( Edge(origin_id, dest_id, airfare))=> ( (airport_map(origin_id), airport_map(dest_id), airfare)) }.takeOrdered(5)(Ordering.by(_._3)).mkString("\n")

// COMMAND ----------

// Airports id with lowest airfare price
println(call_pregel.vertices.take(5).mkString("\n"))



// COMMAND ----------

// Airports code with lowest airfare price; DFW = 0.0 because that is our start vertex.
call_pregel.vertices.collect.map(x => (airport_map(x._1), x._2)).sortWith(_._2 < _._2).take(10).mkString("\n")



// COMMAND ----------

// Airports code with lowest airfare price; DFW = 0.0 because that is our start vertex.
call_pregel.vertices.collect.map(x => (airport_map(x._1), x._2)).sortWith(_._2 < _._2).take(10)

//Can use more algorithms in addition to those supported to GraphX, like breadth first and also supports Java and python APIs


// COMMAND ----------

//Working with GraphFrames

// COMMAND ----------

// For creating data frames
val df = flights_RDD.toDF()
df.printSchema()

val city_data = df.select("origin").distinct()
val trip_data = df.select("origin", "dest", "dep_delay_mins", "distance", "dest_state", "scheduled_dept_time")


// COMMAND ----------

city_data.printSchema()
trip_data.printSchema()

val trip_vertices = city_data
  .withColumnRenamed("origin", "id")

val trip_edges = trip_data
  .withColumnRenamed("origin", "src")
  .withColumnRenamed("dest", "dst")

// COMMAND ----------

val trip_graph = GraphFrame(trip_vertices, trip_edges)

trip_edges.cache()
trip_vertices.cache()

// COMMAND ----------

trip_graph.vertices.show(5)
trip_graph.edges.show(5)

// COMMAND ----------

// Total number of airports: 
trip_graph.vertices.count


// COMMAND ----------

// Total number of flights:
trip_graph.edges.count

// COMMAND ----------

// flights covering more than 800 miles distance
trip_graph.edges.filter("distance > 800").show


// COMMAND ----------

//The GraphFrames triplets put all of the edge, src, and dst columns together in a DataFrame.
//triplets = src edge dst
trip_graph.triplets.show


// COMMAND ----------

//longest routes
trip_graph.edges
  .groupBy("src", "dst")
  .max("distance")
  .sort(desc("max(distance)")).show

// COMMAND ----------

// distance is greater than 1000 miles for DTW airport arranged by delay
trip_graph.edges.filter("distance > 1000 and src == 'DTW'")
.orderBy(desc("dep_delay_mins")).show(5)


// COMMAND ----------

//Flights where destination state is california
trip_graph.edges.filter("dest_state ='CA'").show

// COMMAND ----------

//busiest flight routes as per trip count
val most_trips = trip_graph
  .edges
  .groupBy("src", "dst")
  .count()
  .orderBy(desc("count"))
  .limit(10)

display(most_trips)

// COMMAND ----------

val res = trip_graph
 .find("(a)-[]->(b); !(b)-[]->(a)")

display(res)

// COMMAND ----------

val in_degree = trip_graph.inDegrees
display(in_degree.orderBy(desc("inDegree")).limit(15))


// COMMAND ----------

val out_degree = trip_graph.outDegrees
display(out_degree.orderBy(desc("outDegree")).limit(15))


// COMMAND ----------

val degree_ratio = in_degree.join(out_degree, in_degree.col("id") === out_degree.col("id"))
  .drop(out_degree.col("id"))
  .selectExpr("id", "double(inDegree)/double(outDegree) as degree_ratio")

degree_ratio.cache()
  
display(degree_ratio.orderBy(desc("degree_ratio")).limit(10))

// COMMAND ----------

display(trip_graph.edges.filter("src = 'SFO' and dep_delay_mins > 100"))


// COMMAND ----------

//breadth-first
val paths = trip_graph.bfs.fromExpr("id = 'ATL'")
 .toExpr("id = 'LGA'")
 .maxPathLength(1).run().limit(4)
paths.show()

// COMMAND ----------

trip_graph.edges.filter("src = 'ATL' and dep_delay_mins > 1")
 .groupBy("scheduled_dept_time")
 .avg("distance")
 .sort(desc("avg(distance)")).show(5)


// COMMAND ----------

//using motif with dataframe operation
val motif =trip_graph.find("(a)-[ab]->(b); (b)-[bc]->(c)")
.filter("a.id = 'LGA'")
.filter("c.id = 'BOS'").limit(5)

display(motif)

// COMMAND ----------

//breadth-first algorithm
val breadth_first = trip_graph.bfs
  .fromExpr((col("id") === "JFK"))
  .toExpr(col("id") === "ORD")
  .maxPathLength(1).run()

display(breadth_first)

// COMMAND ----------

// use pageRank
val page_ranks = trip_graph.pageRank.resetProbability(0.15).tol(0.01).run()

page_ranks.vertices.orderBy($"pagerank".desc).show()


// COMMAND ----------

//shortest-path
val shortest_path = trip_graph.shortestPaths.landmarks(Seq("ORD")).run()
display(shortest_path)
