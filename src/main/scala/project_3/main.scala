package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var g=g_in
    var rgen = scala.util.Random
    var remaining_vertices=2
    while (remaining_vertices >= 1) {
      println("iterating")
      // generate bv
      val v1:VertexRDD[Int]=g.aggregateMessages[Int](
        triplet=>{
          if (triplet.dstAttr != 1 && triplet.dstAttr != -1) {
            triplet.sendToDst(rgen.nextInt(1000) + 5)
          }
          if (triplet.srcAttr != 1 && triplet.srcAttr != -1) {
            triplet.sendToSrc(rgen.nextInt(1000) + 5)
          }
              //if(triplet.srcAttr>triplet.dstAttr){
              //  triplet.srcAttr=1
              //  }
      },
      //merge msgs
      (a,b) => a + b
      )
     // val g1=g.joinVertices(v1)(
       // (id:VertexId,bv:Float)=>bv)
       val g1 = g.joinVertices(v1)(
         (id, old_info, bv) => old_info+bv
       )

      val v2:VertexRDD[Int]=g1.aggregateMessages[Int](
        triplet=>{
          if(triplet.srcAttr>triplet.dstAttr && (triplet.srcAttr != 1 || triplet.srcAttr != -1) && (triplet.dstAttr != 1 || triplet.dstAttr != -1)){
             triplet.sendToDst(-5)
             triplet.sendToSrc(5)
          }
          //fringe case (surrounded by inactive vertices)
          else if (triplet.srcAttr > 1 && triplet.dstAttr == -1) {
            triplet.sendToSrc(5)
          }
          else if (triplet.dstAttr > 1 && triplet.srcAttr == -1) {
            triplet.sendToDst(5)
          }
        },
        //merge msgs
        (a,b)=>(if (a==b) a else 0)
        )

      val g2=g.joinVertices(v2)(
        (id,bv,mark) => if (bv == 1 || bv == -1) bv else mark)


        val v3:VertexRDD[Int]=g2.aggregateMessages[Int](
          triplet=>{
            if(triplet.srcAttr == 5){
              triplet.sendToDst(-1)
              triplet.sendToSrc(1)
            }
            if(triplet.dstAttr == 5){
              triplet.sendToDst(1)
              triplet.sendToSrc(-1)
            }
          },
          //merge msgs
          (a,b) => if (a == -1 || b == -1) {-1} else if (a == 1 && b == 1) 1 else 0
          )
      val g3=g2.joinVertices(v3)(
        (id,old,new1) => new1)

      g=g3
      g.cache()

      remaining_vertices=g.vertices.filter({case(id,x)=>(x!=1)&&(x!=(-1))}).count().toInt

    }
    return g
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    val g=g_in
    val active:VertexRDD[(Boolean)]=g.aggregateMessages[Boolean](
      triplet => {
        if(triplet.srcAttr==1 && triplet.dstAttr==1){
           triplet.sendToDst(false);
           }
         },
        (a,b)=>(a && b)
        )

    if(active.filter{case (id, mark)=>mark==false}.count>0) return false else return true
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
