import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
object Graph {
  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)
    def read(line:Array[String]) :(Long,Long,List[Long]) ={
      var node = line(0).toLong
      var i = 1
      var l = new ListBuffer[Long]()
      l += line(i).toLong
      i=i+1
      while(i< line.size) {
        l += line(i).toLong
        i = i + 1;
      }
      return (node,node, l.toList)
    }
    var graph = sc.textFile(args(0)).map(line => {  read(line.split(","))})
    var graph1 = graph.map( g1 => (g1._1,g1))
    for(i <- 1 to 5){
      graph = graph.flatMap(map => map match { case (x, y, z) => (x, y) :: z.map( a => (a,y)) })
          .reduceByKey( (g1,g2) => (if(g1>=g2) g2 else g1) ).join(graph1).map( gr => (gr._2._2._2 , gr._2._1, gr._2._2._3))
    }
    val result = graph.map(g1 => (g1._2,1)).reduceByKey( (x,y) => (x+y)).sortByKey().collect()
    result.foreach(println)
    }
}
