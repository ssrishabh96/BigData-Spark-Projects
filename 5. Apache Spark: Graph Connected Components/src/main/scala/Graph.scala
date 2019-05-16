import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

  object Graph{
    def main(args: Array[String]){
      val config = new SparkConf().setAppName("Graph Processing Proj5")
      val cox = new SparkContext(config)
       
       var graph = cox.textFile(args(0)).map( line => { var a=line.split(",")
                                                        var elem=new ListBuffer[Long]()
                                                        for(i <- 1 to (a.length-1))
                                                          {elem+=a(i).toLong}
                                                        var adj=elem.toList
                                                        (a(0).toLong,a(0).toLong,adj)
     })  
        var original_graph=graph.map(f => (f._1,f))
        for(i <- 1 to 5){
           graph=graph.flatMap( f=>{
            var newEle=new ListBuffer[(Long,Long)]()
            newEle+=((f._1,f._2))
            for(i<-0 to (f._3.length-1)){newEle+=((f._3(i),f._2))}
            var newAdj=newEle.toList
            (newAdj)
           }
           )
          .reduceByKey((t1, t2) => (if (t1 >= t2) t2 else t1)).join(original_graph).map(f => (f._2._2._2, f._2._1, f._2._2._3))
       }
       val groupCount = graph.map(f => (f._2, 1)).reduceByKey((x, y) => (x + y)).sort(x).collect().foreach(println)
     }
  }