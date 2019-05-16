import org.apache.spark.graphx.{Graph=>Graph1, VertexId,Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// Cloud Computing: Project 8
// Rishabh Agrawal

object Graph {
  def main ( args: Array[String] ){
    val conf=new SparkConf().setAppName("Connected Graph")

    val sc = new SparkContext(conf)
   
    val edges :RDD[Edge[Long]]=sc.textFile(args(0)).map(line => { val (node,neighbours)=line.split(",").splitAt(1)
                                         (node(0).toLong,neighbours.toList.map(_.toLong))})
                                         .flatMap(x=> x._2.map(y=>(x._1,y)))
                                         .map(nodes=>Edge(nodes._1,nodes._2,nodes._1))
                                       
    val graph: Graph1[Long,Long]=Graph1.fromEdges(edges,"defaultProperty").mapVertices((id,_)=>id)
    
    val cc=graph.pregel(Long.MaxValue,5)(
        (id,oldGrp,newGrp)=> math.min(oldGrp,newGrp),
        triplet=>{
            if(triplet.attr<triplet.dstAttr){
              Iterator((triplet.dstId,triplet.attr))
            }else if((triplet.srcAttr<triplet.attr)){
              Iterator((triplet.dstId,triplet.srcAttr))
            }else{
              Iterator.empty
            }
        }, (a,b)=>math.min(a,b)
        )

        val res =cc.vertices.map(graph=>(graph._2,1)).reduceByKey((m, n) => (m+n))
        .sortByKey().map(k=>k._1.toString+" "+k._2.toString)  
        
        println("Connected Components:");
        res.collect().foreach(println)
  }
}