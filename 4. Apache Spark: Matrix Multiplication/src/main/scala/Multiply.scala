import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {

    val conf = new SparkConf().setAppName("Matrix_Multiplication")
    val context = new SparkContext(conf)
    val matM = context.textFile(args(0)).map( line => { val a = line.split(",")
                                                (a(0).toInt,a(1).toInt,a(2).toDouble) } )
    val matN = context.textFile(args(1)).map( line => { val a = line.split(",")
                                                (a(0).toInt,a(1).toInt,a(2).toDouble) } )
    val intermediate = matM.map( matM => (matM._2,(0,matM._1,matM._3)) ).join(matN.map( matN => (matN._1,(1,matN._2,matN._3)) ))
                .map { case (k,(matM,matN)) => matM._2+","+matN._2+","+matM._3*matN._3 }
    val result = intermediate.map( line => { val a = line.split(",")
                                             (a(0).toInt,a(1).toInt,a(2).toDouble) } ).
                                             map( res => ((res._1,res._2),res._3) ).
                                             reduceByKey((x,y) => x+y ).sortByKey().map { case (x,y) => x._1+","+x._2+","+y}
    result.collect().foreach(println)  
    result.saveAsTextFile(args(2))
    context.stop()
  }
}