import org.apache.spark._
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.MurmurHash
import scala.util.Sorting

object Search {
  def main(args: Array[String]) {


val typeOfReleations = graph.subgraph(epred = e => e.srcId == MurmurHash.stringHash(args(0)))



val sorted = typeOfReleations.triplets.collect.sortWith(_.attr.asInstanceOf[EdgeAttribute].x > _.attr.asInstanceOf[EdgeAttribute].x)

sorted.foreach(println(_))

for (tr <- sorted){

var typeOFRelation = ""

if (tr.attr.asInstanceOf[EdgeAttribute].y.equals(null)
 || tr.attr.asInstanceOf[EdgeAttribute].y == 0.0 )
{
	typeOFRelation = "Type of relation is N/A"
}
else if (tr.attr.asInstanceOf[EdgeAttribute].y  == 0) 
{
	typeOFRelation = "complementry"
}
else
{
	typeOFRelation = "supplementry"
}

var message = "The product: '" + tr.srcAttr + "' is '"+ typeOFRelation +"' to product " + tr.dstAttr + " the relation strong by "  + tr.attr.asInstanceOf[EdgeAttribute].x + " and secound wight "+ tr.attr.asInstanceOf[EdgeAttribute].y

println( message)

}


if(sorted.length ==0)
{
 println("There is no related product to: " + args(0))
}

	
  }
}
