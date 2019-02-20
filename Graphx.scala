
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.MurmurHash
import org.apache.spark.storage.StorageLevel

class EdgeAttribute (xc: Double, yc: Double) extends Serializable {
   var x: Double = xc
   var y: Double = yc
}	

val mainPath = "file:////home/cloudera/BigDataCourseProject/baby/";

val intermediatePath = mainPath+"intermediate/";



val relationFile = sc.textFile(intermediatePath+"graph_relation")
val typeFile = sc.textFile(intermediatePath+"graph_compsub")

val relation_list =relationFile.map(line =>line.substring(1,line.length()-1).split(','))
//.filter(x =>  x(2).toDouble >= 0.7)
val type_list =typeFile.map(line =>line.substring(1,line.length()-1).split(','))

val l1 = relation_list.map(x=>((x(0).trim(),x(1).trim()),x(2).trim()))
val l2 = type_list.map(x=>((x(0).trim(),x(1).trim()),x(2).trim()))

print(l1.count())
print(l2.count())

val l3 = l1.join(l2)							// ((id1,id2),(relation,type))

val l4 =  l3.map(x=> Array(x._1._1.replace("u'","").replace("'",""), x._1._2.replace("u'","").replace("'",""), x._2._1,x._2._2))	// (id1,id2,relation,type)

 
val v1 = l4.flatMap(x => Iterable(x(0).toString, x(1).toString))

val products: RDD[(VertexId, (String))] = v1.distinct().map(x => (MurmurHash.stringHash(x), x))

val relationships: RDD[Edge[EdgeAttribute]] =
l4.map(x =>
    ((MurmurHash.stringHash(x(0).toString),MurmurHash.stringHash(x(1).toString)), new EdgeAttribute(x(2).toDouble, x(3).toDouble))).map(x => Edge(x._1._1, x._1._2,x._2.asInstanceOf[EdgeAttribute]))

val rootProduct = ("A000")
val graph = Graph(products, relationships, rootProduct,  StorageLevel.DISK_ONLY)