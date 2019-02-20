from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from numpy import array
######################################################
topicsNumber=9

mainPath = "file:////home/cloudera/BigDataCourseProject/baby/";

intermediatePath = mainPath+"intermediate/";

undirected_relation_json_file = intermediatePath + "undirected_relation_json.json";
directed_relation_json_file = intermediatePath + "directed_relation_json.json";

	
def parsePoint(point):
	point_dict=point.asDict()
	values = [float(point_dict.get("f_"+str(x),0)) for x in range(topicsNumber)]
	label = float(point_dict.get('relation',0))
	return LabeledPoint(label,values)

def parseDirectedPoint(point):
	point_dict=point.asDict()
	# 2 ids, topic features, 2 -> price, brand, label
	values = [float(point_dict.get("f_"+str(x),0)) for x in range(topicsNumber)]
	values.extend([float(point_dict.get("brand",0)),float(point_dict.get('price',0))])
	label = float(point_dict.get('relation',0))
	return LabeledPoint(label,values)
	
def getIds(point):
	point_dict=point.asDict()
	return (point_dict.get("asin_1",''),point_dict.get("asin_2",''))
	
######################################################
sc = SparkContext(appName="Relation Logistic Regression")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

### Prepare Undirected Data

undirected_relation_df = sqlContext.read.load(undirected_relation_json_file, format="json")
#.limit(100)
undirectedParsedData = undirected_relation_df.map(lambda point: parsePoint(point))

print("@@@@@@@@@@@@@@@@@@@@@ 0 @@@@@@@@@@@@@@@@@@@@@@")
print("undirectedParsedData == "+str(undirectedParsedData.take(10)))
print("@@@@@@@@@@@@@@@@@@@@@ END0 @@@@@@@@@@@@@@@@@@@@@@")


### Build ModeL undirected
undirectedModel = LogisticRegressionWithSGD.train(undirectedParsedData)
undirectedModel.clearThreshold()
undirectedModel.save(sc,mainPath+"undireced_relation_model")
undirectedLabelsAndPreds = undirectedParsedData.map(lambda point: (point.label,float(undirectedModel.predict(point.features))))
undirectedLabelsAndPredsIndexed = undirectedLabelsAndPreds.zipWithIndex().map(lambda (x,y): (y,x));

print("@@@@@@@@@@@@@@@@@@@@@ 0 @@@@@@@@@@@@@@@@@@@@@@")
print("undirectedLabelsAndPredsIndexed == "+str(undirectedLabelsAndPredsIndexed.take(10)))
print("@@@@@@@@@@@@@@@@@@@@@ END0 @@@@@@@@@@@@@@@@@@@@@@")

######################################################
#join with productsIds 
productsIds = undirected_relation_df.map(lambda point: getIds(point))
productsIdsIndexed = productsIds.zipWithIndex().map(lambda (x,y): (y,x));

print("@@@@@@@@@@@@@@@@@@@@@ 3 @@@@@@@@@@@@@@@@@@@@@@")
print("productsIds == "+str(productsIds.take(10)))
print("@@@@@@@@@@@@@@@@@@@@@ END3 @@@@@@@@@@@@@@@@@@@@@@")

productsIds_undirectedLabelsAndPredsIndexed = productsIdsIndexed.join(undirectedLabelsAndPredsIndexed)
productsIdsIndexed.unpersist()
undirectedLabelsAndPredsIndexed.unpersist()
undirectedLabelsAndPreds.unpersist()
######################################################
### Prepare Directed Data

directed_relation_df = sqlContext.read.load(directed_relation_json_file, format="json")
#.limit(100)
directedParsedData = directed_relation_df.map(lambda point: parseDirectedPoint(point))

print("@@@@@@@@@@@@@@@@@@@@@ 0 @@@@@@@@@@@@@@@@@@@@@@")
print("directedParsedData   == "+str(directedParsedData.take(10)))
print("@@@@@@@@@@@@@@@@@@@@@ END0 @@@@@@@@@@@@@@@@@@@@@@")
######################################################
### Build ModeL directed

directedModel = LogisticRegressionWithSGD.train(directedParsedData)
directedModel.clearThreshold()
directedModel.save(sc,mainPath+"direced_relation_model")
directedLabelsAndPreds = directedParsedData.map(lambda point: (point.label,float(directedModel.predict(point.features))))
directedLabelsAndPredsIndexed = directedLabelsAndPreds.zipWithIndex().map(lambda (x,y): (y,x));

print("@@@@@@@@@@@@@@@@@@@@@ 0 @@@@@@@@@@@@@@@@@@@@@@")
print("directedLabelsAndPredsIndexed   == "+str(directedLabelsAndPredsIndexed.take(10)))
print("@@@@@@@@@@@@@@@@@@@@@ END0 @@@@@@@@@@@@@@@@@@@@@@")


graphRelationData = productsIds_undirectedLabelsAndPredsIndexed.join(directedLabelsAndPredsIndexed)

productsIds_undirectedLabelsAndPredsIndexed.unpersist();
directedLabelsAndPredsIndexed.unpersist();
directedLabelsAndPreds.unpersist();

######################################################
### Prepare Data for Graph
print("@@@@@@@@@@@@@@@@@@@@@ 4 @@@@@@@@@@@@@@@@@@@@@@")
print(graphRelationData.take(10))
#(idx, (((id1, id2), (lbl1, prd1)), (lbl2, prd2)))
print("@@@@@@@@@@@@@@@@@@@@@@ END4 @@@@@@@@@@@@@@@@@@@@@")
#graphRelationData = graphRelationData.map(lambda (((id1, id2), (lbl1, prd1)), (lbl2, prd2)):(id1,id2,float(prd1)*(1-float(prd1))*float(prd2)*(1-float(prd2))))
#  ==> (id1,id2,pred,lbl)
graphRelationData = graphRelationData.map(lambda (idx, (((id1, id2), (lbl1, prd1)), (lbl2, prd2))):(id1,id2,float(prd1)*float(prd2),lbl1))

print("@@@@@@@@@@@@@@@@@@@@@ 1 @@@@@@@@@@@@@@@@@@@@@@")
for i in graphRelationData.filter(lambda (id1,id2,pred,lbl): 0 ==  lbl ).take(20):
	print(i)
## (idx, (((id1, id2), (lbl1, prd1)), (lbl2, prd2)))
print("@@@@@@@@@@@@@@@@@@@@@@ END1 @@@@@@@@@@@@@@@@@@@@@")

######################################################
### Calculate Model Accuracy
trainThreshold = 0.2
testing=   graphRelationData.map(lambda (id1,id2,pred,lbl): (id1,id2,pred,lbl,(1 if float(pred)>float(trainThreshold) else 0) ==  lbl) )

print("@@@@@@@@@@@@@@@@@@@@@ 1 @@@@@@@@@@@@@@@@@@@@@@")
for i in testing.take(100):
	print(i)
## (idx, (((id1, id2), (lbl1, prd1)), (lbl2, prd2)))
print("@@@@@@@@@@@@@@@@@@@@@@ END1 @@@@@@@@@@@@@@@@@@@@@")

trainErr = testing.filter(lambda (id1,id2,pred,lbl,success): not success).count()

trainErr = float(trainErr) / float(graphRelationData.count())

print("@@@@@@@@@@@@@@@@@@@@@ 5 @@@@@@@@@@@@@@@@@@@@@@")
print("Training Error = " + str(trainErr*100)+"%")
print("@@@@@@@@@@@@@@@@@@@@@@ END5 @@@@@@@@@@@@@@@@@@@@@")

######################################################
### Write Graph Data on Hadoop
graphRelationData.filter(lambda (id1,id2,pred,lbl): float(pred)>float(trainThreshold) ).map(lambda (id1,id2,pred,lbl):(id1,id2,pred)).saveAsTextFile(intermediatePath+"graph_relation")
print("@@@@@@@@@@@@@@@@@@@@@ 6 @@@@@@@@@@@@@@@@@@@@@@")
print(graphRelationData.take(5))
print("@@@@@@@@@@@@@@@@@@@@@@ END6 @@@@@@@@@@@@@@@@@@@@@")

######################################################
sc.stop()
