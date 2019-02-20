# For Python examples, use spark-submit directly:
#spark-submit path_to_python_file/python_file.py

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from numpy import array, product
######################################################

topicsNumber=9

mainPath = "file:////home/cloudera/BigDataCourseProject/baby/";

intermediatePath = mainPath+"intermediate/";

undirected_comp_sub_json_file = intermediatePath + "undirected_relation_type_json.json";
directed_comp_sub_json_file = intermediatePath + "directed_relation_type_json.json";
	
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
	
def getIds_likelyhood(point):
	point_dict=point.asDict()
	values = [float(point_dict.get("f_"+str(x),0)) for x in range(topicsNumber)]
	return (point_dict.get("asin_1",''),point_dict.get("asin_2",''),product(values))


######################################################
sc = SparkContext(appName="Complementary and Supplementary Logistic Regression")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

############################# Undirected #########################
### Prepare Undirected Data

undirected_comp_sub_df = sqlContext.read.load(undirected_comp_sub_json_file, format="json").limit(100)
undirectedParsedData = undirected_comp_sub_df.map(lambda point: parsePoint(point))

### Build ModeL
undirectedModel = LogisticRegressionWithSGD.train(undirectedParsedData)
undirectedModel.clearThreshold()
undirectedModel.save(sc,mainPath+"comp_sub_undireced_model")
undirectedLabelsAndPreds = undirectedParsedData.map(lambda point: (point.label,undirectedModel.predict(point.features)))
undirectedLabelsAndPredsIndexed=undirectedLabelsAndPreds.zipWithIndex().map(lambda (x,y): (y,x));

undirectedLabelsAndPreds.unpersist()
undirectedParsedData.unpersist()

#extract productsIds 
productsIds = undirected_comp_sub_df.map(lambda point: getIds_likelyhood(point))
productsIdsIndexed = productsIds.zipWithIndex().map(lambda (x,y): (y,x));

productsIds_undirectedLabelsAndPredsIndexed = productsIdsIndexed.join(undirectedLabelsAndPredsIndexed)

productsIdsIndexed.unpersist()
undirectedLabelsAndPredsIndexed.unpersist()

######################################################
### Prepare Directed Data

directed_comp_sub_df = sqlContext.read.load(directed_comp_sub_json_file, format="json").limit(100)
directedParsedData = directed_comp_sub_df.map(lambda point: parseDirectedPoint(point))

######################################################

directedModel = LogisticRegressionWithSGD.train(directedParsedData)
directedModel.clearThreshold()
directedModel.save(sc,mainPath+"comp_sub_direced_model")

directedLabelsAndPreds = directedParsedData.map(lambda point: (point.label,directedModel.predict(point.features)))
directedLabelsAndPredsIndexed=directedLabelsAndPreds.zipWithIndex().map(lambda (x,y): (y,x));

#print("@@@@@@@@@@@@@@@@@@@@@ 0 @@@@@@@@@@@@@@@@@@@@@@")
#print((undirectedLabelsAndPreds.join(directedLabelsAndPreds)).take(15))
#print("@@@@@@@@@@@@@@@@@@@@@ END0 @@@@@@@@@@@@@@@@@@@@@@")

######################################################
### Prepare Data for Graph
graphCompSupData = productsIds_undirectedLabelsAndPredsIndexed.join(directedLabelsAndPredsIndexed)

print("@@@@@@@@@@@@@@@@@@@@@ 1 @@@@@@@@@@@@@@@@@@@@@@")
print(graphCompSupData.take(20))
## (idx, (((id1, id2,likeliHood), (lbl1, prd1)), (lbl2, prd2)))
print("@@@@@@@@@@@@@@@@@@@@@@ END1 @@@@@@@@@@@@@@@@@@@@@")

trainThreshold = 0.2

def getCompSub(prd1,prd2) :
	return 1 if (float(prd1)*float(prd2))> trainThreshold else 0
	

graphCompSupData = graphCompSupData.map(lambda (idx, (((id1, id2,likeliHood), (lbl1, prd1)), (lbl2, prd2))):(id1,id2, getCompSub(prd1,prd2) ,lbl1))

print("@@@@@@@@@@@@@@@@@@@@@ 1 @@@@@@@@@@@@@@@@@@@@@@")
for i in graphCompSupData.filter(lambda (id1,id2,pred,lbl): 0 ==  lbl ).take(20):
	print(i)
## (idx, (((id1, id2), (lbl1, prd1)), (lbl2, prd2)))
print("@@@@@@@@@@@@@@@@@@@@@@ END1 @@@@@@@@@@@@@@@@@@@@@")

######################################################
### Calculate Model Accuracy

testing=graphCompSupData.map(lambda (id1,id2,pred,lbl): (id1,id2,pred,lbl, pred ==  lbl) )

print("@@@@@@@@@@@@@@@@@@@@@ 1 @@@@@@@@@@@@@@@@@@@@@@")
for i in testing.take(100):
	print(i)
## (idx, (((id1, id2), (lbl1, prd1)), (lbl2, prd2)))
print("@@@@@@@@@@@@@@@@@@@@@@ END1 @@@@@@@@@@@@@@@@@@@@@")

trainErr = testing.filter(lambda (id1,id2,pred,lbl,success): not success ).count()

trainErr = float(trainErr) / float(graphCompSupData.count())
print("@@@@@@@@@@@@@@@@@@@@@ 2 @@@@@@@@@@@@@@@@@@@@@@")
print("Training Error = " + str(trainErr*100)+"%")
print("@@@@@@@@@@@@@@@@@@@@@@ END2 @@@@@@@@@@@@@@@@@@@@@")

######################################################
### Write Graph Data on Hadoop
final_graphCompSupData=graphCompSupData.map(lambda (id1,id2,pred,lbl):(id1,id2,pred))
final_graphCompSupData.saveAsTextFile(intermediatePath+"graph_compsub")
print("@@@@@@@@@@@@@@@@@@@@@ 3 @@@@@@@@@@@@@@@@@@@@@@")
print(final_graphCompSupData.take(5))
print("@@@@@@@@@@@@@@@@@@@@@@ END3 @@@@@@@@@@@@@@@@@@@@@")

######################################################
sc.stop()
