#################################################################
#   installation Guide                                          #
#################################################################
# Data Preperations:
##################################
# 1- create floder "/home/cloudera/BigDataCourseProject/baby/"
#
# 2- copy the 3 files meta_Baby.json,qa_Baby.json,reviews_Baby_5.json to the folder 
#
# 3- create floder "/home/cloudera/BigDataCourseProject/baby/intermediate" for the intermediate created files
#
# 4- run the python file prepare _data_files_json.py	for data preparation
################################################################
# Model:
###################################
# For the LDA part
##################
# 1- Run the below command 
# spark-submit --class lda.LDAModule  --master local[2] /home/cloudera/BigDataCourseProject/FinalProject.jar
#
###################################
# For the logestic regression part
###################################
# 1- Run the below command
# spark-submit --master local[2] /home/cloudera/BigDataCourseProject/relation_logit.py
# 2- Run the below command 
# spark-submit --master local[2] /home/cloudera/BigDataCourseProject/compsup_logit.py

################################################################
#   GraphX installation Guide     #
################################################################
# 
# 1- this step is not manadtory it is only in case there is problem in the HDFS mode: in case HDFS is not in the # safemode run the below command 
#    hdfs dfsadmin -safemode leave
#
# 2 - run the scala shell
# spark-shell
#
# 3- compile the Graphx scala file 
# :load Graphx.scala
#
# 4 - compile the Search scala file
# :load Search.scala
#
# run any query you want
#
# Search.main(Array("B006DQ54L0"))
##