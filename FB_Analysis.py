import ibmos2spark
import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType 
# @hidden_cell
credentials = {
    'endpoint': 'https://s3-api.us-geo.objectstorage.service.networklayer.com',
    'service_id': 'iam-ServiceId-483acb8d-7120-4eae-a2be-d4f9e0af1b6d',
    'iam_service_endpoint': 'https://iam.cloud.ibm.com/oidc/token',
    'api_key': 'jb7eGpeGsDq4s_rDX_cymC3745z3_3wbACksaxigtwcv'
}

configuration_name = 'os_592857e215ab4400b1f0cfeeddc12484_configs'
cos = ibmos2spark.CloudObjectStorage(sc, credentials, configuration_name, 'bluemix_cos')

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
#Testing 
df = spark.read\
  .format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat')\
  .option('header', 'true')\
  .load(cos.url('Fb_testdata_model.csv', 'trinityfb-donotdelete-pr-p7zcpkgojehbsq'))

df.show()


casted_df =df.withColumn("Tenure",F.col('Tenure').cast(IntegerType()))\
                .withColumn("Age",F.col('Age').cast(IntegerType()))\
                .withColumn("Friends",F.col('Friends').cast(IntegerType()))\
                .withColumn("Views",F.col('Views').cast(IntegerType()))\
                .withColumn("Likes",F.col('Likes').cast(IntegerType()))
casted_df.printSchema()



#input_df.show()

#Age Score UDF
def age_score(age):
    if 0<=age<=21:
        return 7
    if 22<=age<=24:
        return 8
    if 25<=age<=26:
        return 10
    if 27<=age<=29:
        return 9
    if 30<=age<=32:
        return 6
    if 33<=age<=34:
        return 5
    if 35<=age<=37:
        return 4
    if 38<=age<=45:
        return 3
    if 46<=age<=49:
        return 2
    if age>=50:
        return 1
#network_connection_Score  
def friends_score(x):
    if 0<=x<=20:
        return 1
    if 21<=x<=100:
        return 2
    if 101<=x<=140:
        return 3
    if 141<=x<=200:
        return 4
    if 201<=x<=400:
        return 5
    if 401<=x<=500:
        return 6
    if 501<=x<=1000:
        return 7
    if 1001<=x<=2000:
        return 8
    if 2001<=x<=5000:
        return 9
    if x>=5000:
        return 10
#Likes_Score
def like_view_score(x):
    if 0<=x<=1000:
        return 1
    if 1001<=x<=5000:
        return 2
    if 5001<=x<=10000:
        return 3
    if 10001<=x<=30000:
        return 4
    if 30001<=x<=50000:
        return 5
    if 50001<=x<=80000:
        return 6
    if 80001<=x<=100000:
        return 7
    if 100001<=x<=150000:
        return 8
    if 150001<=x<=200000:
        return 9
    if x>=200000:
        return 10

#Age Score UDF
def tenure_score(age):
    if 0<=age<=21:
        return 1
    if 22<=age<=24:
        return 2
    if 25<=age<=26:
        return 3
    if 27<=age<=29:
        return 4
    if 30<=age<=32:
        return 5
    if 33<=age<=34:
        return 6
    if 35<=age<=37:
        return 7
    if 38<=age<=45:
        return 8
    if 46<=age<=49:
        return 9
    if age>=50:
        return 10






spark.udf.register("age_py",age_score)
spark.udf.register("friends_py",friends_score)
spark.udf.register("like_view_py",like_view_score)
spark.udf.register("tenure_py",tenure_score)



score_df=casted_df.select(F.col('UserId'),F.col('Name'),F.col('Gender'),F.col('Age'),F.col('City'),F.col('Friends'),F.col('Likes'),F.col('Views'),F.col('Tenure'),F.expr("age_py(Age)").alias('age_score')\
                           ,F.expr("tenure_py(Tenure)").alias('tenure_score')\
                           ,F.expr("like_view_py(Likes)").alias('like_score')\
                           ,F.expr("like_view_py(Views)").alias('view_score')\
                           ,F.expr("friends_py(Friends)").alias('friends_score'))                    
final_df=score_df.select(F.col('*'))\
        .withColumn("FB_score",((F.col("age_score") + F.col("tenure_score") + F.col("like_score") + F.col("view_score") +F.col("friends_score"))/5*100).alias('overall_score'))

final_df.write\
  .format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat')\
  .option('header', 'true')\
  .save(cos.url('fb_scored_pythontest.csv', 'trinityfb-donotdelete-pr-p7zcpkgojehbsq'))