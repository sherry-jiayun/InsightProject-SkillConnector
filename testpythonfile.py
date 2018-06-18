from __future__ import print_function
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
from neo4j.v1 import GraphDatabase
from pyspark.sql.functions import *

# hardcode
sc = SparkContext(master="spark://10.0.0.13:7077")
sqlContext = SQLContext(sc)

# connect to neo4j
uri = "bolt://ec2-34-234-207-154.compute-1.amazonaws.com:7687"
driver = GraphDatabase.driver(uri,auth=("neo4j","yjy05050609"))
session = driver.session()

# get null null tags from 
df = sqlContext.read.format("jdbc").options(url="jdbc:mysql://sg-cli-test.cdq0uvoomk3h.us-east-1.rds.amazonaws.com:3306/dbo",driver = "com.mysql.jdbc.Driver",dbtable="(SELECT AnswerCount,CommentCount,FavoriteCount,Tags FROM Posts WHERE Tags IS NOT NULL LIMIT 10000) tmp",user="sherry_jiayun",password="yjy05050609").option('numPartitions',4).option('lowerBound',1).option('upperBound',1000).option('partitionColumn',4).load()
# replace < > and.  
df = df.withColumn('Tags', regexp_replace('Tags', '<', ' '))
df = df.withColumn('Tags', regexp_replace('Tags', '>', ' '))
df = df.withColumn('Tags', regexp_replace('Tags', '  ', ' '))

# calculate weight
# 
df = df.withColumn('total', df['AnswerCount']+df['CommentCount']+df['FavoriteCount'])

def get_v(xx):
	xx_v = ''
	for xc in xx:
		if xc.isalpha():
			xx_v += xc
		else:
			xx_v += '_'
	return xx_v
df.foreach(print)

# create vertex and edge 
# MERGE (:vertex{ name: '"+node_name_1+"'})
# MERGE (:vertex{ name: '"+node_name_2+"'})
# create edge
# MATCH (v1:vertex {name:'"+node_name_1+"'}),(v2:vertex { name:'"+node_name_2+"'})
# MERGE (v1)-[r:Group { name:'"+node_name_1+'-'+node_name_2+"'}]->(v2)
# ON CREATE SET r.weight = 0 
# WITH r 
# SET r.weight = r.weight + str(new)
print("++++++++++++++start to do insert++++++++++++++")
'''check_list = list()
for x in df.collect():
	vertex_list = x[3].strip().split(' ')
	for xx in vertex_list:
		for xxx in vertex_list:
			if not xxx == xx:
				cypher = ""
				cypher += "MERGE (:vertex{ name: '"+xx+"' }) " # create node 1 if not exist
				cypher += "MERGE (:vertex{ name: '"+xxx+"'}) " # create node 2 if not exist
				session.run(cypher)
				cypher = ""
				cypher += "MATCH (v1:vertex { name:'"+xx+"' }), (v2:vertex { name:'"+xxx+"'}) "
				cypher += "MERGE (v1)-[r:Group { name:'"+xx+'-'+xxx+"'}]->(v2) " # create relationship
				cypher += "ON CREATE SET r.weight = 0 " # initialize weight
				cypher += "WITH r " # update relationship
				cypher += "SET r.weight = r.weight + "+str(x[4])
				print (xx,xxx,x[4])
				session.run(cypher)'''
	
