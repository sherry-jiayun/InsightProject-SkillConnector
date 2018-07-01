from __future__ import print_function
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
from neo4j.v1 import GraphDatabase
from pyspark.sql.functions import *


# hardcode
sc = SparkContext(master="xxxxxxxx")
sqlContext = SQLContext(sc)

# get null null tags from 
df = sqlContext.read.format("jdbc").options(url="jdbc:mysql://xxxxxxxx",driver = "com.mysql.jdbc.Driver",dbtable="(SELECT AnswerCount,CommentCount,FavoriteCount,Tags FROM Posts WHERE Tags IS NOT NULL LIMIT 10000) tmp",user="xxxxxxxx",password="xxxxxxxx").option('numPartitions',4).option('lowerBound',1).option('upperBound',100).option('partitionColumn',4).load()
# replace < > and for dataframe

df = df.withColumn('Tags', regexp_replace('Tags', '<', ' '))
df = df.withColumn('Tags', regexp_replace('Tags', '>', ' '))
df = df.withColumn('Tags', regexp_replace('Tags', '  ', ' '))
df = df.withColumn('total', df['AnswerCount']+df['CommentCount']+df['FavoriteCount'])

def testFunc(p):
	print ("Hello from rdd.")

def writeToNeo4j(p):
	uri = "bolt://xxxxxxxx"
	driver = GraphDatabase.driver(uri,auth=("neo4j","xxxxxxxx"))
	session = driver.session()
	for x in p:
		vertex_list = x[3].strip().split(' ')
		for xx in vertex_list:
			for xxx in vertex_list:
				if not xxx == xx:
					cypher = ""
					cypher += "MERGE (:vertex{ name: '"+xx+"' }) " # create node 1 if not exist
					cypher += "MERGE (:vertex{ name: '"+xxx+"'}) " # create node 2 if not exist
					print (cypher)
					session.run(cypher)
					cypher = ""
					cypher += "MATCH (v1:vertex { name:'"+xx+"' }), (v2:vertex { name:'"+xxx+"'}) "
					cypher += "MERGE (v1)-[r:Group { name:'"+xx+'-'+xxx+"'}]->(v2) " # create relationship
					cypher += "ON CREATE SET r.weight = 0 " # initialize weight
					cypher += "WITH r " # update relationship
					cypher += "SET r.weight = r.weight + "+str(x[4])
					#print (xx,xxx,x[4])
					session.run(cypher)
	session.close()

df.rdd.foreachPartition(writeToNeo4j)