from __future__ import print_function
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
from neo4j.v1 import GraphDatabase
from pyspark.sql.functions import *


# hardcode
sc = SparkContext(master="spark://10.0.0.7:7077")
sqlContext = SQLContext(sc)

# connect to neo4j
uri = "bolt://ec2-34-234-207-154.compute-1.amazonaws.com:7687"
driver = GraphDatabase.driver(uri,auth=("neo4j","yjy05050609"))
session = driver.session()

# get null null tags from 
df = sqlContext.read.format("jdbc").options(url="jdbc:mysql://sg-cli-test.cdq0uvoomk3h.us-east-1.rds.amazonaws.com:3306/dbo",driver = "com.mysql.jdbc.Driver",dbtable="(SELECT AnswerCount,CommentCount,FavoriteCount,Tags FROM Posts WHERE Tags IS NOT NULL LIMIT 10000) tmp",user="sherry_jiayun",password="yjy05050609").option('numPartitions',4).option('lowerBound',1).option('upperBound',100).option('partitionColumn',4).load()
# replace < > and for dataframe

df = df.withColumn('Tags', regexp_replace('Tags', '<', ' '))
df = df.withColumn('Tags', regexp_replace('Tags', '>', ' '))
df = df.withColumn('Tags', regexp_replace('Tags', '  ', ' '))
df = df.withColumn('total', df['AnswerCount']+df['CommentCount']+df['FavoriteCount'])

def testFunc(p):
	print ("Hello from rdd.")

'''def writeToNeo4j(p):
	uri = "bolt://ec2-34-234-207-154.compute-1.amazonaws.com:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","yjy05050609"))
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

rdd = sc.parallelize(df.collect())
rdd.foreachPartition(testFunc)
rdd_clean = rdd.map(lambda x:(x[0],x[1],x[2],x[3].replace('<',' ').replace('>',' ').replace('  ',' '),x[0]+x[1]+x[2]))

rdd_clean.foreachPartition(writeToNeo4j)
rdd_clean.foreachPartition(testFunc)'''

# create vertex and edge 
# MERGE (:vertex{ name: '"+node_name_1+"'})
# MERGE (:vertex{ name: '"+node_name_2+"'})
# create edge
# MATCH (v1:vertex {name:'"+node_name_1+"'}),(v2:vertex { name:'"+node_name_2+"'})
# MERGE (v1)-[r:Group { name:'"+node_name_1+'-'+node_name_2+"'}]->(v2)
# ON CREATE SET r.weight = 0 
# WITH r 
# SET r.weight = r.weight + str(new)
# check_list = list()
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
				#print (xx,xxx,x[4])
				session.run(cypher)
	
