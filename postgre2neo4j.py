from __future__ import print_function
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
from neo4j.v1 import GraphDatabase
from pyspark.sql.functions import *
import psycopg2

sc = SparkContext(master="spark://10.0.0.7:7077")
sqlContext = SQLContext(sc)

partitionNum = 18
url = 'jdbc:postgresql://time-key-db.cdq0uvoomk3h.us-east-1.rds.amazonaws.com:5432/InsightDB'
df_node = sqlContext.read.format("jdbc").options(
	url=url,
	driver = "org.postgresql.Driver",
	dbtable = 'TECH_NODE',
	user = "sherry_jiayun",
	password = "yjy05050609").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',3).load()

df_relationship = sqlContext.read.format("jdbc").options(
	url=url,
	driver = "org.postgresql.Driver",
	dbtable = 'TECH_REL',
	user = "sherry_jiayun",
	password = "yjy05050609").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',4).load()

def testFunc(p):
	print ("Hello from inner rdd.")

def writeNode(p):
	# connect to neo4j
	uri = "bolt://ec2-34-236-245-247.compute-1.amazonaws.com:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","yjy05050609"))
	session = driver.session()
	# for node, (weight, count)
	cypher = "WITH ["
	for x in p:
		# print(x)
		cypher_tmp = '{'
		cypher_tmp += 'name:"'+x[0]+'",'
		cypher_tmp += 'weight:' + str(x[1]) + ','
		cypher_tmp += 'count:' + str(x[2]) + '},'
		cypher += cypher_tmp
	if cypher[-1] == ',':
		cypher = cypher[:-1]
	cypher += "] as data"
	cypher += " UNWIND data as row"
	cypher += " MERGE (n:vertex {name:row.name})"
	cypher += " ON CREATE SET n.weight = 0,n.count = 0"
	cypher += " SET n.weight = n.weight + row.weight,n.count = n.count + row.count"
	# print (cypher)
	session.run(cypher)
	session.close()

def writeRelationship(p):
	# connect to neo4j
	uri = "bolt://ec2-34-236-245-247.compute-1.amazonaws.com:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","yjy05050609"))
	session = driver.session()
	# for relationship, (weight, count)
	cypher = "WITH ["
	for x in p:
		[xx,xxx] = removeKeyForRelationship(x[0])
		cypher_tmp = '{'
		cypher_tmp += 'from:"'+xx+'",'
		cypher_tmp += 'to:"'+xxx+'",'
		cypher_tmp += 'name:"'+xx+'-'+xxx+'",'
		cypher_tmp += 'weight:'+str(x[1][0])+','
		cypher_tmp += 'count:'+str(x[1][1])+'},'
		cypher += cypher_tmp
	if cypher[-1] == ',':
		cypher = cypher[:-1]
	cypher += "] as data"
	cypher += " UNWIND data as row"
	cypher += " MATCH (v1:vertex {name:row.from})"
	cypher += " MATCH (v2:vertex {name:row.to})"
	cypher += " MERGE (v1)-[r:Group]->(v2)"
	cypher += " ON CREATE SET r.name = row.name,r.weight = 0,weight = 0,r.count = 0"
	cypher += " SET r.weight = r.weight + row.weight,r.count = r.count + row.count"
	# loop batch job
	flag = True
	while (flag):
		try:
			session.run(cypher)
			session.close()
			break
		except:
			time.sleep(1)
	session.close()

rdd = sc.parallelize(df_node.collect())
rdd.foreachPartition(writeNode)

rdd2 = sc.parallelize(df_relationship.collect())
rdd2.


