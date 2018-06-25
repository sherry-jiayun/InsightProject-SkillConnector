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

def testFunc(p):
	print ("Hello from inner rdd.")

def writeNode(p):
	# connect to neo4j
	uri = "bolt://ec2-35-172-240-121.compute-1.amazonaws.com:7687"
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

def sortAndLimit(listtmp):
	listtmp = sorted(listtmp,key=lambda x:x[1],reverse=True)
	if len(listtmp) > 25:
		listtmp = listtmp[:25]
	return listtmp

def writeRelationship(p):
	# connect to neo4j
	uri = "bolt://ec2-35-172-240-121.compute-1.amazonaws.com:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","yjy05050609"))
	session = driver.session()
	# for relationship, (weight, count)
	count = 0
	cypher = "WITH ["
	for x in p:
		xx = x[0]
		listtmp = x[1]
		for l in listtmp:
			xxx = l[0]
			cypher_tmp = '{'
			cypher_tmp += 'from:"'+xx+'",'
			cypher_tmp += 'to:"'+xxx+'",'
			cypher_tmp += 'name:"'+xx+'-'+xxx+'",'
			cypher_tmp += 'weight:'+str(l[1])+','
			cypher_tmp += 'count:'+str(l[2])+'},'
			cypher += cypher_tmp
		count += len(listtmp)
		if count >= 100:
			if cypher[-1] == ',':
				cypher = cypher[:-1]
			cypher += "] as data"
			cypher += " UNWIND data as row"
			cypher += " MATCH (v1:vertex {name:row.from})"
			cypher += " MATCH (v2:vertex {name:row.to})"
			cypher += " MERGE (v1)-[r:Group]->(v2)"
			cypher += " ON CREATE SET r.name = row.name,r.weight = row.weight,r.count = row.count"
			session.run(cypher)
			cypher = "WITH ["
			count = 0
	session.close()

def helpFunction(p):
	for x in p:
		print (x[0],x[1])

rdd = sc.parallelize(df_node.collect())
rdd.foreachPartition(writeNode)

df_MAX = sqlContext.read.format("jdbc").options(
	url=url,
	driver = "org.postgresql.Driver",
	dbtable = '(SELECT count(*) as count_num FROM TECH_REL) tmp',
	user = "sherry_jiayun",
	password = "yjy05050609").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',4).load()

MAX_VALUE = df_MAX.collect()
MAX_VALUE = MAX_VALUE[0]['count_num']

OFFSET = 0 
LIMIT = 300000
count = 0
while (OFFSET < MAX_VALUE):
	df_1 = sqlContext.read.format("jdbc").options(
		url=url,
		driver = "org.postgresql.Driver",
		dbtable = '(SELECT * FROM TECH_REL OFFSET '+str(OFFSET)+' LIMIT '+str(LIMIT)+') tmp',
		user = "sherry_jiayun",
		password = "yjy05050609").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',4).load()
	OFFSET += LIMIT
	rdd2 = sc.parallelize(df_1.collect())
	rdd2_kv = rdd2.map(lambda x:(x[0],[(x[1],x[2],x[3])]))
	rdd2_group = rdd2_kv.aggregateByKey([],lambda n,v: (n+v),lambda v1,v2:(v1+v2))
	rdd2_group_limit = rdd2_group.map(lambda x:(x[0],sortAndLimit(x[1])))
	rdd2_final = rdd2_group_limit.collect()
	writeRelationship(rdd2_final)
# foreachPartition(writeRelationship)


