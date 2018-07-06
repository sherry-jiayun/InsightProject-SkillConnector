from __future__ import print_function
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
from neo4j.v1 import GraphDatabase
from pyspark.sql.functions import *
import psycopg2

sc = SparkContext(master="master ip:7077")
sqlContext = SQLContext(sc)

partitionNum = 18
url = 'xxxxxxxxxx'

def writeNode(p):
	# connect to neo4j
	uri = "xxxxxxxxxx:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","xxxxxxxxxx"))
	session = driver.session()
	# for node, (weight, count)
	cypher = "WITH ["
	for x in p:
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
	uri = "xxxxxxxxxx:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","xxxxxxxxxx"))
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

def writeUserNode(p):
	# connect to neo4j
	uri = "xxxxxxxxxx:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","xxxxxxxxxx"))
	session = driver.session()
	# for relationship, (weight, count)
	count = 0
	cypher = "WITH ["
	cypher_user = "WITH ["
	for x in p:
		user_tmp = x[0]
		rel_tmp = x[1]
		if not '(' in user_tmp[1] and not ')' in user_tmp[1]:
			cypher_user_tmp = '{'
			cypher_user_tmp += 'id:' + str(user_tmp[0]) + ','
			cypher_user_tmp += 'name:"' + user_tmp[1].encode('ascii','ignore') + '",'
			if user_tmp[2]:
				cypher_user_tmp += 'website:"' + user_tmp[2].encode('ascii','ignore') + '"},'
			else:
				cypher_user_tmp += 'website:""},'
			cypher_user += cypher_user_tmp
		else:
			tmp = user_tmp[1].encode('ascii','ignore').replace('(','').replace(')','').replace('"','').replace('"','').split(',')
			cypher_user_tmp = '{'
			cypher_user_tmp += 'id:' + str(user_tmp[0]) + ','
			cypher_user_tmp += 'name:"' + tmp[0] + '",'
			if tmp[1]:
				cypher_user_tmp += 'website:"' + tmp[1] + '"},'
			else:
				cypher_user_tmp += 'website:""},'
			cypher_user += cypher_user_tmp
	if cypher_user[-1]==',':
		cypher_user = cypher_user[:-1]
	cypher_user += "] as data"
	cypher_user += " UNWIND data as row"
	cypher_user += " MERGE (u:user {id:row.id})"
	cypher_user += " ON CREATE SET u.name = row.name, u.website = row.website"
	session.run(cypher_user)
	session.close()

def writeUserRelationship(p):
	# connect to neo4j
	uri = "xxxxxxxxxx:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","xxxxxxxxxx"))
	session = driver.session()
	# for relationship, (weight, count)
	count = 0
	cypher = "WITH ["
	for x in p:
		user_tmp = x[0]
		rel_tmp = x[1]
		for l in rel_tmp:
			cypher_rel_tmp = '{'
			cypher_rel_tmp += 'from:' + str(user_tmp[0]) +','
			cypher_rel_tmp += 'to:"' + l[0].encode('ascii','ignore') + '",'
			cypher_rel_tmp += 'weight:' + str(l[1]) + ','
			cypher_rel_tmp += 'count:' + str(l[2]) + '},'
			cypher += cypher_rel_tmp
		count += len(rel_tmp)
		if count >= 100:
			if cypher[-1] == ',':
				cypher = cypher[:-1]
			cypher += "] as data"
			cypher += " UNWIND data as row"
			cypher += " MATCH (u:user {id:row.from})"
			cypher += " MATCH (v:vertex {name:row.to})"
			cypher += " MERGE (u)-[r:Contribute]->(v)"
			cypher += " ON CREATE SET r.weight = row.weight,r.count = row.count"
			session.run(cypher)
			cypher = "WITH ["
			count = 0
	if cypher[-1] == ',':
		cypher = cypher[:-1]
	cypher += "] as data"
	cypher += " UNWIND data as row"
	cypher += " MATCH (u:user {id:row.from})"
	cypher += " MATCH (v:vertex {name:row.to})"
	cypher += " MERGE (u)-[r:Contribute]->(v)"
	cypher += " ON CREATE SET r.weight = row.weight,r.count = row.count"
	session.run(cypher)
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
	user = "xxxxxxxxxx",
	password = "xxxxxxxxxx").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',4).load()


# for technical relashionship
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
		user = "xxxxxxxxxx",
		password = "xxxxxxxxxx").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',4).load()
	OFFSET += LIMIT
	rdd2 = sc.parallelize(df_1.collect())
	rdd2_kv = rdd2.map(lambda x:(x[0],[(x[1],x[2],x[3])]))
	rdd2_group = rdd2_kv.aggregateByKey([],lambda n,v: (n+v),lambda v1,v2:(v1+v2))
	rdd2_group_limit = rdd2_group.map(lambda x:(x[0],sortAndLimit(x[1])))
	rdd2_final = rdd2_group_limit.collect()
	writeRelationship(rdd2_final)

# for user relationship
MAX_VALUE = 21188313
OFFSET = 0
LIMIT = 300000
count = 0 
while (OFFSET < MAX_VALUE):
	df = sqlContext.read.format('jdbc').options(
		url = url,
		driver = "org.postgresql.Driver",
		dbtable = '(SELECT * FROM USER_TECH OFFSET '+str(OFFSET)+' LIMIT '+str(LIMIT)+') tmp',
		user = "xxxxxxxxxx",
		password = "xxxxxxxxxx").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',18).load()
	OFFSET += LIMIT
	rdd2 = sc.parallelize(df.collect(),72)
	rdd3 = rdd2.map(lambda x:((x[0],x[1],x[2]),[(x[3],x[4],x[5])]))
	rdd4 = rdd3.aggregateByKey([],lambda n,v:(n+v),lambda v1,v2: (v1+v2))
	rdd4.foreachPartition(writeUserNode)
	rdd5 = rdd4.collect()
	writeUserRelationship(rdd5)


