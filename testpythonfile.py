from __future__ import print_function
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
from neo4j.v1 import GraphDatabase
from pyspark.sql.functions import *


# hardcode
sc = SparkContext(master="spark://10.0.0.7:7077")
sqlContext = SQLContext(sc)

CURRENT_VALUE_LOW = 0
CURRENT_VALUE_UPPER = CURRENT_VALUE_LOW + 50000 # 50000 ROWS PER LOOP
df_MAX = sqlContext.read.format("jdbc").options(
	url = "jdbc:mysql://sg-cli-test.cdq0uvoomk3h.us-east-1.rds.amazonaws.com:3306/dbo",
	driver = "com.mysql.jdbc.Driver",
	dbtable = "(SELECT MAX(Id) FROM Posts) tmp",
	user = "sherry_jiayun",
	password = "yjy05050609").load()
MAX_VALUE = df_MAX.collect()
MAX_VALUE = MAX_VALUE[0]['MAX(Id)'] # get max id value 
print(MAX_VALUE)

# get null null tags from 
df = sqlContext.read.format("jdbc").options(
 	url="jdbc:mysql://sg-cli-test.cdq0uvoomk3h.us-east-1.rds.amazonaws.com:3306/dbo",
 	driver = "com.mysql.jdbc.Driver",
 	dbtable="(SELECT AnswerCount,CommentCount,FavoriteCount,Tags, Id, CreationDate FROM Posts WHERE Id > 0 AND Id < 10000 AND Tags IS NOT NULL) tmp",
 	user="sherry_jiayun",
 	password="yjy05050609").option('numPartitions',4).option('lowerBound',1).option('upperBound',10000).option('partitionColumn',6).load()

# help test function
def testFunc(p):
	print ("Hello from inner rdd.")

def testPrintFunction(p):
	for x in p:
		print(x)

# combine node1+node2 
def combineKeyForRelationship(x):
	# for relationship
	return (x[0]+'|'+x[1])

def removeKeyForRelationship(x):
	# for node 
	return x.split('|')

# combine node1+date
def combineKeyForDate(x):
	# convert dateframe to string 
	dateStr = str(x[3]).split(' ')[0]
	return (x[0]+'|'+dateStr)

def removeKeyForDate(x):
	return x.split('|')[0],x.split('|')[1]

# combine node1+postid
def combineKey(x):
	return (x[0]+'|'+str(x[2]))
def removeKey(x):
	return x.split('|')[0]

def innerrdd(x):
	vertex_list = x[3].strip().split(' ')
	tmplist = list()
	for xx in vertex_list:
		for xxx in vertex_list:
			if not xxx == xx:
				# check whether already in the dict
				# x[4] id, x[5] weight
				# tmpitem[node1,node2,postid,time,weight]
				tmpitem = [xx,xxx,x[4],x[5],x[6]]
				tmplist.append(tmpitem)
	return tmplist

def writeNode(p):
	# connect to neo4j
	uri = "bolt://ec2-34-234-207-154.compute-1.amazonaws.com:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","yjy05050609"))
	session = driver.session()
	# for node, (weight, count)
	for x in p:
		cypher = ""
		cypher += "MERGE (v:vertex{ name: '"+ x[0] +"' }) "
		cypher += "ON CREATE SET v.weight = 0,v.count = 0 "
		cypher += "WITH v "
		cypher += "SET v.weight = v.weight + "+str(x[1][0]) + ","
		cypher += "v.count = v.count + "+ str(x[1][1])
		print (cypher)
		session.run(cypher)
	session.close()

def writeRelationship(p):
	# connect to neo4j
	uri = "bolt://ec2-34-234-207-154.compute-1.amazonaws.com:7687"
	driver = GraphDatabase.driver(uri,auth=("neo4j","yjy05050609"))
	session = driver.session()
	# for relationship, (weight, count)
	for x in p:
		[xx,xxx] = removeKeyForRelationship(x[0])
		cypher = ""
		cypher += "MATCH (v1:vertex { name:'"+xx+"' }), (v2:vertex { name:'"+xxx+"'}) "
		cypher += "MERGE (v1)-[r:Group { name:'"+xx+'-'+xxx+"'}]->(v2) " # create relationship
		cypher += "ON CREATE SET r.weight = 0,r.count = 0 " # initialize weight
		cypher += "WITH r " # update relationship
		cypher += "SET r.weight = r.weight + "+str(x[1][0]) +","
		cypher += "r.count = r.count + "+ str(x[1][0])
		print (cypher) 
		session.run(cypher)
	session.close()

def writeDate(p):
	# connect to postgresql
	postgre = "dbname=InsightDB user=sherry_jiayun password=yjy05050609 host=time-key-db.cdq0uvoomk3h.us-east-1.rds.amazonaws.com"
	conn = psycopg2.connect(postgre)
	cur = conn.cursor()
	data_dict = dict()
	for x in p:
		data_tmp_1 = (x[0][1],x[0][0],x[1]) # time, tech, appNum for update
		data_tmp_2 = (x[0][1],x[0][0],0) # time, tech 0 for insert
		# decide database 
		data_base = x[0][1].split('-')[0]
		if "DATE_"+data_base not in data_dict.keys():
			data_dict["DATE_"+data_base] = dict()
		data_dict["DATE_"+data_base][0].append(data_tmp_1)
		data_dict['DATE_'+data_base][1].append(data_tmp_2)
	for db in data_dict.keys():
		data_str_insert = ','.join(cur.mogrify("(%s,%s,%s)",x) for x in data_dict[db][0])
		sql_insert = "INSERT INTO " + db + " VALUEs "+data_str_insert +" ON CONFLICT (time,tech) DO NOTHING;"
		# print(sql_insert)
		cur.execute(sql_insert)
		conn.commit()
		data_str_update = ','.join(cur.mogrify("(date%s,%s,%s)",x) for x in data_dict[db][1])
		sql_update = "UPDATE " + db + " AS d SET appNum = c.appNum + d.appNum FROM (VALUES "+data_str_update+" ) as c(time, tech, appNum) WHERE c.time = d.time and c.tech = d.tech;"
		cur.execute(sql_insert)
		conn.commit()
	cur.close()
	conn.close()

while (CURRENT_VALUE_LOW < MAX_VALUE or CURRENT_VALUE_LOW > 51000):
	# get null null tags from mysql db
	df = sqlContext.read.format("jdbc").options(
	 	url="jdbc:mysql://sg-cli-test.cdq0uvoomk3h.us-east-1.rds.amazonaws.com:3306/dbo",
	 	driver = "com.mysql.jdbc.Driver",
	 	dbtable="(SELECT AnswerCount,CommentCount,FavoriteCount,Tags, Id, CreationDate FROM Posts WHERE Id > " + str(CURRENT_VALUE_LOW) + " AND Id < " + str(CURRENT_VALUE_UPPER) +" AND Tags IS NOT NULL) tmp",
	 	user="sherry_jiayun",
	 	password="yjy05050609").option('numPartitions',4).option('lowerBound',1).option('upperBound',50000).option('partitionColumn',6).load()
	CURRENT_VALUE_LOW = CURRENT_VALUE_UPPER
	CURRENT_VALUE_UPPER = CURRENT_VALUE_LOW + 50000

	rdd = sc.parallelize(df.collect())
	rdd_clean = rdd.map(lambda x:(x[0],x[1],x[2],x[3].replace('<',' ').replace('>',' ').replace('  ',' '),x[4],x[5],x[0]+x[1]+x[2]))
	rdd_fm = rdd_clean.flatMap(lambda x: [(w) for w in innerrdd(x)])

	# map and collect relationship weight need to divided by 2
	# relationship, weight and count
	rdd_rel = rdd_fm.map(lambda x: (combineKeyForRelationship(x),x[4]))
	rdd_rel_count = rdd_rel.combineByKey(lambda value:(value,1),lambda x,value:(value+x[0],x[1]+1),lambda x,y: (x[0]+y[0],x[1]+y[1]))

	# remove duplicate
	rdd_fm_node = rdd_fm.map(lambda x: (combineKey(x),x[4])).combineByKey(lambda value: (value),lambda x, value:(value),lambda x, y: (x))
	rdd_node_flat = rdd_fm_node.map(lambda x: (removeKey(x[0]),x[1]))
	# for node, (weight,count)
	rdd_node_cal = rdd_node_flat.combineByKey(lambda value: (value,1),lambda x,value:(value+x[0],x[1]+1),lambda x,y:(x[0]+y[0],x[1]+y[1]))

	# time and node key: time+node, value 1
	rdd_date_key = rdd_fm.map(lambda x: (combineKeyForDate(x),1)).combineByKey(lambda value:(value),lambda x,value:(value+x),lambda x,y:(x+y))
	rdd_date_cal = rdd_date_key.map(lambda x: (removeKeyForDate(x[0]),x[1]))
	rdd_date_cal.foreachPartition(writeDate)
	# write to database for node
	rdd_node_cal.foreachPartition(writeNode)
	# write to database for relationship
	rdd_rel_count.foreachPartition(writeRelationship)