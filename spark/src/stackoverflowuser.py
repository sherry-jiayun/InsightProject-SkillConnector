from __future__ import print_function
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
from neo4j.v1 import GraphDatabase
from pyspark.sql.functions import *
import psycopg2
import time

from pyspark.sql.functions import col

sc = SparkContext(master="master ip")
sqlContext = SQLContext(sc)
# read and combine 

url = 'xxxxxxxxxx'

def innerrdd(x):
	vertex_list = x[3].strip().split(' ')
	tmplist = list()
	for xx in vertex_list:
		tmpitem = [x[0],x[1],x[2],xx,x[4]]
		tmplist.append(tmpitem)
	return tmplist

def writeUser(p):
	# connect to postgresql
	postgre = "xxxxxxxxxx"
	connecttmp = 0 # try 10 times
	while (connecttmp < 10 ):
		try:
			conn = psycopg2.connect(postgre,connect_timeout=3)
			break
		except:
			print ("connect attemp: ",connecttmp)
			time.sleep(1)
			connecttmp += 1
	cur = conn.cursor()
	data_dict = dict()
	data_dict[0] = list()
	data_dict[1] = list()
	for x in p:
		data_tmp_1 = (x[0],x[1],x[2],x[3],0,0)
		data_tmp_2 = (x[0],x[3],x[4],x[5])
		data_dict[0].append(data_tmp_1)
		data_dict[1].append(data_tmp_2)
	db = "USER_TECH"
	data_str_insert = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s)",x) for x in data_dict[0])
	sql_insert = "INSERT INTO insight." + db + " VALUEs "+data_str_insert +" ON CONFLICT (userId,tech) DO NOTHING;"
	cur.execute(sql_insert)
	conn.commit()
	data_str_update = ','.join(cur.mogrify("(%s,%s,%s,%s)",x) for x in data_dict[1])
	sql_update = "UPDATE insight." + db + " AS d SET score = c.score + d.score, count = c.count + d.count FROM (VALUES "+data_str_update+" ) as c(userId,tech,score,count) WHERE c.userId = d.userId and c.tech = d.tech;"
	# print (sql_update)
	cur.execute(sql_update)
	conn.commit()
	cur.close()
	conn.close()

num_of_row = 200000
CURRENT_VALUE_LOW = 0
CURRENT_VALUE_UPPER = CURRENT_VALUE_LOW + num_of_row # 50000 ROWS PER LOOP
df_MAX = sqlContext.read.format("jdbc").options(
	url = url,driver = "org.postgresql.Driver",
	dbtable = """(SELECT MAX("Id") FROM dbo."Comments") tmp""",
	user = "xxxxxxxxxx",
	password = "xxxxxxxxxx").load()
MAX_VALUE = df_MAX.collect()
MAX_VALUE = MAX_VALUE[0]['max'] # get max id value 

# only test first 1/3 
# MAX_VALUE = MAX_VALUE / 3
count = 0 
partitionNum = 18
while(CURRENT_VALUE_LOW < MAX_VALUE):
	df_c = sqlContext.read.format("jdbc").options(
	 	url = url,driver = "org.postgresql.Driver",
	 	dbtable = """(SELECT "Id","Score","UserId","PostId" FROM dbo."Comments" WHERE "Id" > """+str(CURRENT_VALUE_LOW)+""" AND "Id" < """ + str(CURRENT_VALUE_UPPER) +""" AND "UserId" is not null) tmp""",
	 	user="xxxxxxxxxx",
	 	password="xxxxxxxxxx").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',6).load()
	CURRENT_VALUE_LOW = CURRENT_VALUE_UPPER
	CURRENT_VALUE_UPPER = CURRENT_VALUE_LOW + num_of_row
	df_c = df_c.where(col("UserId").isNotNull())
	post_id = df_c.select('PostId').rdd.map(lambda x: str(x.PostId)).collect()
	postIdStr = ','.join(post_id)
	postIdStr = '('+postIdStr+')'
	df_p = sqlContext.read.format("jdbc").options(
	 	url = url,driver = "org.postgresql.Driver",
	 	dbtable = """(SELECT "Id","Tags" FROM dbo."Posts" WHERE "Id" IN """+ postIdStr +""") tmp""",
	 	user="xxxxxxxxxx",
	 	password="xxxxxxxxxx").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',6).load()
	user_id = df_c.select('UserId').rdd.map(lambda x: str(x.UserId)).collect()
	userIdStr = ','.join(user_id)
	userIdStr = '('+userIdStr+')'
	df_u = sqlContext.read.format("jdbc").options(
	 	url = url,driver = "org.postgresql.Driver",
	 	dbtable = """(SELECT "Id","DisplayName","WebsiteUrl" FROM dbo."Users" WHERE "Id" IN """ + userIdStr + """) tmp""",
	 	user="xxxxxxxxxx",
	 	password="xxxxxxxxxx").option('numPartitions',partitionNum).option('lowerBound',1).option('upperBound',20).option('partitionColumn',6).load()
	df_combine = df_c.alias('c').join(df_p.alias('p'),col('c.PostId') == col('p.Id')).join(df_u.alias('u'),col('c.UserId')==col('u.Id'))
	df_combine = df_combine.where(col("Tags").isNotNull())
	rdd = sc.parallelize(df_combine.collect(),72)
	rdd_clean = rdd.map(lambda x:(x[2],x[7],x[8],x[5].replace('<',' ').replace('>',' ').replace('  ',' '),x[1]))
	rdd_fm = rdd_clean.flatMap(lambda x: [w for w in innerrdd(x)]).map(lambda x: ((x[0],x[3]),(x[1],x[2],x[4])))
	rdd_cal = rdd_fm.combineByKey(lambda value: ((value[0],value[1],value[2]),1),lambda x,value:((x[0][0],x[0][1],x[0][2]+value[2]),x[1]+1),lambda x,y:((x[0],x[1],x[0][2]+y[0][2]),x[1]+y[1]))
	rdd_final = rdd_cal.map(lambda x:(x[0][0],x[1][0][0],x[1][0][1],x[0][1],x[1][0][2],x[1][1]))
	rdd_final.foreachPartition(writeUser)
	count += 1
	if count > 10:
		sc.stop()
		sc = SparkContext(master="master ip:7077")
		sqlContext = SQLContext(sc)
		count = 0
sc.stop()

