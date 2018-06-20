# start spark 
# peg service spark-cluster spark stop 
df = sqlContext.read.format("jdbc").options(url="jdbc:mysql://sg-cli-test.cdq0uvoomk3h.us-east-1.rds.amazonaws.com:3306/stackoverflow2010",driver = "com.mysql.jdbc.Driver",dbtable="(SELECT Tags FROM posts WHERE Tags IS NOT NULL LIMIT 100) tmp",user="sherry_jiayun",password="yjy05050609")
.option('numPartitions',4).option.load()
print(df.printSchema())

df.withColumn('Tags', regexp_replace('Tags', '<', ' '))
newDfString = newDf.agg(concat_ws(" ", collect_list(newDf['Tags'])))

uri = "bolt://ec2-34-234-207-154.compute-1.amazonaws.com:7687"
driver = GraphDatabase.driver(uri,auth=("neo4j","yjy05050609"))
session = driver.session()
cypher = 'CREATE (Reaction1:Reaction {RXNid:"reaction1", name:"AmideFormation"})'

"MERGE (vertex_name:vertex{ name: '"+list_x[0]+"' })RETURN vertex_name.name"
session.run(cypher)
session.close()

MATCH (c_:vertex { name: 'c#' }),(winforms:vertex { name: 'winforms' })
MERGE (c_)-[r1:KNOWS]-(winforms)

newDf = newDf.withColumn('Tags', trim(col('Tags')))
newDf.collect()

pyspark --jars mysql-connector-java-8.0.11.jar --master spark://18.208.87.33:7077
# submit work
spark-submit --jars mysql-connector-java-8.0.11.jar --master spark://ip-10-0-0-7.ec2.internal:7077 testpythonfile.py

spark-submit --jars mysql-connector-java-8.0.11.jar testpythonfile.py
/usr/local/spark/bin/spark-submit --class org.apache.spark.examples.SparkPi --jars mysql-connector-java-8.0.11.jar --master spark://ip-10-0-0-7.ec2.internal:7077 testpythonfile.py

/home/ubuntu/.local/bin/spark-submit --jars /usr/share/java/mysql-connector-java.jar testpythonfile.py

spark-submit --deploy-mode cluster --master yarn --num-executors 5 --executor-cores 5 --executor-memory 20g –conf spark.yarn.submit.waitAppCompletion=false wordcount.py s3://inputbucket/input.txt s3://outputbucket/

peg sshcmd-cluster spark-cluster "cd ~/InsightProject/ ; git pull"

# database delete 
MATCH p=()-[r:Group]->() DELETE r
MATCH (n:vertex) DELETE n

MATCH p=()-[r:Group]->() RETURN COUNT(*)
MATCH (n:vertex) RETURN COUNT(*)

MATCH (n)
RETURN n
ORDER BY n.weight LIMIT 25


schema = StructType([StructField("node_name_1", StringType(), True),StructField("node_name_2", StringType(), True),StructField("edge_weight", IntegerType(), True)])
df1 = spark.createDataFrame([],schema)
df2 = spark.createDataFrame([],schema)
df3 = spark.createDataFrame([],schema)
df4 = spark.createDataFrame([],schema)
testdata = [('C#','.NET',25)]

# combiner 
testcombiner = list()
testcombiner.append(['C#',1])
testcombiner.append(['C#',1])
testcombiner.append(['python',2])
testcombiner.append(['django',6])
testcombiner.append(['django',5])

rdd = sc.parallelize(testcombiner)


rdd_node1_sum = rdd_fm_node1.combineByKey(lambda value: (value, 1),lambda x, value: (x[0] + value, x[1] + 1),lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd_node1_sum = rdd_fm_node1.combineByKey(lambda value: (value),lambda x, value:(x+value),lambda x, y: (x+y))
rdd_node2_sum = rdd_fm_node2.combineByKey(lambda value: (value),lambda x, value:(x+value),lambda x, y: (x+y))

df = sqlContext.read.format("jdbc").options(
	url="jdbc:mysql://sg-cli-test.cdq0uvoomk3h.us-east-1.rds.amazonaws.com:3306/dbo",
	driver = "com.mysql.jdbc.Driver",
	dbtable="(SELECT AnswerCount,CommentCount,FavoriteCount,Tags, Id, CreationDate FROM Posts WHERE Tags IS NOT NULL) tmp",
	user="sherry_jiayun",
	password="yjy05050609").option('numPartitions',4).option('lowerBound',1).option('upperBound',1000).option('partitionColumn',6).load()


 nohup mysql -h sg-cli-test.cdq0uvoomk3h.us-east-1.rds.amazonaws.com -P 3306 -u sherry_jiayun -p dbo < partition.sql >> log.txt

 nohup mysql -h testmysql.cdq0uvoomk3h.us-east-1.rds.amazonaws.com -P 3306 -u sherry_jiayun -p stackoverflow2010 < partition2.sql &



