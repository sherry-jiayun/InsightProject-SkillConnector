from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
dataframe_mysql = sqlContext.read.format("jdbc").options(url="jdbc:mysql://sg-cli-test.cdq0uvoomk3h.us-east-1.rds.amazonaws.com:3306/stackoverflow2010",driver = "com.mysql.jdbc.Driver",dbtable="stackoverflow2010.posts",user="sherry_jiayun",password="yjy05050609").load()