# InsightProject
## Motivation 

Provide information that helps the developer to compare different technical sets and make a decision before they start their project.


## Pipeline

![pipeline](https://github.com/sherry-jiayun/InsightProject-SkillConnector/blob/master/pipeline.png)

## Start

1) Spark-cluster with 4 nodes (1 master and 3 slaves)

2) 2 AWS RDS instance (mysql and postgresql)

3) 1 neo4j AWS instance, follow this link (https://aws.amazon.com/marketplace/pp/B071P26C9D)

4) Start Flask front-end website by using command
```console
cd ./front-end
python app.py
```
5) See command.sh for spark/pyspark submit command
