# InsightProject
## Motivation 

1)Developper want to make sure they are updated.

2)Before starting a project, developer may want to know what technology set to choose by finding out whether this set make a good group.

3)In addition, developer may also want to reach out for the expert in one specific area.

## Pipeline

![pipeline](https://github.com/sherry-jiayun/InsightProject/blob/master/pipeline.png)

## Start

1) Spark-cluster with 4 nodes (1 master and 3 slaves)

2) 2 AWS RDS instance (mysql and postgresql)

3) 1 neo4j AWS instance, follow this link (https://aws.amazon.com/marketplace/pp/B071P26C9D)

4) Start Flask front-end website by using command
```console
cd ./front-end
python app.py
```
5) See command.py for spark/pyspark submit command
