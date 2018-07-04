#!/bin/bash
# open pyspark in master
pyspark --jars mysql-connector-java-8.0.11.jar --master spark://(master ip)
# submit work with mysql 
spark-submit --jars mysql-connector-java-8.0.11.jar --master spark://(master ip):7077 python_script.py