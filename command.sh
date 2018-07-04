#!/bin/bash
# open pyspark in master
pyspark --jars path/to/mysql-connector-java-8.0.11.jar --master spark://(master ip)
# submit work with mysql 
spark-submit --jars path/to/mysql-connector-java-8.0.11.jar --master spark://(master ip):7077 path/to/python_script.py