from flask import Flask
from flask import render_template
from neo4jrestclient.client import GraphDatabase
import json

app = Flask(__name__)

# neo4j param
uri = "http://ec2-35-172-240-121.compute-1.amazonaws.com:7474/"
username = "neo4j"
password = "yjy05050609"

def reorganize(graph_data):
	data_after = dict()
	data_after["nodes"] = list()
	data_after["edges"] = list()

	ROOT =True
	for g in graph_data:
		node = g['nodes']
		edge = g['relationships']
		for i in node:
			nodetmp = dict()
			nodetmp['id'] = i['id']
			nodetmp['label'] = i['labels'][0]
			nodetmp['caption'] = i['properties']['name']
			nodetmp['count'] = i['properties']['count']
			nodetmp['weight'] = i['properties']['weight']
			if ROOT:
				nodetmp['root'] = 'true'
				ROOT = False
			data_after['nodes'].append(nodetmp)
		for e in edge:
			print (e)
			edgetmp = dict()
			edgetmp['source'] = e['startNode']
			edgetmp['target'] = e['endNode']
			edgetmp['name'] = e['properties']['name']
			edgetmp['count'] = e['properties']['count']
			edgetmp['caption'] = e['properties']['weight']
			data_after['edges'].append(edgetmp)
	return data_after

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/neo4j/")
def getNeo4jJson():
	gdb = GraphDatabase(uri, username=username,password=password)
	query = "MATCH p=(v1:vertex {name:'django'})-[r:Group]-() WHERE r.weight > 1000 RETURN r LIMIT 25"
	results = gdb.query(query,data_contents=True)
	graph_result = results.graph
	graph_result = reorganize(graph_result)
	js_result = str(graph_result)
	# return js_result
	return render_template("neo4j.html",data = js_result)

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)