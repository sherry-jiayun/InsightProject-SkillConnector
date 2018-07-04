from flask import Flask
from flask import render_template
from neo4jrestclient.client import GraphDatabase
import json

from flask import request

app = Flask(__name__)

# neo4j param
uri = "************"
username = "******"
password = "**********"

def reorganize(graph_data,root):
	data_after = dict()
	data_after["nodes"] = list()
	data_after["edges"] = list()

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
			if i['properties']['name'] == root:
				nodetmp['root'] = 'true'
			data_after['nodes'].append(nodetmp)
		for e in edge:
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
	graph_result = reorganize(graph_result,'django')
	js_result = str(graph_result)
	# return js_result
	return render_template("neo4j.html",data = js_result)

@app.route("/btn")
def testBtn():
	test = request.args.get('testinput')
	node = request.args.get('node')
	node1 = request.args.get('node1')
	node2 = request.args.get('node2')
	gdb = GraphDatabase(uri, username=username,password=password)
	root = 'django'
	query = "MATCH p=(v1:vertex {name:'django'})-[r:Group]-() WHERE r.weight > 1000 RETURN r LIMIT 25"
	result2 = ""
	if len(node) > 0:
		print (node)
		root = node
		query = "MATCH p=(v1:vertex {name:'"+node+"'})-[r:Group]-() RETURN r ORDER BY r.weight DESC LIMIT 25"
	if len(node1) > 0 and len(node2) > 0:
		print (node1, node2)
		query2 = "MATCH p=(v1:vertex {name:'" + node1 + "'})-[r:Group]-(v2:vertex {name:'" + node2 + "'}) return r"
		result2 = gdb.query(query2,data_contents = True)
		print (result2.graph)
	results = gdb.query(query,data_contents=True)
	graph_result = results.graph
	graph_result = reorganize(graph_result,root)
	js_result = str(graph_result)
	# return js_result
	return render_template("neo4j.html",data = js_result,r = result2.graph)

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)