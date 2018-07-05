from flask import Flask,redirect, url_for
from flask import render_template
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import client
import psycopg2
import json
import time
import datetime

from flask import request

app = Flask(__name__)

# neo4j param
uri = "****************"
username = "neo4j"
password = "****************"

url = "****************"

def reorganize(result,tech1,tech2):
	dict_current = dict()
	dict_current["nodes"] = list()
	dict_current["edges"] = list()

	dict_current["nodes"].append({'id':0, 'name':tech1,'fixed':'True', 'x': 150, 'y': 150})
	dict_current["nodes"].append({'id':1, 'name':tech2,'fixed':'True', 'x': 100, 'y': 150})
	count = 2
	for i in range(10):
		# add node 
		node_name = result[i][0]['data']['name']
		dict_current["nodes"].append({'id':count,'name':node_name})
		dict_current["edges"].append({'source':count,'target':0,'name':result[i][1]['data']['name']})
		dict_current["edges"].append({'source':count,'target':1,'name':result[i][2]['data']['name']})
		count += 1
	return dict_current

@app.route('/')
def hello_world():
  return 'Hello from Insight flask!'

@app.route("/neo4j/")
def getNeo4jJson():

	tech = 'django'
	tech_with = 'reactjs'
	tech_or = 'angular'

	# section 1
	sec1 = dict()
	gdb = GraphDatabase(uri,username=username,password=password)
	summary_sql_1 = "MATCH p = (v:vertex {name:'"+tech+"'})-[r:Group]->(v1:vertex {name:'"+tech_with+"'}) RETURN v,r,v1;"
	result = gdb.query(q=summary_sql_1,data_contents=True)
	sec1['compare1'] = [result[0][0]['data'],result[0][1]['data'],result[0][2]['data']]

	summary_sql_2 = "MATCH p = (v:vertex {name:'"+tech+"'})-[r:Group]->(v1:vertex {name:'"+tech_or+"'}) RETURN v,r,v1;"
	result = gdb.query(q=summary_sql_2,data_contents=True)
	sec1['compare2'] = [result[0][0]['data'],result[0][1]['data'],result[0][2]['data']]

	# section 2 
	connecttmp = 0 # try 10 times
	while (connecttmp < 10 ):
		try:
			conn = psycopg2.connect(url,connect_timeout=3)
			break
		except:
			print ("connect attemp: ",connecttmp)
			time.sleep(1)
			connecttmp += 1
	cur = conn.cursor()
	sec2 = dict()
	in_list = "('"+tech+"','"+tech_with+"','"+tech_or+"')"
	month_list = [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0]
	for i in [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018]:
		time_sql_tmp = "SELECT EXTRACT (month FROM time) AS month, tech, SUM(appNum) AS h FROM date_" + str(i) + " WHERE tech in " + in_list + " GROUP BY tech, 1;"
		cur.execute(time_sql_tmp)
		result = cur.fetchall()
		
		sec2[i] = dict() # initialize a dict 
		for r in result:
			month = r[0]
			if month not in sec2[i].keys():
				sec2[i][month] = dict()
				date = str(i) + '-' + str(int(month))
				sec2[i][month]['date'] = date
				sec2[i][month]['tech_label'] = tech
				sec2[i][month]['tech1_label'] = tech_with
				sec2[i][month]['tech2_label'] = tech_or
			if r[1] == tech:
				sec2[i][month]['tech'] = r[2]
			if r[1] == tech_with:
				sec2[i][month]['tech1'] = r[2]
			if r[1] == tech_or:
				sec2[i][month]['tech2'] = r[2]

	sec2_list = list()
	for key in [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018]:
		for key2 in month_list:
			if key2 in sec2[key]:
				dict_tmp = sec2[key][key2]
				if 'tech' not in dict_tmp.keys():
					dict_tmp['tech'] = 0
				if 'tech1' not in dict_tmp.keys():
					dict_tmp['tech1'] = 0
				if 'tech2' not in dict_tmp.keys():
					dict_tmp['tech2'] = 0
				sec2_list.append(dict_tmp)
	sec2_list = sec2_list[:-1]
	conn.close()

	# section 3

	user_sql_1 = "MATCH p1 = (u:user) -[r1:Contribute] -> (v1:vertex {name:'"+tech+"'}), \
	p2 = (u:user) - [r2:Contribute] -> (v2:vertex {name:'"+tech_with+"'}) \
	RETURN u,r2 ORDER BY r2.weight DESC,r1.weight DESC LIMIT 10"
	result = gdb.query(q=user_sql_1,data_contents=True)
	sec3 = dict()
	sec3['compare1'] = list()
	for i in range(4):
		dict_tmp = dict()
		dict_tmp['user_name'] = result[i][0]['data']['name']
		dict_tmp['user_web'] = result[i][0]['data']['website']
		dict_tmp['weight'] = result[i][1]['data']['weight']
		dict_tmp['count'] = result[i][1]['data']['count']
		sec3['compare1'].append(dict_tmp)

	user_sql_1 = "MATCH p1 = (u:user) -[r1:Contribute] -> (v1:vertex {name:'"+tech+"'}), \
	p2 = (u:user) - [r2:Contribute] -> (v2:vertex {name:'"+tech_or+"'}) \
	RETURN u,r2 ORDER BY r2.weight DESC,r1.weight DESC LIMIT 10"
	result = gdb.query(q=user_sql_1,data_contents=True)
	sec3['compare2'] = list()
	for i in range(4):
		dict_tmp = dict()
		dict_tmp['user_name'] = result[i][0]['data']['name']
		dict_tmp['user_web'] = result[i][0]['data']['website']
		dict_tmp['weight'] = result[i][1]['data']['weight']
		dict_tmp['count'] = result[i][1]['data']['count']
		sec3['compare2'].append(dict_tmp)

	# section 4
	sec4 = dict()
	related_sql_1 = "MATCH p1 = (v:vertex) -[r1:Group] -> (v1:vertex {name:'"+tech+"'}), \
	p2 = (v:vertex) - [r2:Group] -> (v2:vertex {name:'"+tech_with+"'})\
	RETURN v,r1,r2 ORDER BY r2.weight DESC, r1.weight DESC LIMIT 10"
	result = gdb.query(q=related_sql_1,data_contents=True)
	sec4['compare1'] = reorganize(result,tech,tech_with)

	related_sql_2 = "MATCH p1 = (v:vertex) -[r1:Group] -> (v1:vertex {name:'"+tech+"'}), \
	p2 = (v:vertex) - [r2:Group] -> (v2:vertex {name:'"+tech_or+"'})\
	RETURN v,r1,r2 ORDER BY r2.weight DESC, r1.weight DESC LIMIT 10"
	result = gdb.query(q=related_sql_2,data_contents=True)
	sec4['compare2'] = reorganize(result,tech,tech_or)

	return render_template("index.html",data = '[]',sec1 = sec1, sec2 = sec2_list, sec3 = sec3, sec4=sec4)

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)