import neo4j
import random
import math
import json

with open('conf.json','r') as f :
	conf = json.loads(f.read())

reAssign = conf['reAssign']
link_speed = conf['link_speed']
radio_radius = conf['radio_radius']
packet_size = conf['packet_size']
prop_speed_channel =  conf['prop_speed_channel']
x_reliability = conf['x_reliability']
x_delay = conf['x_delay']
x_hc = conf['x_hc']
reliability_constant = conf['reliability_constant']
hc_constant = conf['hc_constant']
delay_constant = conf['delay_constant']
db_address = conf['db_address']

def obj_func( delay , hc , reliability ) :
	return x_reliability * (  reliability / reliability_constant) + x_delay * (  delay_constant / delay ) + x_hc * (  hc_constant / hc )

radio_radius = 250

connection = neo4j.connect('http://localhost:7474')
cursor = connection.cursor()

def getRoute(params) :
	if conf['biDirectional'] :
		query = "MATCH p = (:Node{id:" + str(params['start']) + "})-[r:IS_CONNECTED*1..10]-(:Node{id:" + str(params['end']) + "}) RETURN DISTINCT p"
	else :
		if params['end'] > params['start'] :
			query = "MATCH p = (:Node{id:" + str(params['start']) + "})-[r:IS_CONNECTED*1..10]->(:Node{id:" + str(params['end']) + "}) RETURN DISTINCT p"
		else :
			query = "MATCH p = (:Node{id:" + str(params['end']) + "})-[r:IS_CONNECTED*1..10]->(:Node{id:" + str(params['start']) + "}) RETURN DISTINCT p"				
	qosMatrix = []
	maxQoS = 0
	max_index = 0
	minQoS = 100000000000000
	min_index = 0
	count = 0
	Route = []
	connection = neo4j.connect('http://localhost:7474')
	cursor = connection.cursor()
	nodeDetails = {}
	print 'starting'
	for r in cursor.execute(query) :		
		delay = 0
		reliability = 1
		hc = 0
		temp = []
		for link in r[0] :
			if link['type'] == 'node' :
				temp.append(link['id'])
				#nodeDetails.update({str(link['id']) : {'server_ip':link['server_ip'],'server_port':link['server_port']}})
			elif link['type'] == 'edge' :				
				delay += float(link['delay'])
				reliability *= link['reliability']
				hc += link['hc']
		print temp		
		data = str(temp)
		Route.append(temp)
		obj_val = obj_func( delay , hc , reliability )
		qosMatrix.append([ delay , hc , reliability , obj_val ])
		if obj_val > maxQoS :
			maxQoS = obj_val
			max_index = count
		if obj_val < minQoS :
			minQoS = obj_val
			min_index = count
		count += 1
	print '\nRoute to be followed : ' , Route[max_index][1:]
	#reply = nodeDetails[str(Route[max_index][1])]
	#reply.update({'next':Route[max_index][1]})
	#return json.dumps(reply)

getRoute({'start':1,'end':11})
