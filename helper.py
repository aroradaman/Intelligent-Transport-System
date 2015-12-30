import pika
import uuid
import neo4j
import neo4j
import time
import threading
import socket
import select
import random
import json
import math
import hashlib
from operator import itemgetter

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

SERVER_IP	=	'127.0.0.1'
SERVER_PORT	=	12345
MAX_SEGMENT =	9000

def md5(anything) :
	hasher = hashlib.md5()
	hasher.update(str(anything).lower())
	return hasher.hexdigest().lower()

def link_parameters(node1,node2,dist) :
	dx = node1['x'] - node2['x']
	dy = node1['y'] - node2['y']
	angle_rads = math.atan2(dx,dy)
	angle_deg = math.degrees(angle_rads)
	if angle_deg > 90 :
		angle_deg -= 90

	mod_vel = math.sqrt( node1['velocity']**2 + node1['velocity']**2 - node1['velocity']*node2['velocity']*math.cos(angle_deg))
	reliability = float(dist) / mod_vel
	hc = 1

	trans_delay = float(packet_size) / link_speed
	prop_delay = float(dist) / prop_speed_channel
	queueing_delay = float(random.randrange(200))/ pow(10,8)

	# print "trans_delay",trans_delay
	# print "queueing_delay",queueing_delay
	# print "prop_delay",prop_delay

	delay = trans_delay + prop_delay + queueing_delay	
	return angle_deg,reliability,delay,hc

def obj_func( delay , hc , reliability ) :
	return x_reliability * (  reliability / reliability_constant) + x_delay * (  delay_constant / delay ) + x_hc * (  hc_constant / hc )

class rpc_client (object):
	
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		self.channel = self.connection.channel()
		result = self.channel.queue_declare(exclusive=True)
		self.callback_queue = result.method.queue
		self.channel.basic_consume(self.on_response, no_ack=True,queue=self.callback_queue)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body

	def call(self, n):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(exchange='',routing_key='rpc_queue',properties = pika.BasicProperties(reply_to = self.callback_queue,correlation_id = self.corr_id,),body=str(n))
		while self.response is None:
			self.connection.process_data_events()
		self.connection.close()
		return self.response

def gpsUpdate(params) :	
	connection = neo4j.connect('http://localhost:7474')
	cursor = connection.cursor()
	try :
		cursor.execute('MATCH (n:Node{id:'+ str(params['id'])+'})-[rel:IS_CONNECTED]-(r:Node) DELETE rel')
		connection.commit()
	except :
		pass
	cursor.execute('MATCH (n:Node{id:'+ str(params['id']) +'}) SET n.x = n.x + ' + str( float(random.randrange(-200,200))/100) + ' ')
	connection.commit()
	for r in cursor.execute('MATCH (p:Node{id:'+ str(params['id']) +'}) RETURN p') :
		outer = r[0]
	try :
		for n in cursor.execute('MATCH (n:Node) RETURN n ') :				
			for inner in n :
				if inner == outer :
					pass
				else :
					dist = math.sqrt( ( inner['x'] - outer['x']) **2 + (inner['y'] - outer['y']) **2 )
					if dist < radio_radius :
						angle,reliability,delay,hc = link_parameters(inner,outer,dist)
						if inner['x'] > outer['x'] :
							cursor.execute('MATCH (n:Node {id:'+str(inner['id'])+'}), (r:Node {id:'+str(outer['id'])+'}) CREATE UNIQUE (r)-[:IS_CONNECTED { type:"edge", reliability:' + str(reliability)+ ', delay:"' + str(delay) + '" , to:' + str(inner['id']) +' ,hc:1 , type:"edge", angle:' + str(angle) + ' , from:'+ str(outer['id']) +'}]->(n)')
							connection.commit()
						else :
							cursor.execute('MATCH (n:Node {id:'+str(inner['id'])+'}), (r:Node {id:'+str(outer['id'])+'}) CREATE UNIQUE (n)-[:IS_CONNECTED { reliability:' + str(reliability)+ ', delay:"' + str(delay) + '" , from:' + str(inner['id']) +' ,hc:1 , angle:' + str(angle) + ' , type:"edge", to:'+ str(outer['id']) +'}]->(r)')
							connection.commit()
		connection.close()
	except :
		connection.close()
		pass

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
	for r in cursor.execute(query) :
		delay = 0
		reliability = 1
		hc = 0
		temp = []		
		for link in r[0]:			
			if link['type'] == 'node' :
				temp.append(link['id'])				
				nodeDetails.update({str(link['id']) : {'server_ip':link['server_ip'],'server_port':link['server_port']}})
			elif link['type'] == 'edge' :				
				delay += float(link['delay'])
				reliability *= link['reliability']
				hc += link['hc']		
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
	print 'Route to be followed : ' , Route[max_index][1:]
	reply = nodeDetails[str(Route[max_index][1])]
	reply.update({'next':Route[max_index][1]})
	return json.dumps(reply)

def transferToNextHop(next,ip,port,end,segment) :
	client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
	client.connect((ip,port))	
	client.send('FWRD DATA' + json.dumps({'start':next,'end':end}) + segment)
	client.close()

def servAddrUpdate(params) :
	connection = neo4j.connect('http://localhost:7474')
	cursor = connection.cursor()
	cursor.execute('MATCH (n:Node{id:'+ str(params['id']) +'}) SET n.server_ip = "' + params['ip'] + '" ')
	connection.commit()
	cursor.execute('MATCH (n:Node{id:'+ str(params['id']) +'}) SET n.server_port = ' + str(params['port']) + ' ')
	connection.commit()	

def requestForRoute(start,end) :
	client = rpc_client()
	response =  client.call('GET ROUTE'+json.dumps({'start':start,'end':end}))
	return json.loads(response)
