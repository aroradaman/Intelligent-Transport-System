import neo4j
import time
import threading
import socket
import select
import random
import json
import math

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


class Node (object) :
	def __init__(id) :
		self.id = id
		connection = neo4j.connect('http://localhost:7474')
		cursor = connection.cursor()
		print 'haha'
		for r in cursor.execute(' MATCH (node:Node{id:' + str(id) + '}) RETURN node ') :
			print r[0]


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

def transferToNextHop(data,next,ip,port,end) :
	client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)	
	client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
	client.connect((ip,port))
	client.send('FWRD DATA'+json.dumps({'data':data,'start':next,'end':end}))

def requestForRoute(start,end) :
	client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)	
	client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)	
	client.connect((SERVER_IP,SERVER_PORT))	
	client.send('GET ROUTE'+json.dumps({'start':start,'end':end}))		
	return json.loads(client.recv(MAX_SEGMENT))	

def getRoute(params) :
	query = "MATCH p = (:Node{id:" + str(params['start']) + "})-[r:IS_CONNECTED*1..10]->(:Node{id:" + str(params['end']) + "}) RETURN DISTINCT p"
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
				temp.append(link['to'])
				delay += float(link['delay'])
				reliability *= link['reliability']
				hc += link['hc']
				
		temp = sorted(list(set(temp)))
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
	print '\tRoute :\t' , Route[max_index]
	reply = nodeDetails[str(Route[max_index][1])]
	reply.update({'next':Route[max_index][1]})
	return json.dumps(reply)

def gpsUpdateRequest(id,ip,port) :
	client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)	
	client.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
	client.connect((SERVER_IP,SERVER_PORT))
	client.send('GPS UPDTE'+json.dumps({'ip':ip,'port':port,'id':id}))
	client.close()

def gpsUpdate(params) :
	id = params['id']
	ip = params['ip']
	port = params['port']
	connection = neo4j.connect('http://localhost:7474')
	cursor = connection.cursor()
	cursor.execute('MATCH (n:Node{id:'+str(id)+'}) SET n.server_ip = "' + ip + '" ')
	connection.commit()
	cursor.execute('MATCH (n:Node{id:'+str(id)+'}) SET n.server_port = ' + str(port) + ' ')
	connection.commit()
	try :
		cursor.execute('MATCH (n:Node{id:'+str(id)+'})-[rel:IS_CONNECTED]-(r:Node) DELETE rel')
		connection.commit()
	except :
		pass
	cursor.execute('MATCH (n:Node{id:'+str(id)+'}) SET n.x = n.x + ' + str(random.randrange(10,30)) + ' ')
	connection.commit()		
	for r in cursor.execute('MATCH (p:Node{id:'+str(id)+'}) RETURN p') :
		outer = r[0]
	for p in cursor.execute('MATCH (n:Node) RETURN collect(n)') :
		for inner in p[0] :
			dist = math.sqrt( ( inner['x'] - outer['x']) **2 + (inner['y'] - outer['y']) **2 )
			if dist < radio_radius :
				angle,reliability,delay,hc = link_parameters(inner,outer,dist)
				if inner['x'] > outer['x'] :
					cursor.execute('MATCH (n:Node {id:'+str(inner['id'])+'}), (r:Node {id:'+str(outer['id'])+'}) CREATE UNIQUE (r)-[:IS_CONNECTED { type:"edge", reliability:' + str(reliability)+ ', delay:"' + str(delay) + '" , to:' + str(inner['id']) +' ,hc:1 , angle:' + str(angle) + ' , from:'+ str(outer['id']) +'}]->(n)')
					connection.commit()
				else :
					cursor.execute('MATCH (n:Node {id:'+str(inner['id'])+'}), (r:Node {id:'+str(outer['id'])+'}) CREATE UNIQUE (n)-[:IS_CONNECTED { reliability:' + str(reliability)+ ', delay:"' + str(delay) + '" , from:' + str(inner['id']) +' ,hc:1 , angle:' + str(angle) + ' , to:'+ str(outer['id']) +'}]->(r)')
					connection.commit()


def nodeServer(id) :
	nodeServer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)	
	nodeServer.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
	nodeServer.bind(('127.0.0.1',0))
	addr = nodeServer.getsockname()
 	gpsUpdateRequest(id,addr[0],addr[1])
	inputFd = [ nodeServer ]
	outputFd = []
	nodeServer.listen(100)
	while True :
		readFd , writeFd , expFd = select.select(inputFd,outputFd,[])
		for fd in readFd :
			if fd is nodeServer :
				client , cli_Addr = fd.accept()				
				inputFd.append(client)
			else :
				data = fd.recv(MAX_SEGMENT)
				if len(data) > 9 :
					header = data[:9]
					segment = data[9:]
					print header + '\t' + segment	
					if header == 'FWRD DATA' :
						params = json.loads(segment)
						if params['end'] == id :
							print 'Reached Destination'
							print 'Data Recieved : ' + params['data'] 
						else :
							reply = requestForRoute(params['start'],params['end'])							
							transferToNextHop(params['data'],reply['next'],reply['server_ip'],reply['server_port'],params['end'])

def Node(id) :	
 	threading.Thread(target=nodeServer,args=(id,)).start()
 	time.sleep(3)
 	if id == 10 :
 		actual_fragment = 'This is sample data for testing :p'
	 	data = requestForRoute(id,18)
	 	transferToNextHop(actual_fragment,data['next'],data['server_ip'],data['server_port'],18)
	 
	while True :
	 	time.sleep(5)

# for i in range(10,20) :
# 	threading.Thread(target=Node,args=(i,)).start()

a = Node(10)
print a

queue = {}

server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
server.bind((SERVER_IP,SERVER_PORT))

inputFd = [ server ]
outputFd = []

server.listen(100)

while True :
	readFd , writeFd , expFd = select.select(inputFd,outputFd,[])
	for fd in readFd :
		if fd is server :
			client , cli_Addr = fd.accept()			
			inputFd.append(client)
		else :
			data = fd.recv(MAX_SEGMENT)
			if len(data) > 9 :				
				header = data[:9]
				segment = data[9:]
				print header+ '\t' + segment
				if header == 'GET ROUTE' :
					params = json.loads(segment)
					nextHop = getRoute(params)
					fd.send(nextHop)
				if header == 'GPS UPDTE' :				
					params = json.loads(segment)
					threading.Thread(target=gpsUpdate,args=(params,)).start()
					#gpsUpdate(params)
