import neo4j
import time
import threading
import socket
import select
import random
import json

with open('conf.json','r') as f :
	conf = json.loads(f.read())

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

def gpsUpdate(id,addr) :	
	connection = neo4j.connect('http://localhost:7474')
	cursor = connection.cursor()
	cursor.execute('MATCH (n:Node{id:'+str(id)+'}) SET n.server_ip = "' + addr[0] + '" ')
	connection.commit()
	cursor.execute('MATCH (n:Node{id:'+str(id)+'}) SET n.server_port = ' + str(addr[1]) + ' ')
	connection.commit()
	while True :
		cursor.execute('MATCH (n:Node{id:'+str(id)+'}) SET n.x = n.x + ' + str(random.randrange(50,100)) + ' ')
		connection.commit()
		time.sleep(1)

	
	while True :		
		time.sleep(1)

def nodeServer(id) :
	nodeServer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)	
	nodeServer.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
	nodeServer.bind(('127.0.0.1',0))
	addr = nodeServer.getsockname()
 	threading.Thread(target=gpsUpdate,args=(id,addr)).start()
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
	 	time.sleep(3)

for i in range(10,20) :
	threading.Thread(target=Node,args=(i,)).start()

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
