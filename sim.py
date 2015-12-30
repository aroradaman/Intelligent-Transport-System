import json
import neo4j
import math
import random
import socket
import math
import threading
import pika
import time
import helper
import select
import Queue
from operator import itemgetter

with open('conf.json','r') as f :
	conf = json.loads(f.read())

counter = 0

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
MAX_DATA_SIZE = 40000
MAX_SIZE = 50000


class Node (object) :

	def __init__(self,Id) :
		self.id = Id
		self.bindNodeServer()
		self.forwardingCache = {}
		self.forwardingQueue = Queue.Queue()
		self.incompleteData = {}
		self.openFds = {}		
		client = helper.rpc_client()
		client_fd = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		client_fd.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
		self.transferingFd = client_fd
		response =  client.call('SRV UPDTE'+json.dumps({'ip':self.server_ip,'port':self.server_port,'id':self.id}))
		threading.Thread(target=self.nodeParameterUpdateRequest,args=()).start()
		threading.Thread(target=self.forwardPacket,args=()).start()
		
	def nodeParameterUpdateRequest(self) :
		time.sleep(1)
		while True :
			time.sleep(1)
			#client = helper.rpc_client()
			#response = client.call('GPS UPDTE'+json.dumps({'id':self.id}))

	def bindNodeServer(self) :
		self.server = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		self.server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
		self.server.bind(('127.0.0.1',0))
		addr = self.server.getsockname()
		self.server_ip = addr[0]
		self.server_port = addr[1]
		threading.Thread(target=self.runNodeServer,args=()).start()

	def transferData(self,end,file_path) :
		FILE = file_path.split('/')[-1]
		with open(file_path,'rb') as f :
			data = f.read()
		time_Stamp = helper.md5(time.time())[:6] + helper.md5(data[:100])[:6]		
		client = helper.rpc_client()
		reply = json.loads(client.call('GET ROUTE' + json.dumps({'start':self.id,'end':end})))
		packets_seq = [ '!_$@$_!' + FILE + '!_$@$_!' +  str(i) + '!_$@$_!' + time_Stamp + '!_$@$_!' for i in range((len(data)/MAX_DATA_SIZE)+1)] 
		client = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		count = 00
		for item in packets_seq :
			client.sendto('FWRD DATA' + json.dumps({'next':reply['next'],'start':self.id,'end':end}) + '!_$@damei@$_!' + FILE + '!_$@damei@$_!' +  str(count) + '!_$@damei@$_!' + time_Stamp + '!_$@damei@$_!' + data[(count)*MAX_DATA_SIZE:(count+1)*MAX_DATA_SIZE],(reply['server_ip'],reply['server_port']))
			ack_data , ack_addr = client.recvfrom(MAX_DATA_SIZE)
			count += 1	
		client.sendto('FWRD DATA' + json.dumps({'next':reply['next'],'start':self.id,'end':end}) + '!_$@damei@$_!' + FILE + '!_$@damei@$_!ENDENDEND!_$@damei@$_!' + time_Stamp + '!_$@damei@$_!',(reply['server_ip'],reply['server_port']))
		ack_data , ack_addr = client.recvfrom(MAX_DATA_SIZE)

	def forwardPacket(self) :
		while True :
			req = self.forwardingQueue.get()			
			params = req['params']
			segment = req['segment']
			if params['next'] in self.forwardingCache :
				if time.time() - self.forwardingCache[params['next']]['time'] < conf['cache_update_limit'] :				
					reply = self.forwardingCache[params['next']]
					reply.update({'next':params['next']})
				else :
					print 'Expired, Updating cache' , params , self.id
					client = helper.rpc_client()
					reply = json.loads(client.call('GET ROUTE' + json.dumps({'start':self.id,'end':params['end']})))
					self.forwardingCache.update({reply['next']:{'time':time.time(),'server_ip':reply['server_ip'],'server_port':reply['server_port']}})
			else :
				print 'Not in cache' , params , self.id
				client = helper.rpc_client()
				reply = json.loads(client.call('GET ROUTE' + json.dumps({'start':self.id,'end':params['end']})))
				self.forwardingCache.update({params['next']:{'time':time.time(),'server_ip':reply['server_ip'],'server_port':reply['server_port']}})
			self.transferingFd.sendto('FWRD DATA' + json.dumps({'start':params['start'],'next':reply['next'],'end':params['end']}) + segment,(reply['server_ip'],reply['server_port']))		
			ack_Data , ack_addr = self.transferingFd.recvfrom(MAX_SIZE)			

	def runNodeServer(self) :
		# self.server.listen(100)
		# inputFd = [ self.server ]
		# outputFd = []
		# while True :
		# 	readFd , writeFd , expFd = select.select(inputFd,outputFd,[])
		# 	for fd in readFd :
		# 		if fd is self.server :
		# 			client , client_addr = self.server.accept()
		# 			inputFd.append(client)
		# 		else :
		while True :
			data , addr = self.server.recvfrom(MAX_SIZE)			
			if len(data) > 9 :
				header = data[:9]
				if header == 'FWRD DATA' :
					array = data[9:].split('!_$@damei@$_!')
					params = json.loads(array[0])
					segment = ''.join([ '!_$@damei@$_!' + item for item in array[1:]])
					if params['end'] == self.id :
						identifier = array[3]
						packets_seq = array[2]
						file_name = array[1]
						try :
							actual_fragment = array[4]
						except IndexError :
							pass
						if packets_seq ==  'ENDENDEND' :
							dataArray = sorted(self.incompleteData[identifier]['data'],key=itemgetter(0))
							file_name = self.incompleteData[identifier]['name']
							print 'Done for file : ' + file_name
							with open('output.mp3','wb') as f :
									f.write(''.join([ item[1] for item in dataArray]))
							self.incompleteData.pop(identifier)
						else :							
							if identifier not in self.incompleteData :
								self.incompleteData.update({identifier:{'data':[],'name':file_name}})
							else :
								self.incompleteData[identifier]['data'].append([int(packets_seq),actual_fragment])					
					else :						
						self.forwardingQueue.put({'params':params,'segment':segment})
			self.server.sendto('ack',addr)

nodes = [ Node(i+1) for i in range(conf['nodes'])]
time.sleep(2)
nodes[2].transferData(13,'data.mp3')

while True :
	time.sleep(1)