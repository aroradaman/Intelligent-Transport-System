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

connection = neo4j.connect(conf['db_address'])
cursor = connection.cursor()


def clear() :
	cursor.execute('MATCH n-[rel:IS_CONNECTED]->r DELETE rel')
	connection.commit()
	cursor.execute("MATCH (n:Node) DELETE n")
	connection.commit()

if reAssign :
	clear()

def obj_func( delay , hc , reliability ) :
	return x_reliability * (  reliability / reliability_constant) + x_delay * (  delay_constant / delay ) + x_hc * (  hc_constant / hc )

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

nodes =  [ {'id' : i + 1 , 'velocity' : random.randrange(60,100), 'x' : 100 * i +  random.randrange(100) , 'y' : random.randrange(-100,100) } for i in range(conf['nodes']) ]

for node in nodes :
	if reAssign :
		cursor.execute("MERGE (n:Node { type:'node',id:{id}, velocity:{velocity}, x:{x}, y:{y} })", **node)
		connection.commit()
		print node

estab = []

for outer in nodes :
	if reAssign :
		for inner in nodes :
			go = True
			if outer == inner :
				pass
			else :
				if inner['id'] < outer['id'] :
					if str(inner['id']) + str(outer['id']) in estab :
						go = False
				else :
					if str(outer['id']) + str(inner['id']) in estab :
						go = False
				if go :
					dist = math.sqrt( ( inner['x'] - outer['x']) **2 + (inner['y'] - outer['y']) **2 )
				if dist < radio_radius :
					angle,reliability,delay,hc = link_parameters(inner,outer,dist)
					if inner['x'] > outer['x'] :
						cursor.execute('MATCH (n:Node {id:'+str(inner['id'])+'}), (r:Node {id:'+str(outer['id'])+'}) CREATE UNIQUE (r)-[:IS_CONNECTED { type:"edge", reliability:' + str(reliability)+ ', delay:"' + str(delay) + '" , to:' + str(inner['id']) +' ,hc:1 , angle:' + str(angle) + ' , from:'+ str(outer['id']) +'}]->(n)')
						connection.commit()
					else :
						cursor.execute('MATCH (n:Node {id:'+str(inner['id'])+'}), (r:Node {id:'+str(outer['id'])+'}) CREATE UNIQUE (n)-[:IS_CONNECTED { reliability:' + str(reliability)+ ', delay:"' + str(delay) + '" , from:' + str(inner['id']) +' ,hc:1 , angle:' + str(angle) + ' , to:'+ str(outer['id']) +'}]-(r)')					
						connection.commit()
					if inner['id'] < outer['id'] :
						estab.append(str(inner['id']) + str(outer['id']))
					else :
						estab.append(str(outer['id']) + str(inner['id']))

node1 = raw_input("Please enter id for node1 : ")
node2 = raw_input("Please enter id for node2 : ")
print 
query = "MATCH p = (:Node{id:" + node1 + "})-[r:IS_CONNECTED*1.."+str(conf['hc_constant'])+"]->(:Node{id:" + node2 + "}) RETURN DISTINCT p"


print query
print 

qosMatrix = []

maxQoS = 0
max_index = 0
minQoS = 100000000000000
min_index = 0
count = 0
Route = []

for r in cursor.execute(query) :
	delay = 0
	reliability = 1
	hc = 0
	temp = []
	for link in r[0]:
		if len(link.keys()) == 4 :
			temp.append(link['id'])		
		elif len(link.keys()) == 6 :
			temp.append(link['to'])
			delay += float(link['delay'])
			reliability *= link['reliability']
			hc += link['hc']
	temp = sorted(list(set(temp)))
	data = str(temp)
	Route.append(temp)
	obj_val = obj_func( delay , hc , reliability )
	print data ,obj_val
	qosMatrix.append([ delay , hc , reliability , obj_val ])
	if obj_val > maxQoS :
		maxQoS = obj_val
		max_index = count
	if obj_val < minQoS :
		minQoS = obj_val
		min_index = count
	count += 1
print
print 'Optimal Array in QoS matrix : ',qosMatrix[max_index]
print
print 'Optimal Route ' , Route[max_index]
print

print query