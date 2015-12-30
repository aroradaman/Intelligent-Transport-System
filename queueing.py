import pika
import helper
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='rpc_queue')

def process(data):
	header = data[:9]
	segment = data[9:]
	if header == 'GET ROUTE' :
		print header+ '\t' + segment
		params = json.loads(segment)
		nextHop = helper.getRoute(params)
		return nextHop
	elif header == 'SRV UPDTE' :
		print header+ '\t' + segment
		params = json.loads(segment)
		helper.servAddrUpdate(params)		
		return 'done'

	elif header == 'GPS UPDTE' :
		#print header+ '\t' + segment
		params = json.loads(segment)
		helper.gpsUpdate(params)
		return 'done'

def on_request(ch, method, props, body):
	n = body
	response = process(n)
	ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = props.correlation_id), body=str(response))
	ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='rpc_queue')

channel.start_consuming()