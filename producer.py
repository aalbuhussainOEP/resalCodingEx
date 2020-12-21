# to install kafka and run the kafka server please follow https://kafka.apache.org/quickstart
from confluent_kafka import Producer
import json
from pathlib import Path

def acked(err, msg):
	'''
	this function is to make the tracing of the producer easier by showing what message was 
	send either successfully or not 
	'''
	if err is not None:
		print("Failed to deliver message: {0}: {1}"
			.format(msg.value(), err.str()))
	else:
		print("Message produced: {0}".format(msg.value()))

def create_producer():
	'''
	This is standard function to create a simple kafka producer.

	Return:

	p -- the created kafka producer.
	'''
	conf = {'bootstrap.servers': 'localhost:9092'}
	p = Producer(conf)
	return p

def stream_csv(file):
	'''
	the main functionality of the producer is implemented here. The protocol is going to start with a 
	control json message in the following format
	{'type' : 'senteian-start', 'file-name', <name of the file streamed>} <= this message signles to the consumer that a new file is being streamed, followed by
	{'type' : 'line', 'content', <current-line>, file-name', <name of the file streamed>} line by line then anoter control message to signle the end of the stream as follows:
	{'type' : 'senteian-stop', 'file-name', '<name of the file streamed'}

	Arguments: 

	file -- path for the file the user wants to stream
	'''
	try:
		prod = create_producer()
		file_name_to_stream = file.split('/')[-1] # need to extract file name
		sentinel_start_messge = {'type': 'sentinel-start', 'file-name': file_name_to_stream}
		start_message_json = json.dumps(sentinel_start_messge)
		prod.produce('mytopic', start_message_json, callback=acked)
		with open(file) as f:
			for line in f:
				l = {'type': 'line', 'content': line, 'file-name': file_name_to_stream}
				line_json = json.dumps(l)
				prod.produce('mytopic', line_json,callback=acked)
		sentinel_stop_messge = {'type': 'sentinel-stop', 'file-name': file_name_to_stream}
		stop_message_json = json.dumps(sentinel_stop_messge)
		prod.produce('mytopic', stop_message_json, callback=acked)
	except KeyboardInterrupt:
		pass
	prod.flush(30)

filepath = input('Please Enter the csv file path: ')
stream_csv(filepath)