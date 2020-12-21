from confluent_kafka import Consumer, KafkaError
import csvtojsonapi as top_rating_products
import ast
import os
import json
from pathlib import Path

def creat_consumer():
    '''
    Standard kafka consumer creation function

    Return
    c -- the created consumer based on the seetings
    '''
    settings = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
    }
    c = Consumer(settings)
    return c


def process_message(message, f, to_process_file_name):
    '''
    Processing messages consumed from the kafka queue and following the protocol specificed in the producer.py.
    If we get a sentinel-start we print that we have a new file to process.
    if we get a line type message that mean we have a file content and we append it to a temp file.
    if we get a sentinel-stop message then we end the processing by computing the top rated products and we print the json results.

    Arguments:
    messgae --  messgae read on the kafka queue
    f -- file object
    to_process_file_name -- the name of the temp file where we store the file contents  
    '''
    if message['type'] == 'sentinel-start':
        print('Processing file : {}'.format(message['file-name']))
    elif message['type'] == 'sentinel-stop':
        print('Done processing file : {}'.format(message['file-name']))
        current_file_content_path = Path(__file__).parent / "./{}".format(to_process_file_name)
        try:
            result = top_rating_products.find_best_rating_products(csv_file = current_file_content_path)
            to_return = {"top_product(s)" : result["product_name"].to_string(), "product_rating(s)" : result['customer_avrage_rating'].to_string()}
            json_to_return = json.dumps(to_return, indent = 4) 
            print(json_to_return)
        except BaseException as e:
            print({"Error": "problem reading/opening/processing file {}".format(to_process_file_name), "Exception type": type(e).__name__, "Exception args": e.args})
        f.close()
        os.remove(current_file_content_path)
    elif message['type'] == 'line':
        f.write(message['content']) 


def main():
    '''
    The main funtion where the main loop is running to continuously consume messages of the kafka queue
    '''
    consumer = creat_consumer()
    consumer.subscribe(['mytopic'])
    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                msg_str = msg.value().decode("UTF-8")
                message = ast.literal_eval(msg_str)
                file_name = 'current-file.csv'
                f = open(file_name, "a")
                process_message(message, f, file_name)
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached {0}/{1}'
                      .format(msg.topic(), msg.partition()))
            else:
                print('Error occured: {0}'.format(msg.error().str()))

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

main()