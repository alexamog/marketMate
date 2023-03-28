import connexion
from connexion import NoContent
import datetime
import json
import logging
import logging.config
import pykafka
from pykafka import KafkaClient
import requests
import uuid
import yaml 
from pykafka import topic
# def process_event(event, endpoint):
#     trace_id = str(uuid.uuid4())
#     event['trace_id'] = trace_id

#     # TODO: call logger.debug and pass in message "Received event <type> with trace id <trace_id>"
#     logger.debug(f"received event {endpoint} with trace id {trace_id}")

#     h = { 'Content-Type': 'application/json' }
#     print(event)
#     print(endpoint)
#     res = requests.post(app_config[f'{endpoint}']['url'], headers = h, data = json.dumps(event))

#     # TODO: call logger.debug and pass in message "Received response with trace id <trace_id>, status code <status_code>"
#     logger.debug(f'Received response with trace id {trace_id}, status code {res.status_code}')
    
#     return res.text, res.status_code

def process_event(event, endpoint):
    trace_id = str(uuid.uuid4())
    event['trace_id'] = trace_id

    logger.debug(f'Received {endpoint} event with trace id {trace_id}')
    host = app_config["events"]["hostname"]
    port = app_config["events"]["port"]
    # TODO: create KafkaClient object assigning hostname and port from app_config to named parameter "hosts"

    # and store it in a variable named 'client'
    # client = topic.

    client = KafkaClient(hosts=f"{host}:{port}")

    # TODO: index into the client.topics array using topic from app_config
    # and store it in a variable named topic
    topic_name = app_config["events"]["topic"]
    topic = client.topics[topic_name]
    
    # TODO: call get_sync_producer() on your topic variable
    # and store the return value in variable named producer


    producer = topic.get_sync_producer()

    # TODO: create a dictionary with three properties:
    myDict = {
        "type": endpoint,
        "datetime": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "payload": event
    }
    # type (equal to the event type, i.e. endpoint param)
    # datetime (equal to the current time, formatted as per our usual format)
    # payload (equal to the entire event passed into the function)

    # TODO: convert the dictionary to a json string
    my_dict_str = json.dumps(myDict)

    # TODO: call the produce() method of your producer variable and pass in your json string
    # note: encode the json string as utf-8
    producer.produce(my_dict_str.encode("utf-8"))
    # TODO: log "PRODUCER::producing x event" where x is the actual event type
    logging.info(f"PRODUCER::producting {event} ")
    # TODO: log the json string
    logging.info(my_dict_str)

    return NoContent, 201

# Endpoints
def buy(body):
    process_event(body, 'buy')
    return NoContent, 201

def sell(body):
    process_event(body, 'sell')
    return NoContent, 201

def health():
    return NoContent, 200

app = connexion.FlaskApp(__name__, specification_dir='')
# app.add_api("openapi.yml", strict_validation=True, validate_responses=True)
app.add_api("openapi.yml", base_path="/receiver", strict_validation=True, validate_responses=True)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8080)