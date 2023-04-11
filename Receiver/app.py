import connexion
from connexion import NoContent
import datetime
import json
import logging
import logging.config
from pykafka import KafkaClient
import uuid
import yaml


def process_event(event, endpoint):
    trace_id = str(uuid.uuid4())
    event['trace_id'] = trace_id

    logger.debug(f'Received {endpoint} event with trace id {trace_id}')
    host = app_config["events"]["hostname"]
    port = app_config["events"]["port"]
    client = KafkaClient(hosts=f"{host}:{port}")
    topic_name = app_config["events"]["topic"]
    topic = client.topics[topic_name]

    producer = topic.get_sync_producer()

    myDict = {
        "type": endpoint,
        "datetime": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "payload": event
    }
    my_dict_str = json.dumps(myDict)

    producer.produce(my_dict_str.encode("utf-8"))
    logging.info(f"PRODUCER::producting {event} ")
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
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)
# app.add_api("openapi.yml", base_path="/receiver",
#             strict_validation=True, validate_responses=True)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8080)
