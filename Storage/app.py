import connexion
from connexion import NoContent
import yaml
import logging
import logging.config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from buy import Buy
from sell import Sell

import json
import pykafka
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka import topic

import threading
from threading import Thread

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# TODO: create connection string, replacing placeholders below with variables defined in log_conf.yml
DB_ENGINE = create_engine(f"mysql+pymysql://{app_config['user']}:{app_config['password']}@{app_config['hostname']}:{app_config['port']}/{app_config['db']}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def process_messages():
    # TODO: create KafkaClient object assigning hostname and port from app_config to named parameter "hosts"
    # and store it in a variable named 'client'
    host = app_config["events"]["hostname"]
    port = app_config["events"]["port"]
    client = KafkaClient(hosts=f"{host}:{port}")
    
    # TODO: index into the client.topics array using topic from app_config
    # and store it in a variable named topic
    topic_name = app_config["events"]["topic"]
    topic = client.topics[topic_name]
    # Notes:
    #
    # An 'offset' in Kafka is a number indicating the last record a consumer has read,
    # so that it does not re-read events in the topic
    #my_dict_str
    # When creating a consumer object,
    # reset_offset_on_start = False ensures that for any *existing* topics we will read the latest events
    # auto_offset_reset = OffsetType.LATEST ensures that for any *new* topic we will also only read the latest events
    
    messages = topic.get_simple_consumer( 
        reset_offset_on_start = False, 
        auto_offset_reset = OffsetType.LATEST)
    for msg in messages:
        # This blocks, waiting for any new events to arrive
        # TODO: decode (utf-8) the value property of the message, store in a variable named msg_str
        msg_str = msg.value.decode("utf-8") 
        # TODO: convert the json string (msg_str) to an object, store in a variable named msg
        msg = json.loads(msg_str)
        # TODO: extract the payload property from the msg object, store in a variable named payload
        # TODO: extract the type property from the msg object, store in a variable named msg_type
        msg_type = msg["type"]
        # TODO: create a database session
        session = DB_SESSION()
        # TODO: log "CONSUMER::storing buy event"
        # TODO: log the msg object
        logger.info(f"CONSUMER::storing {msg_type} event")
        # TODO: if msg_type equals 'buy', create a Buy object and pass the properties in payload to the constructor
        # if msg_type equals sell, create a Sell object and pass the properties in payload to the constructor
        if msg_type == "buy":
            buy_obj = Buy(
                buy_id=msg['payload']['buy_id'],
                item_name=msg['payload']['item_name'],
                item_price=msg['payload']['item_price'],
                buy_qty=msg['payload']['buy_qty'],
                trace_id=msg['payload']['trace_id'],
            )
            session.add(buy_obj)
            session.commit()
        
        if msg_type == "sell":
            sell_obj = Sell(
                sell_id=msg['payload']['sell_id'],
                item_name=msg['payload']['item_name'],
                item_price=msg['payload']['item_price'],
                sell_qty=msg['payload']['sell_qty'],
                trace_id=msg['payload']['trace_id'],
            )
            session.add(sell_obj)
            session.commit()
        # TODO: session.add the object you created in the previous step
        # TODO: commit the session

    # TODO: call messages.commit_offsets() to store the new read position
    messages.commit_offsets()
# Endpoints
def buy(body):
    # TODO create a session
    session = DB_SESSION()

    # TODO additionally pass trace_id (along with properties from Lab 2) into Buy constructor
    buy_obj = Buy(
        buy_id=body['buy_id'],
        item_name=body['item_name'],
        item_price=body['item_price'],
        buy_qty=body['buy_qty'],
        trace_id=body['trace_id']
    )

    # TODO add, commit, and close the session

    session.add(buy_obj)
    session.commit()
    session.close()

    # TODO: call logger.debug and pass in message "Stored buy event with trace id <trace_id>"

    logger.debug('Stored buy event with trace id' + body['trace_id'])

    return NoContent, 201


# end

def get_buys(timestamp):
    # TODO create a DB SESSION
    session = DB_SESSION()
    # TODO query the session and filter by Buy.date_created >= timestamp
    # e.g. rows = session.query(Buy).filter etc...
    rows = session.query(Buy).filter(Buy.date_created >= timestamp)
    # TODO create a list to hold dictionary representations of the rows
    # e.g. data = []
    data = []
    # TODO loop through the rows, appending each row (use .to_dict() on each row) to 'data'
    for row in rows:
        print(row.to_dict())
        data.append(row.to_dict())
    # TODO close the session
    session.close()
    # TODO log the request to get_buys including the timestamp and number of results returned
    logger.info(f" GET request: get_buys - timestamp:{timestamp}")

    return data, 200

def sell(body):
    # TODO create a session
    session = DB_SESSION()

    sell_obj = Sell(
        sell_id=body['sell_id'],
        item_name=body['item_name'],
        item_price=body['item_price'],
        sell_qty=body['sell_qty'],
        trace_id=body['trace_id']
    )

    # TODO add, commit, and close the session

    session.add(sell_obj)
    session.commit()
    session.close()

    # TODO: call logger.debug and pass in message "Stored buy event with trace id <trace_id>"
    logger.debug('Stored sell event with trace id' + body['trace_id'])


    # return NoContent, 201
    pass
# end

def get_sells(timestamp):
 # TODO create a DB SESSION
    session = DB_SESSION()
    # TODO query the session and filter by Sell.date_created >= timestamp
    # e.g. rows = session.query(Sell).filter etc...
    rows = session.query(Sell).filter(Sell.date_created > timestamp)
    # TODO create a list to hold dictionary representations of the rows
    # e.g. data = []
    data = []
    # TODO loop through the rows, appending each row (use .to_dict() on each row) to 'data'
    for row in rows:
        data.append(row.to_dict())
    # TODO close the session
    session.close()
    # TODO log the request to sells including the timestamp and number of results returned
    logger.info(f" GET request: get_sells - timestamp:{timestamp}")

    return data, 200
with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')


app = connexion.FlaskApp(__name__, specification_dir='')
# app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)
app.add_api("openapi.yml", base_path="/storage", strict_validation=True, validate_responses=True)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    tl = Thread(target=process_messages)
    tl.daemon = True
    tl.start()
    app.run(port=8090)