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

from pykafka import KafkaClient
from pykafka.common import OffsetType

from threading import Thread

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DB_ENGINE = create_engine(
    f"mysql+pymysql://{app_config['user']}:{app_config['password']}@{app_config['hostname']}:{app_config['port']}/{app_config['db']}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)


def process_messages():
    host = app_config["events"]["hostname"]
    port = app_config["events"]["port"]
    client = KafkaClient(hosts=f"{host}:{port}")

    topic_name = app_config["events"]["topic"]
    topic = client.topics[topic_name]

    messages = topic.get_simple_consumer(
        reset_offset_on_start=False,
        auto_offset_reset=OffsetType.LATEST)
    for msg in messages:
        msg_str = msg.value.decode("utf-8")
        msg = json.loads(msg_str)
        msg_type = msg["type"]
        session = DB_SESSION()
        logger.info(f"CONSUMER::storing {msg_type} event")
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
    messages.commit_offsets()
# Endpoints


def buy(body):
    session = DB_SESSION()

    buy_obj = Buy(
        buy_id=body['buy_id'],
        item_name=body['item_name'],
        item_price=body['item_price'],
        buy_qty=body['buy_qty'],
        trace_id=body['trace_id']
    )

    session.add(buy_obj)
    session.commit()
    session.close()

    logger.debug('Stored buy event with trace id' + body['trace_id'])

    return NoContent, 201


def get_buys(timestamp):
    session = DB_SESSION()
    rows = session.query(Buy).filter(Buy.date_created >= timestamp)
    data = []
    for row in rows:
        print(row.to_dict())
        data.append(row.to_dict())
    session.close()
    logger.info(f" GET request: get_buys - timestamp:{timestamp}")

    return data, 200


def sell(body):
    session = DB_SESSION()

    sell_obj = Sell(
        sell_id=body['sell_id'],
        item_name=body['item_name'],
        item_price=body['item_price'],
        sell_qty=body['sell_qty'],
        trace_id=body['trace_id']
    )

    session.add(sell_obj)
    session.commit()
    session.close()
    logger.debug('Stored sell event with trace id' + body['trace_id'])


def health():
    return NoContent, 200


def get_sells(timestamp):
    session = DB_SESSION()
    rows = session.query(Sell).filter(Sell.date_created > timestamp)
    data = []
    for row in rows:
        data.append(row.to_dict())
    session.close()
    logger.info(f" GET request: get_sells - timestamp:{timestamp}")

    return data, 200


with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')


app = connexion.FlaskApp(__name__, specification_dir='')
# app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)
app.add_api("openapi.yaml", base_path="/storage",
            strict_validation=True, validate_responses=True)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    tl = Thread(target=process_messages)
    tl.daemon = True
    tl.start()
    app.run(port=8090)
