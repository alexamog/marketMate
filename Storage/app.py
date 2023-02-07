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

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# TODO: create connection string, replacing placeholders below with variables defined in log_conf.yml
DB_ENGINE = create_engine(f"mysql+pymysql://{app_config['user']}:{app_config['password']}@{app_config['hostname']}:{app_config['port']}/{app_config['db']}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

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
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8090)