import connexion
from connexion import NoContent
import datetime
import logging
import logging.config
import requests
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from stats import Stats
from flask_cors import CORS
DB_ENGINE = create_engine("sqlite:///stats.sqlite")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)


def get_latest_stats():
    session = DB_SESSION()
    record = session.query(Stats).order_by(Stats.last_updated.desc()).first()
    if record:
        return record.to_dict()
    return NoContent, 201


def health():
    return NoContent, 200


def populate_stats():
    time_stamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    last_updated = time_stamp
    logging.info("Started populate_stats")
    session = DB_SESSION()

    result = session.query(Stats).order_by(Stats.last_updated.desc()).all()
    if result:
        result = result[-1].to_dict()
        last_updated = result["last_updated"]
    else:
        result = {'max_buy_price': 0.0, 'num_buys': 0,
                  'max_sell_price': 0.0, 'num_sells': 0, 'last_updated': None}
    buy_endpoint = app_config["buy_url"]
    rows = requests.get(f"{buy_endpoint}?timestamp={last_updated}")

    payload = rows.json()
    max_buy_price = 0.00
    max_sell_price = 0.00
    total_buys = 0
    total_sales = 0
    for row in payload:
        if row['item_price'] > max_buy_price:
            max_buy_price = row['item_price']
        total_buys += row["buy_qty"]

    sell_endpoint = app_config["sell_url"]
    rows_sell = requests.get(f"{sell_endpoint}?timestamp={last_updated}")
    payload_sells = rows_sell.json()
    for row in payload_sells:
        if row['item_price'] > max_sell_price:
            max_sell_price = row['item_price']
        total_sales += row["sell_qty"]
    stat_obj = Stats(max_buy_price, total_buys,
                     max_sell_price, total_sales, time_stamp)
    session.add(stat_obj)
    session.commit()
    session.close()
    return NoContent, 201


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['period'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)
# app.add_api("openapi.yml", base_path="/processing",
#             strict_validation=True, validate_responses=True)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')
CORS(app.app)
if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, use_reloader=False)
