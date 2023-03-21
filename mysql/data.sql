
CREATE DATABASE events;

FLUSH PRIVILEGES;

USE events;

CREATE TABLE buy(
    id INT NOT NULL AUTO_INCREMENT,
    buy_id VARCHAR(250) NOT NULL,
    item_name VARCHAR(250) NOT NULL,
    item_price FLOAT NOT NULL,
    buy_qty INT NOT NULL,
    trace_id VARCHAR(100) NOT NULL,
    date_created VARCHAR(100) NOT NULL, 
    PRIMARY KEY (id)
);

CREATE TABLE sell(
    id INT NOT NULL AUTO_INCREMENT,
    sell_id VARCHAR(250) NOT NULL,
    item_name VARCHAR(250) NOT NULL,
    item_price FLOAT NOT NULL,
    sell_qty INT NOT NULL,
    trace_id VARCHAR(100) NOT NULL,
    date_created VARCHAR(100) NOT NULL, 
    PRIMARY KEY (id)
);

CREATE USER 'events-user' @'%' IDENTIFIED BY '123';

GRANT ALL PRIVILEGES ON events.* TO 'events-user' @'%' WITH GRANT OPTION;

FLUSH PRIVILEGES;