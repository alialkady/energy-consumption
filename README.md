# energy-consumption

## Table of content
1. [Project overview](#Project-overview)
2. [Technologies Used](#Technologies-Used)
3. [ETL Process](#ETL-process)
4. [Setup and Installation](#Setup-and-Installation)

## Project overview
**Energy consumption** is focused on realtime proccessing data for energy usage in homes during the year..
It's categorized according to different machines in the home.

## Technologies used
- **Docker** to containerize the project with eazy deploymnet.
- **SQL server** To load data in permanent storage.
- **Kafka** to hangle realtime data and streaming process.
- **Spark streaming** to consume the data from kafka topic and load it to SQL server.

## ETL Process

![Image](kafkapipeline.jpg)

## Setup and Installation

### Prerequisites:
- Docker installed
- Docker-compose installed

### Commands
1- Clone repo
```
https://github.com/alialkady/energy-consumption.git
```
2- Initialize docker
```
docker-compose up -d
```
3- Access to spark container and open webUI

4- Upload all files in spark container

5- access command line in spark container write this command
```
pip install kafka-python
```
6- use SQL Client with these info
- Host: 127.0.0.1 or localhost
- Port: 3307
- User: root
- Password: 123456

7- Create SQL table using schema.sql file

8- run produce_kafka.py and spark_consumer.py together

**Congratulations it works**
