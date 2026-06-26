This project is based on Apache Kafka Series - Learn Apache Kafka for Beginners v3
https://learning.oreilly.com/course/apache-kafka-series/9781789342604/

Conduktor Kafka Quick Start
https://docs.conduktor.io/guide

This project contains code to talk to the Kafka cluster. See details below:

To Start the local instance of Kafka:

First Format the Kafka Directory
dthomas43@L128936 ~ % ~/Downloads/kafka_2.12-3.4.0/bin/kafka-storage.sh format -t 669YoMQoS-aPeugp-6E1mw -c ~/Downloads/kafka_2.12-3.4.0/config/kraft/server.properties

Then Start the Kafka Server
dthomas43@L128936 ~ % ~/Downloads/kafka_2.12-3.4.0/bin/kafka-server-start.sh ~/Downloads/kafka_2.12-3.4.0/config/kraft/server.properties

Then Start the Conduktor Kafka Quick Start
From Command Line, to Start the local instance of Kafka in Docker, and Conduktor
dthomas43@L128936 ~ % curl -fL https://releases.conduktor.io/quick-start | docker compose -f - up 

Sign in to the Conduktor UI:
http://localhost:8080/console/local-kafka/topics/demo_java?tab=consume

    
