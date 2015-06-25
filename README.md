Kafka Smart-Grid event Producer
======================

Consume a Smart-Grid data set and send all messages to a Kafka broker.
http://www.cse.iitb.ac.in/debs2014/?page_id=42

Data file is located in data/sorted100M.csv
Configuration
-------------

Set up your kafka brokers in ``conf/producer.conf``


Execution
-------------
just run ``gradlew run -Pargs="PATH_TO/producer.conf"``
 
