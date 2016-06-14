# LogstashKafkaSparkStreamingMlibTest

==========================

Quick Set-Up Guide To Test The Application

Note: You need Java, Scala and Gradle to run this application.

# Setting up ELK 

This guide is a good introduction to setting up ELK and have some examples for each of the different components in ELK 
if you are new to them:
	-[Complete guide to ELK by logz.io](http://logz.io/learn/complete-guide-elk-stack/)
	
For Mac Users, an easy way to set them up is using brew package manager to install Elasticsearch, Logstash and Kibana.

Install brew package manager following the steps on the main website: [Brew](http://brew.sh/)

Then run the commands (should be obvious what is being installed) :

	```brew install elasticsearch```
	```brew install logstash```
	```brew install kibana```

Alternatively, you can download them from their respective websites.
	-[Elasticsearch] (https://www.elastic.co/downloads/elasticsearch)
	-[Logstash] (https://www.elastic.co/downloads/logstash)
	-[Kibana] (https://www.elastic.co/downloads/kibana)
	
# Setting up Kafka
Kafka is a distributed, partitioned, replicated commit log service. It provides the functionality of a messaging 
system, but with a unique design.

For Mac Users, you can use ```brew install kafka``` to easily install Kafka. However, from my experience, the kafka formula on 
brew only contains core kafka and does not have kafka connect when I was setting it up.

Alternatively, you can install download it from its main website
	-[Kafka] (https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz)

First bootstrap and download the wrapper.

```
cd kafka_source_dir
gradle
```

Then, build the jar and running it

```./gradlew jar```  

A quick start on Kafka and to test your Kafka installation:
http://kafka.apache.org/documentation.html#quickstart




	
