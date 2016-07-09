
# LogstashKafkaSparkStreamingMlibTest

Note: You need Java 7 or higher, Scala 2 or higher and Gradle 2.0 or higher to run this application.

# Setting up ELK

This guide is a good introduction to setting up ELK and have some examples for each of the different components in ELK
if you are new to them:
* [Complete guide to ELK by logz.io](http://logz.io/learn/complete-guide-elk-stack/)

For Mac Users, an easy way to set them up is using brew package manager to install Elasticsearch, Logstash and Kibana.

Install brew package manager following the steps on the main website: [Brew](http://brew.sh/)

Then run the commands (should be obvious what is being installed) :

```
brew install elasticsearch
brew install logstash
brew install kibana
```

Alternatively, you can download them from their respective websites.
* [Elasticsearch] (https://www.elastic.co/downloads/elasticsearch)
* [Logstash] (https://www.elastic.co/downloads/logstash)
* [Kibana] (https://www.elastic.co/downloads/kibana)

# Setting up Kafka
Kafka is a distributed, partitioned, replicated commit log service. It provides the functionality of a messaging
system, but with a unique design.

For Mac Users, you can use ```brew install kafka``` to easily install Kafka. However, from my experience, the kafka formula on
brew only contains core kafka and does not have kafka connect when I was setting it up.

Alternatively, you can download it from its main website
* [Kafka] (https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz)

First bootstrap and download the wrapper.

```
cd kafka_source_dir
gradle
```

Then, build the jar and running it

```./gradlew jar```  

A quick start on Kafka and to test your Kafka installation:
http://kafka.apache.org/documentation.html#quickstart

# Setting up Spark

For Mac Users, you can use ```brew install apache-spark``` to easily install Spark.

Alternatively, you can download it from:
* [Spark] (http://spark.apache.org/downloads.html)

Quick start on Spark and to test your Spark installation:
http://spark.apache.org/docs/latest/quick-start.html

# Application-specific settings

Now on to a couple of configuration bits before you can make use of the application.

First, you need to set up a logstash configuration file that ingests log file data from a directory of your choosing into Kafka (the rest is then connected with Kafka and Spark Streaming). You can put this configuration anywhere you like but I personally put it under ```/etc/logstash/conf.d```

```
#Local file to elasticsearch

input {
    file {
        path => [ log_source_dir ]
        start_position => beginning
        ignore_older => 0
        type => "apache-access"
    }
}

filter {
  grok {
    match => { "message" => "%{COMBINEDAPACHELOG}" }
  }

  geoip {
    source => "clientip"
  }

}

output {
  kafka { topic_id => "logstash_logs" }
  stdout { }
}
```

Next, you need to start up Kafka as usual:

Kafka uses ZooKeeper so you need to first start a ZooKeeper server if you don't already have one. You can use the convenience script packaged with kafka to get a quick-and-dirty single-node ZooKeeper instance.

```
> bin/zookeeper-server-start.sh config/zookeeper.properties
[2013-04-22 15:01:37,495] INFO Reading configuration from: config/zookeeper.properties (org.apache.zookeeper.server.quorum.QuorumPeerConfig)
...
Now start the Kafka server:
> bin/kafka-server-start.sh config/server.properties
[2013-04-22 15:01:47,028] INFO Verifying properties (kafka.utils.VerifiableProperties)
[2013-04-22 15:01:47,051] INFO Property socket.send.buffer.bytes is overridden to 1048576 (kafka.utils.VerifiableProperties)
...
```

Then create a topic that your Kafka consumer will listen on (I used ```logstash_logs``` in my case):
```> bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic logstash_logs```

Then, running the script below to start logstash with the configuration above:
```logstash -f /etc/logstash/conf.d/file_to_kafka.conf --auto-reload```

Sample apache server log file data (Ones that I am using while developing this app):
* https://download.elastic.co/demos/logstash/gettingstarted/logstash-tutorial.log.gz
* redlug.com/logs/access.log

You can test whether your configuration is working by running Kafka in the CLI and then running logstash with the configuration above. Logstash should start processing if a file that is specified in the logstash input is created or lines are appended to it (Note: You can delete the file and copy it into the folder that logstash is pointing at to trigger processing logstash to the application). You should then see outputs on your Kafka consumer that is listening on the Kafka topic specified in the logtash output in the config file which in the case of the code snippet above is called ```logstash_logs```.

When you have the application running, the Kafka consumer in the application will consume the topic instead of a consumer that is running in the CLI.

Continuing on, you need the maven dependencies below for this application. (Shouldn't be a problem if you are cloning or forking this repo with the pom.xml. If you do encounter any problem, please do tell. :D )

```
<dependencies>
	<dependency>
	    <groupId>org.apache.spark</groupId>
	    <artifactId>spark-mllib_2.10</artifactId>
	    <version>1.3.0</version>
	</dependency>
	<dependency>
	    <groupId>org.apache.kafka</groupId>
	    <artifactId>kafka-clients</artifactId>
	    <version>0.10.0.0</version>
	</dependency>
	<dependency>
	    <groupId>org.apache.spark</groupId>
	    <artifactId>spark-streaming-kafka_2.10</artifactId>
	    <version>1.6.1</version>
	</dependency>
<dependencies>
```

Additional Notes:
- You can set up your IDE (IntelliJ or Eclipse) to run a Spark application locally inside the IDE without packaging a uber jar:
	* https://cwiki.apache.org/confluence/display/SPARK/Useful+Developer+Tools#UsefulDeveloperTools-IDESetup
