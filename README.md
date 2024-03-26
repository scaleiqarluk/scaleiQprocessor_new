ScaleiQ Processor for Kafka and ScaleiQ Query Index

The ScaleiQ Java Processor (kafka consumer) will automatically process KafkaStreams and send it to an IndexStore, ScaleiQQIndex, OpenSearch, Solr, elasticSearch.

Requirements 1- Java 11->19 . The code has been compiled with GraalVM for Polyglot extensions Python-Ruby-(in development)

2- The package has been coded to talk to ScaleiQ Ingestor (Kafka-Broker) with best practice settings.

3- The two packages requires a KafkaStream Backend Server running on localhost, and IndexStore, such as ScaleiQQIndex, OpenSearch, ElasticSearch.

4- For demo purposes ( the config has been hardcode with a URL(wikimedia) for testing and development

5-If the jar files will be running as SystemD Process, the config file is attached. The systemd files, are for the two modules ScaleiQ Ingestor and ScaleiQ Processor, a separate Systemd file is required for Kafka Systemd.

6-Run the jar files, in order , first the Ingestor then the Processor, or thru Systemd , 'sudo systemctl start ingestor' 'sudo systemctl start processor.

7-To see the output, you can run tail -f /var/log/syslog | grep processor, or send it to the Index dashboard, grafana etc.

8-To Stop the pakages, 'sudo systemctl stop ingestor', sudo systemctl stop processor'

# Java Trust store path
$JAVA_HOME/lib/security/cacerts

Please set the env variable JAVA_HOME if not set, and add the certificates to the Java trustStore
Keystore password is set to 'scaleiq'.