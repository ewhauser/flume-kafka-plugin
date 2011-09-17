# Flume plugin for Kafka

Fair warning - this is an experiment and is not used anywhere in production.

## Sink

To use the KafkaSink, add the following to your flume-site.xml file:

```xml
<property>
  <name>flume.plugin.classes</name>
  <value>org.apache.flume.kafka.KafkaSink</value>
</property>
```

To configure the sink:

```bash
flume shell -c localhost:35873 -e exec config localhost 'tail("/var/log/messages")' 'kafka("localhost:2181", "topic")'
```

If you want your events to go to a particular partition, you can set a property on the event called "kafka.partition.key" to provide a key for the Kafka client to partition on.

## Source

To use the KafkaSource, add the following to your flume-site.xml file:

```xml
<property>
  <name>flume.plugin.classes</name>
  <value>org.apache.flume.kafka.KafkaSource</value>
</property>
```

To configure the source:

```bash
flume shell -c localhost:35873 -e exec config localhost 'kafka("localhost:2181", "topic", "my_group_id", "1")' 'console'
```

The parameter list is (zookeeper_string, topic, group_id, num_consumer_threads).

If your downstream logic is not idempotent, you should not currently be using the Kafka source.  The Kafka client commits the offsets for the messages it has received to ZooKeeper asyncronously (configurable in Kafka) as a background operation.  Because the Flume source is not committing the message offsets on every event processed, you will end up getting duplicates.
