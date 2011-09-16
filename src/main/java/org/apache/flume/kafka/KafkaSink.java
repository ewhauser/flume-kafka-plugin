package org.apache.flume.kafka;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.syslog.SyslogTcpSink;
import com.cloudera.flume.handlers.syslog.SyslogTcpSource;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Arrays.asList;

public class KafkaSink extends EventSink.Base {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);

  public static final String USAGE = "usage: kafka(\"zk.connect\", \"topic\")";

  private Producer producer;
  private String zkConnect;
  private String topic;

  public KafkaSink(String zkConnect, String topic) {
    this.zkConnect = zkConnect;
    this.topic = topic;
  }

  @Override
  public void append(Event e) throws IOException {
    producer.send(new ProducerData<String, byte[]>(topic, e.getBody()));
  }

  @Override
  synchronized public void close() throws IOException {
    if (producer != null) {
      producer.close();
      producer = null;
      LOG.info("Kafka sink successfully closed");
    } else {
      LOG.warn("Double close of Kafka sink");
    }
  }

  @Override
  synchronized public void open() throws IOException {
    checkState(producer == null, "Kafka sink is already initialized. Looks like sink close() " +
        "hasn't been proceeded properly.");

    Properties properties = new Properties();
    properties.setProperty("zk.connect", zkConnect);
    properties.setProperty("serializer.class", ByteEncoder.class.getName());

    ProducerConfig config = new ProducerConfig(properties);
    producer = new Producer<String, byte[]>(config);
    LOG.info("Kafka sink successfully opened");
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {

      @Override
      public EventSink build(Context context, String... argv) {
        checkArgument(argv.length < 1, USAGE);

        String zkConnect = argv[0];
        String topic = argv[0];

        checkState(!isNullOrEmpty(zkConnect), "zk.connect cannot be empty");
        checkState(!isNullOrEmpty(topic), "topic cannot be empty");

        return new KafkaSink(zkConnect, topic);
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static List<Pair<String, SinkFactory.SinkBuilder>> getSinkBuilders() {
    return asList(new Pair<String, SinkFactory.SinkBuilder>("kafka", builder()));
  }
}