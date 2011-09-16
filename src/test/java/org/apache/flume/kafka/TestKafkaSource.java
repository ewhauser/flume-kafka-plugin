package org.apache.flume.kafka;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SourceFactory;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Utils;
import kafka.zk.EmbeddedZookeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;

public class TestKafkaSource {

  private EmbeddedZookeeper zkServer;
  private int brokerId = 0;
  private short port = 9092;
  private KafkaServer server;
  private Producer<String, byte[]> producer;

  /**
   * Output Debugging messages
   */
  @Before
  public void setup() {
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
    Properties props = TestUtils.createBrokerConfig(brokerId, port);
    KafkaConfig config = new KafkaConfig(props);
    server = TestUtils.createServer(config);

    Properties properties = new Properties();
    properties.setProperty("zk.connect", TestZKUtils.zookeeperConnect());
    properties.setProperty("serializer.class", ByteEncoder.class.getName());

    ProducerConfig producerConfig = new ProducerConfig(properties);
    producer = new Producer<String, byte[]>(producerConfig);
  }

  @Test
  public void simpleConsumer() throws FlumeSpecException, IOException, InterruptedException {
    SourceFactory.SourceBuilder bld = KafkaSource.kafkaSourceBuilder();
    EventSource src = bld.create(null, TestZKUtils.zookeeperConnect(), "test", 2);
    final List<Event> captured = Lists.newArrayList();
    EventSink snk = new EventSink.Base() {
      @Override
      public void append(Event e) throws IOException, InterruptedException {
        captured.add(e);
      }
    };

    src.open();
    snk.open();

    producer.send(new ProducerData<String, byte[]>("test", "hello world".getBytes()));

    for (int i = 0; i < 1; i++) {
      Event e = src.next();
      snk.append(e);
    }
    snk.close();
    src.close();

    assertEquals(1, captured.size());
    assertEquals("hello world", new String(captured.get(0).getBody()));
  }

  @After
  public void after() throws Exception {
    if (producer != null) producer.close();

    server.shutdown();
    Utils.rm(server.config().logDir());
    Utils.rm(server.config().logDir());
    Thread.sleep(500);
    zkServer.shutdown();
  }
}
