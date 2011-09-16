package org.apache.flume.kafka;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.util.Clock;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Utils;
import kafka.zk.EmbeddedZookeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

public class TestKafaSink {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafaSink.class);
  private EmbeddedZookeeper zkServer;

  private int brokerId = 0;
  private int port = 9092;
  private KafkaConfig config;
  private Properties props;
  private KafkaServer server;
  private SimpleConsumer consumer;
  private KafkaSink kafkaSink;

  @Before
  public void before() {
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
    Properties props = TestUtils.createBrokerConfig(brokerId, port);
    KafkaConfig config = new KafkaConfig(props);
    server = TestUtils.createServer(config);
    consumer = new SimpleConsumer("localhost", port, 1000000, 64*1024);
  }

  @Test
  public void appendsMessage() throws Exception {
    kafkaSink = new KafkaSink(TestZKUtils.zookeeperConnect(), "test");
    kafkaSink.open();
    Event e = new EventImpl("test1".getBytes(), Clock.unixTime(), Event.Priority.INFO, 0, "localhost");
    kafkaSink.append(e);
    Thread.sleep(100);
    // cross check if brokers got the messages
    Iterator<Message> messageSet1 = consumer.fetch(new FetchRequest("test", 0, 0, 10000)).iterator();
    assertTrue("Message set should have 1 message", messageSet1.hasNext());
    assertEquals(new Message("test1".getBytes()), messageSet1.next());
  }

  @Test(expected = IllegalArgumentException.class)
  public void requiresZkConnectionString() {
    KafkaSink.builder().create(null, "", "test");
    fail();
  }

  @Test(expected = IllegalArgumentException.class)
  public void requiresTopic() {
    KafkaSink.builder().create(null, "localhost:2181", "");
    fail();
  }

  @After
  public void after() throws Exception {
    if (kafkaSink != null) kafkaSink.close();

    server.shutdown();
    Utils.rm(server.config().logDir());
    Utils.rm(server.config().logDir());
    Thread.sleep(500);
    zkServer.shutdown();
  }

}