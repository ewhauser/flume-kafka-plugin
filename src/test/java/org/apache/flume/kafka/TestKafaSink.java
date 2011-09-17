package org.apache.flume.kafka;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.util.Clock;
import junit.framework.AssertionFailedError;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import static junit.framework.Assert.*;

public class TestKafaSink {

  private EmbeddedZookeeper zkServer;

  private int port = 9092;
  private KafkaServer server;
  private SimpleConsumer consumer;
  private KafkaSink kafkaSink;

  @Before
  public void before() {
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
    Properties props = TestUtils.createBrokerConfig(0, port);
    props.setProperty("num.partitions", "2");
    props.setProperty("topic.partition.count.map", "test:2");
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
    expectOneMessage(0);
  }

  @Test
  public void sampleKeyGoesToCorrectPartition() {
    assertEquals(new String("testPartitionKey".getBytes()).hashCode() % 2, 1);
  }

  @Test
  public void canSendToPartition() throws Exception {
    kafkaSink = new KafkaSink(TestZKUtils.zookeeperConnect(), "test");
    kafkaSink.open();
    sendPartitionedMessage();
    Thread.sleep(100);
    expectOneMessage(0);

    //Race condition while waiting for balancing
    long start = System.currentTimeMillis();
    boolean found = false;
    while (!found && System.currentTimeMillis() < (start + 3000)) {
      Thread.sleep(100);
      try {
        sendPartitionedMessage();
        expectOneMessage(1);
      } catch (AssertionFailedError e) {
        continue;
      }
      found = true;
    }
    sendPartitionedMessage();
    expectOneMessage(1);
  }

  private void sendPartitionedMessage() throws IOException {
    Event e = new EventImpl("test1".getBytes(), Clock.unixTime(), Event.Priority.INFO, 0, "localhost");
    e.set("kafka.partition.key", "testPartitionKey".getBytes());
    kafkaSink.append(e);
  }

  private void expectOneMessage(int partition) {
    Iterator<Message> messageSet1 = consumer.fetch(new FetchRequest("test", partition, 0, 10000)).iterator();
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