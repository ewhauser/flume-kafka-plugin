package org.apache.flume.kafka;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Pair;
import com.google.common.collect.Maps;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;

public class KafkaSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  final BlockingQueue<Event> eventQueue = new ArrayBlockingQueue<Event>(
      FlumeConfiguration.get().getPollerQueueSize());

  private ConsumerConnector connector;
  private String topic;
  private volatile boolean shutdown = false;
  private ExecutorService executor;
  private String group;
  private int threads;

  public KafkaSource(final String zkConnect, final String topic, String group, int threads) {
    this.group = group;
    this.threads = threads;
    this.topic = topic;
    Properties properties = new Properties();
    properties.setProperty("zk.connect", zkConnect);
    properties.setProperty("serializer.class", ByteEncoder.class.getName());
    properties.setProperty("groupid", group);
    ConsumerConfig consumerConfig = new ConsumerConfig(properties);

    connector = Consumer.createJavaConsumerConnector(consumerConfig);
    executor = Executors.newFixedThreadPool(threads, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("kafka_consumer_" + topic);
        return thread;
      }
    });
  }

  public void close() throws IOException {
    // make sure
    shutdown = true;
    try {
      executor.shutdown();
      executor.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.debug("Waiting for Kafka consumer threads to exit was interrupted", e);
    }
  }

  /**
   * Blocks on either getting an event from the queue or process exit (at which
   * point it throws an exception).
   */
  public Event next() throws IOException {
    Event evt;
    try {
      while (true) {
        evt = eventQueue.take();
        if (evt == null) {
          continue;
        }
        updateEventProcessingStats(evt);
        return evt;
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("KafkaSource was interrupted - " + e);
    }
  }

  public void open() throws IOException {
    Map<String, Integer> topicCountMap = Maps.newHashMapWithExpectedSize(1);
    topicCountMap.put(topic, threads);
    Map<String, List<KafkaMessageStream>> consumerMap = connector.createMessageStreams(topicCountMap);

    for (KafkaMessageStream stream : consumerMap.get(topic)) {
      executor.submit(new KafkaConsumerThread(stream));
    }
  }

  class KafkaConsumerThread implements Callable<Object> {

    private KafkaMessageStream stream;

    public KafkaConsumerThread(KafkaMessageStream stream) {
      this.stream = stream;
    }

    @Override
    public Object call() throws Exception {
      try {
        ConsumerIterator it = stream.iterator();
        while(!shutdown && it.hasNext()) {
          Message message = it.next();
          ByteBuffer buffer = message.payload();
          byte [] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          Event e = new EventImpl(bytes);
          while (!eventQueue.offer(e, 200, TimeUnit.MILLISECONDS)) {
          }
        }
      } catch (InterruptedException e) {
        if (!shutdown) {
          LOG.warn("KafkaSource received unexpected InterruptedException", e);
        }
      }
      return new Object();
    }
  }

  public static SourceFactory.SourceBuilder kafkaSourceBuilder() {
    return new SourceFactory.SourceBuilder() {
      @Override
      public EventSource build(Context ctx, String... argv) {
        checkArgument(argv.length >= 0 && argv.length <= 4,
            "kafka[(zk.connect), (topic), (group), (threads)]");

        String zkConnect = argv[0];
        String topic = argv[1];
        String groupid = argv[2];
        Integer threads = Integer.parseInt(argv[3]);

        return new KafkaSource(zkConnect, topic, groupid, threads);
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static List<Pair<String, SourceFactory.SourceBuilder>> getSourceBuilders() {
    return asList(new Pair<String, SourceFactory.SourceBuilder>("kafka", kafkaSourceBuilder()));
  }

}

