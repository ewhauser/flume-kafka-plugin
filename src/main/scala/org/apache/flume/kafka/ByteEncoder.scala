package org.apache.flume.kafka

import kafka.message.Message
import kafka.serializer.Encoder

class ByteEncoder extends Encoder[Array[Byte]] {
  override def toMessage(event: Array[Byte]): Message = new Message(event)
}
