package org.apache.beam.sdk.io.redis;

import java.io.Serializable;

import org.apache.beam.sdk.io.redis.StreamDescriptor.StreamConsumer;

import redis.clients.jedis.StreamEntryID;

public class StreamRestriction implements Serializable {
      private static final long serialVersionUID = 1L;

      private StreamConsumer consumer;

      private StreamEntryID currentEntryID;

      public StreamConsumer getConsumer() {
        return this.consumer;
      }
      public StreamEntryID getCurrentEntry() {
        return this.currentEntryID;
      }

      public StreamRestriction(StreamConsumer consumer, StreamEntryID currentEntryID) {
        this.consumer = consumer;
        this.currentEntryID = currentEntryID;
      }
    }