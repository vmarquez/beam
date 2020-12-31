package org.apache.beam.sdk.io.redis;

import java.io.Serializable;

public class StreamDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String key;
    private final StreamConsumer consumerName;
    private final String groupName;
    private final int batchSize;

    public StreamDescriptor(String key, StreamConsumer consumerName, String groupName, int batchSize) {
      this.key = key;
      this.consumerName = consumerName;
      this.groupName = groupName;
      this.batchSize = batchSize; 
    }

    public String getKey() {
      return key;
    }

    public StreamConsumer getConsumerName() {
      return consumerName;
    }

    public String getGroupName() {
      return groupName;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public static class StreamConsumer implements Serializable {
      private static final long serialVersionUID = 1L;
      private final String name;
      private final int count;
      public StreamConsumer(String name, int count) {
        this.name = name;
        this.count = count;
      }
      public String getName() {
        return name;
      }

      public int getCount() {
        return count;
      }

      @Override
      public String toString() {
        return name + count;
      }
  }

  }