package org.apache.beam.sdk.io.redis;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import org.apache.beam.sdk.io.redis.RedisStreamFn.StreamDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.GetInitialRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedisStreamFn extends DoFn<StreamDescriptor, StreamEntry> {

  private final RedisConnectionConfiguration config;

  private transient Jedis jedis;

  public RedisStreamFn(RedisConnectionConfiguration redisConnectionConfiguration) {
    this.config = redisConnectionConfiguration;
  }

  @Setup
  public void setup() {
    this.jedis = new Jedis(config.host().get(), config.port().get());
  }
  
  @GetInitialRestriction
  public StreamEntryID initialRestriction(@Element StreamDescriptor sd) {
    return StreamEntryID.LAST_ENTRY;
  }

  @ProcessElement
  public ProcessContinuation processElement(@Element StreamDescriptor sd, RestrictionTracker<StreamEntryID, StreamEntryID> tracker, OutputReceiver<StreamEntry> receiver) {
     Map.Entry<String, StreamEntryID> mapEntry = new AbstractMap.SimpleImmutableEntry<>(sd.getKey(),
          StreamEntryID.UNRECEIVED_ENTRY);
    createConsumerGroup(tracker.currentRestriction(), sd);
    while (true) {
      List<StreamEntry> entries = jedis
            .xreadGroup(sd.getGroupName(), "consumername", sd.batchSize, 1000, true, mapEntry)
            .stream()
            .flatMap(entry -> entry.getValue().stream())
            .collect(Collectors.toList());
      if (entries.isEmpty()) {
        return ProcessContinuation.resume();
      }
      for (StreamEntry se : entries) {
        try {
          if (tracker.tryClaim(se.getID())) {
            Instant instant = Instant.now();
            receiver.outputWithTimestamp(se, instant);
          } else {
            return ProcessContinuation.stop();
          }
        } catch (Exception ex) {

        }
      }
    } 
  }

  public static class StreamDescriptor implements Serializable {
    private final String key;
    private final String consumerName;
    private final String groupName;
    private final int batchSize;

    public StreamDescriptor(String key, String consumerName, String groupName, int batchSize) {
      this.key = key;
      this.consumerName = consumerName;
      this.groupName = groupName;
      this.batchSize = batchSize; 
    }

    public String getKey() {
      return key;
    }

    public String getConsumerName() {
      return consumerName;
    }

    public String getGroupName() {
      return groupName;
    }

    public int getBatchSize() {
      return batchSize;
    }

  }

  ////////private

  public void createConsumerGroup(StreamEntryID offset, StreamDescriptor sd) {
    try {
      jedis.xgroupCreate(sd.getKey(), sd.getGroupName(), new StreamEntryID(), true);
    } catch (JedisDataException ex) {
      if (ex.getMessage().contains("name already exists")) {
        System.out.println("already exists");
      } else {
        System.out.println("JedisDataException = " + ex);
        ex.printStackTrace();
      }
    }
    jedis.xgroupSetID(sd.getKey(), sd.getGroupName(), offset);
  }


  public static class RedisStreamResrictionTracker extends
        RestrictionTracker<StreamEntryID, StreamEntryID> {

      StreamEntryID current;

      @Override
      public boolean tryClaim(StreamEntryID position) {
        this.current = position;
        return true;
      }

      @Override
      public StreamEntryID currentRestriction() {
        return current;
      }

      @Override
      public @Nullable SplitResult<StreamEntryID> trySplit(double fractionOfRemainder) {
        return null;
      }

      @Override
      public void checkDone() throws IllegalStateException {

      }

      @Override
      public IsBounded isBounded() {
        return IsBounded.UNBOUNDED;
      }
    }
}
