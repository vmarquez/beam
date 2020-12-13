package org.apache.beam.sdk.io.redis;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.redis.RedisStreamFn.StreamDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
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
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

@UnboundedPerElement
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class RedisStreamFn extends DoFn<StreamDescriptor, StreamEntry> {

  private static final Logger LOG = LoggerFactory.getLogger(RedisStreamFn.class);

  private final RedisConnectionConfiguration config;

  private transient Jedis jedis;

  private final Long limit;

  public RedisStreamFn(RedisConnectionConfiguration redisConnectionConfiguration, long limit) {
    System.out.println("in the redisStreamFn long = " + limit);
    this.config = redisConnectionConfiguration;
    this.limit = limit;
  }

  public RedisStreamFn(RedisConnectionConfiguration redisConnectionConfiguration) {
    this.config = redisConnectionConfiguration;
    this.limit = null;//fixme?  
  }

  @Setup
  public void setup() {
    this.jedis = new Jedis(config.host().get(), config.port().get());
  }
  
  @GetInitialRestriction
  public RedisStreamRestriction initialRestriction(@Element StreamDescriptor sd) {
    return new RedisStreamRestriction(StreamEntryID.UNRECEIVED_ENTRY, sd.getConsumerName());
  }

  @NewTracker
  public RestrictionTracker<RedisStreamRestriction, StreamEntryID> getNewRestrictionTracker(@Restriction RedisStreamRestriction restriction) {
    return new RedisStreamResrictionTracker(restriction);
  }
 
  @ProcessElement
  public ProcessContinuation processElement(@Element StreamDescriptor sd, RestrictionTracker<RedisStreamRestriction, StreamEntryID> tracker, OutputReceiver<StreamEntry> receiver, BundleFinalizer finalizer) {
     Map.Entry<String, StreamEntryID> mapEntry = new AbstractMap.SimpleImmutableEntry<>(sd.getKey(),
          StreamEntryID.UNRECEIVED_ENTRY);
    createConsumerGroup(tracker.currentRestriction().getPosition(), sd);
    finalizer.afterBundleCommit(Instant.now().plus(5 * 1000), () -> commitAcks(sd)); 
    long x = 0;
    while (true) {
      System.out.println("looping, current restriction =" + tracker.currentRestriction().getPosition());
      if (limit != null && x >= limit) {
        return ProcessContinuation.stop();
      }
      try { 
        List<Entry<String, List<StreamEntry>>> entries = jedis
              .xreadGroup(sd.getGroupName(), sd.getConsumerName(), sd.getBatchSize(), 1000, true, mapEntry);
        if ((entries == null || entries.isEmpty()) && limit == null) {
          return ProcessContinuation.resume();
        }
       
        List<StreamEntry> flattened = entries.stream().flatMap(se -> se.getValue().stream()).collect(Collectors.toList());
        for (StreamEntry se : flattened) { 
          if (tracker.tryClaim(se.getID())) {
            x++;
            Instant instant = Instant.now();
            System.out.println("    ooo output = " + se);
            receiver.outputWithTimestamp(se, instant);
            toAck.add(se.getID());
          } else {
            return ProcessContinuation.stop();
          }
        }
      } catch (Exception ex) {
        LOG.error("error pulling data from redis for " + sd.getKey() + " for my id = " + this.hashCode(), ex);
        return ProcessContinuation.stop();
      }
    }
  }

  public static class StreamDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;
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

  @GetRestrictionCoder
  public Coder<RedisStreamRestriction> restrictionCoder() {
    return SerializableCoder.of(RedisStreamRestriction.class);
  }

  ////////private
  private transient List<StreamEntryID> toAck = new ArrayList<>();


  private void commitAcks(StreamDescriptor sd) {
    toAck.forEach(sid -> jedis.xack(sd.getKey(), sd.getGroupName(), sid));
    toAck.clear();
  }
 
  void createConsumerGroup(StreamEntryID offset, StreamDescriptor sd) {
    System.out.println("creating consumer Group " + sd.getConsumerName() + " for my current sd = " + sd.hashCode() + " For the splittabledofn of " + this.hashCode());
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
    System.out.println("setting the offset = " + offset);
    StreamEntryID id = offset.toString().equals(">") ? new StreamEntryID() : offset;
        System.out.println("setting the offset = " + id);
    jedis.xgroupSetID(sd.getKey(), sd.getGroupName(), id);
  }

  public static class RedisStreamRestriction implements Serializable {
    private static final long serialVersionUID = 1L;

    private final StreamEntryID position;
    
    private final String consumerName;

    public RedisStreamRestriction(StreamEntryID position, String consumerName) {
      this.position = position;
      this.consumerName = consumerName;
    }

    public StreamEntryID getPosition() {
      return position;
    }

    public String getConsumerName() {
      return consumerName;
    }
  }

  /* Redis sharding is controlled serverside by the consumer name. Redis Stream IDs are not monotonitcally increasing */
  static class RedisStreamResrictionTracker extends
        RestrictionTracker<RedisStreamRestriction, StreamEntryID> implements Serializable {
 
      StreamEntryID current;

      String consumerName;

      public RedisStreamResrictionTracker(RedisStreamRestriction restriction) {          
        this.current = restriction.getPosition();
        this.consumerName = restriction.getConsumerName();
      }

      public RedisStreamResrictionTracker(String consumerName) {

      }

      @Override
      public boolean tryClaim(StreamEntryID position) {
        this.current = position;
        return true;
      }

      @Override
      public RedisStreamRestriction currentRestriction() {
        return new RedisStreamRestriction(current, consumerName);
      }

      @Override
      public @Nullable SplitResult<RedisStreamRestriction> trySplit(double fractionOfRemainder) {
        RedisStreamRestriction newRestriction = new RedisStreamRestriction(StreamEntryID.UNRECEIVED_ENTRY, consumerName + UUID.randomUUID().toString());
        RedisStreamRestriction currentRestriction = new RedisStreamRestriction(current, consumerName);
        return SplitResult.of(currentRestriction, newRestriction);
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
