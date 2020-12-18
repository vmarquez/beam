package org.apache.beam.sdk.io.redis;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  public StreamConsumer initialRestriction(@Element StreamDescriptor sd) {
    return sd.getConsumerName();
  }

  @NewTracker
  public RestrictionTracker<StreamConsumer, StreamEntryID> getNewRestrictionTracker(@Restriction StreamConsumer restriction) {
    return new RedisStreamResrictionTracker(restriction);
  }

  @ProcessElement
  public ProcessContinuation processElement(@Element StreamDescriptor sd, RestrictionTracker<StreamConsumer, StreamEntryID> tracker, OutputReceiver<StreamEntry> receiver, BundleFinalizer finalizer) {
    createConsumerGroup(sd);
    finalizer.afterBundleCommit(Instant.now().plus(5 * 1000), () -> commitAcks(sd));
    if (this.toAck == null) {
      System.out.println("WOW ITS NULL");
      this.toAck = new ArrayList<>();
    } 
    StreamEntryID fetchId = new StreamEntryID();
    int x = 0;
    while (x < 10) {
      x++;
      try {
        if (limit != null && this.elementCount >= limit) {
          System.out.println("about to call systme.exit");
          return ProcessContinuation.stop(); //keepFetch == false?
        }
        List<StreamEntry> entries = getStreamEntrys(jedis, sd, fetchId);
        System.out.println("entries = " + entries);
        
        if ((entries == null || entries.isEmpty()) && fetchId.equals(new StreamEntryID())) {
            fetchId = StreamEntryID.UNRECEIVED_ENTRY;
        }
        
        entries.forEach(se -> {
          System.out.println("trying to output here ="+ se.toString()); 
          elementCount++;
          tracker.tryClaim(se.getID());
          receiver.outputWithTimestamp(se, Instant.now());
          toAck.add(se.getID());
        });
        elementCount++;
      } catch (Exception ex) {
        ex.printStackTrace();
        System.out.println("ex = " + ex);
        return ProcessContinuation.stop();
      }
    }
    return ProcessContinuation.stop();
  }

  //TODO: move to private

  @GetRestrictionCoder
  public Coder<StreamConsumer> restrictionCoder() {
    return SerializableCoder.of(StreamConsumer.class);
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

  public static class StreamDescriptor implements Serializable {
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
  }

  
  ////////private
  private transient List<StreamEntryID> toAck = new ArrayList<>();

  private long elementCount = 0;

  public static List<StreamEntry> getStreamEntrys(Jedis jedis, StreamDescriptor sd, StreamEntryID fetchId) {
    Map.Entry<String, StreamEntryID> mapEntry = new AbstractMap.SimpleImmutableEntry<>(sd.getKey(),
            fetchId);
            System.out.println(" fetching wtih key " + sd.getKey() + " and fetchId = " + fetchId);
    List<Map.Entry<String, List<StreamEntry>>> list = Optional.ofNullable(jedis
            .xreadGroup(sd.getGroupName(), sd.getConsumerName().toString(), sd.getBatchSize(), 1000, false, mapEntry)).orElse(new ArrayList<>());
    return list.stream().flatMap(se -> se.getValue().stream()).collect(Collectors.toList());
  }

  private void commitAcks(StreamDescriptor sd) {
    toAck.forEach(sid -> jedis.xack(sd.getKey(), sd.getGroupName(), sid));
    toAck.clear();
  }
 
  private void createConsumerGroup(StreamDescriptor sd) {
    System.out.println("creating consumer Group " + sd.getConsumerName() + " for my current sd = " + sd.hashCode() + " For the splittabledofn of " + this.hashCode());
    try {
      jedis.xgroupCreate(sd.getKey(), sd.getGroupName(), new StreamEntryID(), true);
    } catch (JedisDataException ex) {
      if (ex.getMessage().contains("name already exists")) {
        System.out.println("already exists");
      }
    }
  }

  static class RedisStreamResrictionTracker extends
        RestrictionTracker<StreamConsumer, StreamEntryID> implements Serializable {
 
    private static final long serialVersionUID = 1L;

    StreamEntryID current = new StreamEntryID();

      StreamConsumer streamConsumer;

      public RedisStreamResrictionTracker(StreamConsumer streamConsumer) {          
        this.streamConsumer = streamConsumer;
      }

      @Override
      public boolean tryClaim(StreamEntryID position) {
        //check to flip the switch to new
        this.current = position;
        return true;
      }

      @Override
      public StreamConsumer currentRestriction() {
        return this.streamConsumer; 
      }

      @Override
      public @Nullable SplitResult<StreamConsumer> trySplit(double fractionOfRemainder) {
        //TODO: do we need the split to also check for ACKSs?
        return SplitResult.of(this.streamConsumer, new StreamConsumer(this.streamConsumer.getName(), this.streamConsumer.getCount()+1));
      }

      @Override
      public void checkDone() throws IllegalStateException {}

      @Override
      public IsBounded isBounded() {
        return IsBounded.UNBOUNDED;
      }
    }
}
