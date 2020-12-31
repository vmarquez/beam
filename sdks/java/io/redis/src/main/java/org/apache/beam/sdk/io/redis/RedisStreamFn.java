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
import org.apache.beam.sdk.io.redis.StreamDescriptor.StreamConsumer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.beam.sdk.io.redis.RedisStreamRestrictionTracker;

//TODO: internal classes shoudln't be prefaced with Redis, shoudln't be pucli

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
  public StreamRestriction initialRestriction(@Element StreamDescriptor sd) {
    return new StreamRestriction(sd.getConsumerName(), new StreamEntryID()); 
  }

  @NewTracker
  public RestrictionTracker<StreamRestriction, StreamEntryID> getNewRestrictionTracker(@Restriction StreamRestriction restriction) {
    return new RedisStreamRestrictionTracker(restriction);
  }

  @ProcessElement
  public ProcessContinuation processElement(@Element StreamDescriptor sd, RestrictionTracker<StreamRestriction, StreamEntryID> tracker, OutputReceiver<StreamEntry> receiver, BundleFinalizer finalizer) {
    createConsumerGroup(sd);
    //TODO: MAKE THE ARRAYLIST IT'S OPERATING ON LOCAL
    finalizer.afterBundleCommit(Instant.now().plus(5 * 1000), () -> commitAcks(sd));
    if (this.toAck == null) {
      System.out.println("WOW ITS NULL");
      this.toAck = new ArrayList<>();
    }
    int x = 0;
    boolean pending = !tracker.currentRestriction().getCurrentEntry().toString().equals(StreamEntryID.UNRECEIVED_ENTRY.toString());
    while (x < 15) {
      try {
        if (limit != null && this.elementCount >= limit) {
          System.out.println("about to call systme.exit limit = " + limit +" elementCount = " + elementCount);
          return ProcessContinuation.stop();
        }

        List<StreamEntry> entries = getStreamEntrys(jedis, sd, tracker.currentRestriction().getCurrentEntry());
        System.out.println("entries = " + entries);
        
        if ((entries == null || entries.isEmpty()) && pending) {
            pending = false;
            tracker.tryClaim(StreamEntryID.UNRECEIVED_ENTRY);
        } 
        for (StreamEntry se : entries) {
          System.out.println("trying to output here ="+ se.toString());
          elementCount++;
          //only claim if we are fetching pending
          if (pending) {
            tracker.tryClaim(se.getID());
          } else {
            System.out.println("setting to unrecieved entry");
            tracker.tryClaim(StreamEntryID.UNRECEIVED_ENTRY);
          }
          receiver.outputWithTimestamp(se, Instant.now());
          toAck.add(se.getID());
        }
        x++;
      } catch (Exception ex) {
        ex.printStackTrace();
        System.out.println("ex = " + ex);
        return ProcessContinuation.stop();
      }
    }
    System.out.println("what is the x = " + x);
    return ProcessContinuation.stop();
  }

  //TODO: move to private

  @GetRestrictionCoder
  public Coder<StreamRestriction> restrictionCoder() {
    return SerializableCoder.of(StreamRestriction.class);
  }

  ////////private
  private transient List<StreamEntryID> toAck = new ArrayList<>();

  private long elementCount = 0;

  public static List<StreamEntry> getStreamEntrys(Jedis jedis, StreamDescriptor sd, StreamEntryID fetchId) {
    Map.Entry<String, StreamEntryID> mapEntry = new AbstractMap.SimpleImmutableEntry<>(sd.getKey(),
            fetchId);
    System.out.println(" *** fetching wtih groupname = " + sd.getGroupName() + "key = "  + sd.getKey() + " and fetchId = " + fetchId + " with consumerName = " + sd.getConsumerName());
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

  }