package org.apache.beam.sdk.io.redis;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.redis.RedisStreamIOUnboundedSource.RedisCheckpointMarker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class RedisStreamIOUnboundedSource extends
    UnboundedSource<StreamEntry, RedisCheckpointMarker> {

  @Override
  public List<? extends UnboundedSource<StreamEntry, RedisCheckpointMarker>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {

    //FIXME: make this parallell
    return Collections.singleton(this).stream().collect(Collectors.toList());
  }

  @Override
  public UnboundedReader<StreamEntry> createReader(PipelineOptions options,
      @Nullable RedisCheckpointMarker checkpointMark) throws IOException {
    System.out.println("creating reader with checkpointMark = " + checkpointMark);
    createConsumerGroup(Optional.ofNullable(checkpointMark).map(cp -> cp.getLastRead()));
    return new RedisStreamReader(redisKey, groupId, this, config);
  }

  @Override
  public Coder<RedisCheckpointMarker> getCheckpointMarkCoder() {
    return SerializableCoder.of(RedisCheckpointMarker.class);
  }

  @Override
  public Coder<StreamEntry> getOutputCoder() {
    return SerializableCoder.of(StreamEntry.class);
  }

  ////////// Private Members //////////
  private final RedisConnectionConfiguration config;

  private final String redisKey;

  private final long timeout;

  private final String groupId;

  public RedisStreamIOUnboundedSource(String redisKey, long timeout, String groupId, RedisConnectionConfiguration config) {
    this.config = config;
    this.redisKey = redisKey;
    this.timeout = timeout;
    this.groupId = groupId;
  }

  public void createConsumerGroup(Optional<StreamEntryID> offset) {
    System.out.println("create consumergroup groupId = " + groupId + " key =" + redisKey);
    Jedis jedis = new Jedis(config.host().get(), config.port().get());
    try {
      jedis.xgroupCreate(redisKey, groupId, new StreamEntryID(), true);
    } catch (JedisDataException ex) {
      if (ex.getMessage().contains("name already exists")) {
        System.out.println("already exists");
      } else {
        System.out.println("JedisDataException = " + ex);
        ex.printStackTrace();
      }
    }
    //since we aren't doing ACKs, if we crash and need to start over from a previously read point, we do so here
    //FIXME: for multiple streams
    offset.ifPresent(streamEntryID -> System.out.println("Going to set xgroupSetId to " + streamEntryID));
    offset.ifPresent(streamEntryID -> jedis.xgroupSetID(redisKey, groupId, streamEntryID));
  }


  //TODO: make it work for a map of StreamEntries
  public static class RedisCheckpointMarker implements CheckpointMark, Serializable {

    private final StreamEntryID lastRead;

    public RedisCheckpointMarker(StreamEntryID lastRead) {
      this.lastRead = lastRead;
    }

    public StreamEntryID getLastRead() {
      return this.lastRead;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      //should we commit back here?
    }
  }

  public static class RedisStreamReader extends UnboundedReader<StreamEntry> {

    @Override
    public boolean start() throws IOException {
      System.out.println("Start");
      return advance();
    }

    //TODO: make this dump to a simple  queue,

    @Override
    public boolean advance() throws IOException {
      System.out.println("------\n \n \n -0----");
      if (queue.isEmpty() && !fetchNextBatch()) {
        return false;
      } else {
        System.out.println("queue size = " + queue.size());
        currentStreamEntry = Optional.ofNullable(queue.poll());
        return true;
      }
    }

    @Override
    public StreamEntry getCurrent() throws NoSuchElementException {
      System.out.println("GETCurrent");
      if (currentStreamEntry.isPresent()) {
        //this.nextId = currentStreamEntry.map(se -> se.getID());
        return this.currentStreamEntry.get();

      } else {
        System.out.println("NoSUchElementException + " + queue.size());
        throw new NoSuchElementException();
      }
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (this.currentStreamEntry.isPresent()) {
        return Instant.ofEpochMilli(this.currentStreamEntry.get().getID().getTime());
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public void close() throws IOException {
      jedis.close();
    }

    @Override
    public Instant getWatermark() {
      return Instant.now();
      //return null;
    }

    //QUESTION: better to return null?
    @Override
    public CheckpointMark getCheckpointMark() {
      System.out.println("getCHeckpointMark for " + this.hashCode() + " + currentstreamEntry = " + this.currentStreamEntry);
      return new RedisCheckpointMarker(this.currentStreamEntry.map(se -> se.getID()).orElseGet(() -> StreamEntryID.UNRECEIVED_ENTRY));

    }

    @Override
    public UnboundedSource<StreamEntry, ?> getCurrentSource() {
      return redisSource;
    }

    ///////////////////
    private final Jedis jedis;

    private final RedisStreamIOUnboundedSource redisSource;

    private final RedisConnectionConfiguration config;

    private final String key;

    private final String groupId;

    private Optional<StreamEntry> currentStreamEntry = Optional.empty();

    private final Queue<StreamEntry> queue;

    //private Optional<StreamEntryID> nextId = Optional.of(StreamEntryID.UNRECEIVED_ENTRY);

    public RedisStreamReader(String key, String groupId, RedisStreamIOUnboundedSource source,
        RedisConnectionConfiguration config) {
      this.redisSource = source;
      this.jedis = new Jedis(config.host().get(), config.port().get());
      this.config = config;
      this.key = key;
      this.groupId = groupId;
      this.queue = new ArrayDeque<>();
    }

    private boolean fetchNextBatch() {
      Map.Entry<String, StreamEntryID> mapEntry = new AbstractMap.SimpleImmutableEntry<>(key,
          StreamEntryID.UNRECEIVED_ENTRY);

      System.out.println("fetchNextBatch for " + this.hashCode() + " XREADGROUP GROUP " + groupId + " consumername " + " COUNT BLOCK 1000 STREAMS " + key);
      //FIXME: consumername should be based on teh split #
      List<Entry<String, List<StreamEntry>>> entries = jedis
          .xreadGroup(groupId, "consumername", 1, 1000, true, mapEntry);

      if (entries == null || isEmpty(entries)) {
        System.out.println("    EMPTY!!!!");
        return false;
      } else {
        System.out.println("    NOT EMPTY");

        queue.addAll(
            entries.stream().flatMap(se -> se.getValue().stream()).collect(Collectors.toList()));
        return true;
      }
    }

    private boolean isEmpty(List<Entry<String, List<StreamEntry>>> list) {
      return list.stream().mapToLong(e -> e.getValue().size()).sum() == 0;
    }
  }
}