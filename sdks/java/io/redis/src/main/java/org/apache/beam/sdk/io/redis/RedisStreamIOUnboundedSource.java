/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.redis;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.redis.RedisStreamIOUnboundedSource.RedisCheckpointMarker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisStreamIOUnboundedSource
    extends UnboundedSource<StreamEntry, RedisCheckpointMarker> {

  @Override
  public List<? extends UnboundedSource<StreamEntry, RedisCheckpointMarker>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    // TODO: loop and return various consumernames with loop count + prefix
    // FIXME: make this parallell
    RedisStreamIOUnboundedSource source =
        new RedisStreamIOUnboundedSource(redisKey, timeout, groupId, config, consumerPrefix + "1");
    return Collections.singleton(source).stream().collect(Collectors.toList());
  }

  @Override
  public UnboundedReader<StreamEntry> createReader(
      PipelineOptions options, @Nullable RedisCheckpointMarker checkpointMark) throws IOException {
    System.out.println("creating reader with checkpointMark = " + checkpointMark);
    createConsumerGroup(); // Optional.ofNullable(checkpointMark).map(cp -> cp.getLastRead()));
    StreamEntryID startFrom =
        Optional.ofNullable(checkpointMark).map(cp -> cp.getLastRead()).orElse(new StreamEntryID());
    return new RedisStreamReader(redisKey, groupId, this, config, startFrom, consumerPrefix);
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

  private final String consumerPrefix;

  public RedisStreamIOUnboundedSource(
      String redisKey,
      long timeout,
      String groupId,
      RedisConnectionConfiguration config,
      String consumerPrefix) {
    this.config = config;
    this.redisKey = redisKey;
    this.timeout = timeout;
    this.groupId = groupId;
    this.consumerPrefix = consumerPrefix;
  }

  public void createConsumerGroup() {
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
    // since we aren't doing ACKs, if we crash and need to start over from a previously read point,
    // we do so here
    // FIXME: for multiple streams
    // offset.ifPresent(streamEntryID -> System.out.println("Going to set xgroupSetId to " +
    // streamEntryID));
    // offset.ifPresent(streamEntryID -> jedis.xgroupSetID(redisKey, groupId, streamEntryID));
  }

  // TODO: make it work for a map of StreamEntries
  public static class RedisCheckpointMarker implements CheckpointMark, Serializable {

    RedisStreamReader streamReader;

    final ArrayList<StreamEntryID> toAck;

    public RedisCheckpointMarker(RedisStreamReader streamReader, ArrayList<StreamEntryID> toAck) {
      System.out.println("I'm being made!" + hashCode() + " toAck size = " + toAck.size());
      this.streamReader = streamReader;
      this.toAck = new ArrayList<>(toAck);
      System.out.println("StreamReader = " + streamReader.hashCode());
      System.out.println("toAck = " + this.toAck.size() + " for hashCode = " + this.hashCode());
    }

    public StreamEntryID getLastRead() {
      if (this.toAck.size() < 1) {
        return new StreamEntryID();
      } else {
        return this.toAck.get(this.toAck.size() - 1);
      }
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      System.out.println(
          "finalize checkPoint toAck size" + toAck.size() + " hashCode = " + hashCode());
      streamReader.commitAcks(toAck);
    }
  }
}
