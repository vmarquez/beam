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
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.redis.RedisStreamIOUnboundedSource.RedisCheckpointMarker;
import org.joda.time.Instant;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

class RedisStreamReader extends UnboundedReader<StreamEntry> {

  @Override
  public boolean start() throws IOException {
    System.out.println("Start");
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    System.out.println("------\n \n \n -0----");
    if (queue.isEmpty() && !fetchNextBatch()) {
      return false;
    } else {
      System.out.println("queue size = " + queue.size());
      currentStreamEntry = queue.poll();
      return true;
    }
  }

  @Override
  public StreamEntry getCurrent() throws NoSuchElementException {
    toAck.add(currentStreamEntry.getID());
    return currentStreamEntry;
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    return Instant.ofEpochMilli(this.currentStreamEntry.getID().getTime());
  }

  @Override
  public void close() throws IOException {
    jedis.close();
  }

  @Override
  public Instant getWatermark() {
    return Instant.now();
  }

  @Override
  public CheckpointMark getCheckpointMark() {
    ArrayList<StreamEntryID> ids = new ArrayList<>(toAck);
    toAck.clear();
    return new RedisCheckpointMarker(this, ids);
  }

  @Override
  public UnboundedSource<StreamEntry, ?> getCurrentSource() {
    return redisSource;
  }

  public RedisStreamReader(
      String key,
      String groupId,
      RedisStreamIOUnboundedSource source,
      RedisConnectionConfiguration config,
      StreamEntryID startFrom,
      String consumerName) {
    this.redisSource = source;
    this.jedis = new Jedis(config.host().get(), config.port().get());
    this.key = key;
    this.groupId = groupId;
    this.queue = new ArrayDeque<>();
    this.consumerName = consumerName;
  }

  void commitAcks(Collection<StreamEntryID> toAck) {
    System.out.println("toAck = " + toAck.size());
    for (StreamEntryID entry : toAck) {
      jedis.xack(this.key, groupId, entry);
    }
  }

  StreamEntryID lastRead() {
    if (!toAck.isEmpty()) {
      return toAck.get(toAck.size() - 1);
    } else {
      return null;
    }
  }
  /////////////////// PRIVATE /////////////////
  private final Jedis jedis;

  private final RedisStreamIOUnboundedSource redisSource;

  private final String key;

  private final String groupId;

  private final String consumerName;

  private StreamEntry currentStreamEntry = new StreamEntry(new StreamEntryID(), new HashMap<>());

  private final Queue<StreamEntry> queue;

  private boolean readPending = true;

  private final List<StreamEntryID> toAck = new ArrayList<>();

  Map.Entry<String, StreamEntryID> getMapEntryForRead() {
    if (!readPending) {
      return new AbstractMap.SimpleImmutableEntry<>(key, StreamEntryID.UNRECEIVED_ENTRY);
    } else {
      return new AbstractMap.SimpleImmutableEntry<>(
          key, currentStreamEntry.getID());
    }
  }

  private boolean fetchNextBatch() {
    Map.Entry<String, StreamEntryID> mapEntry = getMapEntryForRead();
    System.out.println(
        "fetchNextBatch for "
            + this.hashCode()
            + " XREADGROUP GROUP "
            + groupId
            + " consumername "
            + this.consumerName
            + " COUNT BLOCK 1000 STREAMS "
            + key);
    System.out.println("   with the entry = " + mapEntry);
    List<Entry<String, List<StreamEntry>>> entries =
        Optional.ofNullable(jedis.xreadGroup(groupId, consumerName, 1, 1000, false, mapEntry))
            .orElse(new ArrayList<>());
    // if we aren't flipped to pending, bump it if we're empty
    if (isEmpty(entries) && !readPending) {
      return false;
    } else if (isEmpty(entries)) {
      readPending = false;
      return fetchNextBatch();
    } else {
      queue.addAll(
          entries.stream().flatMap(se -> se.getValue().stream()).collect(Collectors.toList()));
      return true;
    }
  }

  private boolean isEmpty(List<Entry<String, List<StreamEntry>>> list) {
    return list.stream().mapToLong(e -> e.getValue().size()).sum() == 0;
  }
}
