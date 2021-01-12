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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.redis.RedisStreamIOUnboundedSource.RedisCheckpointMarker;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import redis.clients.jedis.StreamEntry;

@Experimental(Kind.SOURCE_SINK)
public class RedisStreamIO {

  @AutoValue
  public abstract static class ReadStream extends PTransform<PBegin, PCollection<StreamEntry>> {

    abstract @Nullable RedisConnectionConfiguration connectionConfiguration();

    abstract @Nullable String keyPattern();

    abstract int batchSize();

    abstract long timeout();

    abstract Builder toBuilder();

    abstract @Nullable String groupId();

    abstract long maxNumRecords();

    abstract String consumerPrefix();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setConnectionConfiguration(RedisConnectionConfiguration connection);

      abstract @Nullable Builder setKeyPattern(String keyPattern);

      abstract Builder setBatchSize(int batchSize);

      abstract Builder setTimeout(long tiemout);

      abstract @Nullable Builder setGroupId(String groupId);

      abstract Builder setMaxNumRecords(long records);

      abstract Builder setConsumerPrefix(String prefix);

      abstract ReadStream build();
    }

    public ReadStream withEndpoint(String host, int port) {
      checkArgument(host != null, "host can not be null");
      checkArgument(0 < port && port < 65536, "port must be a positive integer less than 65536");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withHost(host).withPort(port))
          .build();
    }

    public ReadStream withAuth(String auth) {
      checkArgument(auth != null, "auth can not be null");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withAuth(auth))
          .build();
    }

    public ReadStream withTimeout(int timeout) {
      checkArgument(timeout >= 0, "timeout can not be negative");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withTimeout(timeout))
          .build();
    }

    public ReadStream withKeyPattern(String keyPattern) {
      checkArgument(keyPattern != null, "keyPattern can not be null");
      return toBuilder().setKeyPattern(keyPattern).build();
    }

    public ReadStream withGroupId(String groupId) {
      checkArgument(groupId != null, "groupId can not be null");
      return toBuilder().setGroupId(groupId).build();
    }

    public ReadStream withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "connection can not be null");
      return toBuilder().setConnectionConfiguration(connection).build();
    }

    public ReadStream withBatchSize(int batchSize) {
      return toBuilder().setBatchSize(batchSize).build();
    }

    public ReadStream withMaxNumRecords(long recordSize) {
      return toBuilder().setMaxNumRecords(recordSize).build();
    }

    public ReadStream withConsumerPrefix(String prefix) {
      return toBuilder().setConsumerPrefix(prefix).build();
    }

    @Override
    public PCollection<StreamEntry> expand(PBegin input) {
      checkArgument(connectionConfiguration() != null, "withConnectionConfiguration is required");
      System.out.println("-------- woot here we go --------------------- -" + keyPattern());
      UnboundedSource<StreamEntry, RedisCheckpointMarker> unbounded =
          new RedisStreamIOUnboundedSource(
              keyPattern(), timeout(), groupId(), connectionConfiguration(), consumerPrefix());
      if (this.maxNumRecords() == Long.MAX_VALUE) {
        return input.apply("creating source", org.apache.beam.sdk.io.Read.from(unbounded));
      } else {
        return input.apply(
            "creating source",
            org.apache.beam.sdk.io.Read.from(unbounded)
                .withMaxNumRecords(maxNumRecords())
                .withMaxReadTime(Duration.standardSeconds(30)));
      }
    }
  }

  public static ReadStream read() {
    return new AutoValue_RedisStreamIO_ReadStream.Builder()
        .setConnectionConfiguration(RedisConnectionConfiguration.create())
        .setTimeout(1000)
        .setBatchSize(1)
        .setConsumerPrefix("beam_consumer_prefix")
        .setMaxNumRecords(Long.MAX_VALUE)
        .build();
  }
}
