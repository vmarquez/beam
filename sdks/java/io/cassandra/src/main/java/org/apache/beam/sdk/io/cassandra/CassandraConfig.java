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
package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Session;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

@AutoValue
abstract class CassandraConfig<T> implements Serializable {
  @Nullable
  abstract ValueProvider<List<String>> hosts();

  @Nullable
  abstract ValueProvider<String> query();

  @Nullable
  abstract ValueProvider<Integer> port();

  @Nullable
  abstract ValueProvider<String> keyspace();

  @Nullable
  abstract ValueProvider<String> table();

  @Nullable
  abstract ValueProvider<String> username();

  @Nullable
  abstract ValueProvider<String> password();

  @Nullable
  abstract ValueProvider<String> localDc();

  @Nullable
  abstract ValueProvider<String> consistencyLevel();

  @Nullable
  abstract SerializableFunction<Session, Mapper> mapperFactoryFn();

  @Nullable
  abstract Class<T> entity();

  public static <T> CassandraConfig<T> create(
      ValueProvider<List<String>> hosts,
      ValueProvider<String> query,
      ValueProvider<Integer> port,
      ValueProvider<String> keyspace,
      ValueProvider<String> table,
      ValueProvider<String> username,
      ValueProvider<String> password,
      ValueProvider<String> localDc,
      ValueProvider<String> consistencyLevel,
      SerializableFunction<Session, Mapper> mapperFactoryFn,
      Class<T> entity) {
    return new AutoValue_CassandraConfig(
        hosts,
        query,
        port,
        keyspace,
        table,
        username,
        password,
        localDc,
        consistencyLevel,
        mapperFactoryFn,
        entity);
  }
}
