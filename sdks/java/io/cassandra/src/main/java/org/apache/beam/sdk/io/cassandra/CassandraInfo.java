package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Session;
import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

@AutoValue
abstract class CassandraInfo<T> {
  abstract ValueProvider<List<String>> hosts();

  abstract ValueProvider<String> query();

  abstract ValueProvider<Integer> port();

  abstract ValueProvider<String> keyspace();

  abstract ValueProvider<String> table();

  abstract ValueProvider<String> username();

  abstract ValueProvider<String> password();

  abstract ValueProvider<String> localDc();

  abstract ValueProvider<String> consistencyLevel();

  abstract SerializableFunction<Session, Mapper> mapperFactoryFn();

  abstract Class<T> entity();

  public static <T> CassandraInfo<T> Create(ValueProvider<List<String>> hosts,
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
    return new AutoValue_CassandraInfo(hosts, query, port, keyspace, table, username, password, localDc, consistencyLevel, mapperFactoryFn ,entity);
  }

}
