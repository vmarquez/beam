package org.apache.beam.sdk.io.cassandra;
/*

*/

import static org.apache.beam.sdk.io.cassandra.CassandraIO.CassandraSource.isMurmur3Partitioner;
import static org.apache.beam.sdk.io.cassandra.CassandraIO.getCluster;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import com.datastax.driver.mapping.MappingManager;
import com.google.auto.value.AutoValue;
import java.math.BigInteger;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.cassandra.CassandraIO.Read.Builder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PardoCassandraIO {

  private static final Logger LOG = LoggerFactory.getLogger(PardoCassandraIO.class);

  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

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
    abstract Class<T> entity();

    @Nullable
    abstract Coder<T> coder();

    @Nullable
    abstract ValueProvider<String> username();

    @Nullable
    abstract ValueProvider<String> password();

    @Nullable
    abstract ValueProvider<String> localDc();

    @Nullable
    abstract ValueProvider<String> consistencyLevel();

    @Nullable
    abstract ValueProvider<Integer> minNumberOfSplits();

    @Nullable
    abstract SerializableFunction<Session, Mapper> mapperFactoryFn();

    abstract Builder<T> builder();

    /** Specify the hosts of the Apache Cassandra instances. */
    public CassandraIO.Read<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null, "hosts can not be null");
      checkArgument(!hosts.isEmpty(), "hosts can not be empty");
      return withHosts(ValueProvider.StaticValueProvider.of(hosts));
    }

    /** Specify the hosts of the Apache Cassandra instances. */
    public CassandraIO.Read<T> withHosts(ValueProvider<List<String>> hosts) {
      return builder().setHosts(hosts).build();
    }

    /** Specify the port number of the Apache Cassandra instances. */
    public CassandraIO.Read<T> withPort(int port) {
      checkArgument(port > 0, "port must be > 0, but was: %s", port);
      return withPort(ValueProvider.StaticValueProvider.of(port));
    }

    /** Specify the port number of the Apache Cassandra instances. */
    public CassandraIO.Read<T> withPort(ValueProvider<Integer> port) {
      return builder().setPort(port).build();
    }

    /** Specify the Cassandra keyspace where to read data. */
    public CassandraIO.Read<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "keyspace can not be null");
      return withKeyspace(ValueProvider.StaticValueProvider.of(keyspace));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public CassandraIO.Read<T> withKeyspace(ValueProvider<String> keyspace) {
      return builder().setKeyspace(keyspace).build();
    }

    /** Specify the Cassandra table where to read data. */
    public CassandraIO.Read<T> withTable(String table) {
      checkArgument(table != null, "table can not be null");
      return withTable(ValueProvider.StaticValueProvider.of(table));
    }

    /** Specify the Cassandra table where to read data. */
    public CassandraIO.Read<T> withTable(ValueProvider<String> table) {
      return builder().setTable(table).build();
    }

    /** Specify the query to read data. */
    public CassandraIO.Read<T> withQuery(String query) {
      checkArgument(query != null && query.length() > 0, "query cannot be null");
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    /** Specify the query to read data. */
    public CassandraIO.Read<T> withQuery(ValueProvider<String> query) {
      return builder().setQuery(query).build();
    }

    /**
     * Specify the entity class (annotated POJO). The {@link CassandraIO} will read the data and
     * convert the data as entity instances. The {@link PCollection} resulting from the read will
     * contains entity elements.
     */
    public CassandraIO.Read<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "entity can not be null");
      return builder().setEntity(entity).build();
    }

    /** Specify the {@link Coder} used to serialize the entity in the {@link PCollection}. */
    public CassandraIO.Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    /** Specify the username for authentication. */
    public CassandraIO.Read<T> withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    /** Specify the username for authentication. */
    public CassandraIO.Read<T> withUsername(ValueProvider<String> username) {
      return builder().setUsername(username).build();
    }

    /** Specify the password used for authentication. */
    public CassandraIO.Read<T> withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    /** Specify the password used for authentication. */
    public CassandraIO.Read<T> withPassword(ValueProvider<String> password) {
      return builder().setPassword(password).build();
    }

    /** Specify the local DC used for the load balancing. */
    public CassandraIO.Read<T> withLocalDc(String localDc) {
      checkArgument(localDc != null, "localDc can not be null");
      return withLocalDc(ValueProvider.StaticValueProvider.of(localDc));
    }

    /** Specify the local DC used for the load balancing. */
    public CassandraIO.Read<T> withLocalDc(ValueProvider<String> localDc) {
      return builder().setLocalDc(localDc).build();
    }

    public CassandraIO.Read<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "consistencyLevel can not be null");
      return withConsistencyLevel(ValueProvider.StaticValueProvider.of(consistencyLevel));
    }

    public CassandraIO.Read<T> withConsistencyLevel(ValueProvider<String> consistencyLevel) {
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public CassandraIO.Read<T> withMinNumberOfSplits(Integer minNumberOfSplits) {
      checkArgument(minNumberOfSplits != null, "minNumberOfSplits can not be null");
      checkArgument(minNumberOfSplits > 0, "minNumberOfSplits must be greater than 0");
      return withMinNumberOfSplits(ValueProvider.StaticValueProvider.of(minNumberOfSplits));
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public CassandraIO.Read<T> withMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits) {
      return builder().setMinNumberOfSplits(minNumberOfSplits).build();
    }

    /**
     * A factory to create a specific {@link Mapper} for a given Cassandra Session. This is useful
     * to provide mappers that don't rely in Cassandra annotated objects.
     */
    public CassandraIO.Read<T> withMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactory) {
      checkArgument(
          mapperFactory != null,
          "CassandraIO.withMapperFactory" + "(withMapperFactory) called with null value");
      return builder().setMapperFactoryFn(mapperFactory).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      //calculate number of splits?
      checkArgument((hosts() != null && port() != null), "WithHosts() and withPort() are required");
      checkArgument(keyspace() != null, "withKeyspace() is required");
      checkArgument(table() != null, "withTable() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");

      try (Cluster cluster =
          getCluster(
              hosts(),
              port(),
              username(),
              password(),
              localDc(),
              consistencyLevel())) {
        if (isMurmur3Partitioner(cluster)) {
          LOG.info("Murmur3Partitioner detected, splitting");

          List<BigInteger> tokens =
                cluster.getMetadata().getTokenRanges().stream()
                    .map(tokenRange -> new BigInteger(tokenRange.getEnd().getValue().toString()))
                    .collect(Collectors.toList());

          SplitGenerator splitGenerator = new SplitGenerator(cluster.getMetadata().getPartitioner());
          PTransform<PCollection<Iterable<RingRange>>, PCollection<T>> transform = ParDo.<PCollection<Iterable<RingRange>>, PCollection<T>>of(new QueryFn(this));
          PCollection<RingRange> ranges = input.apply("Creating initial token splits", Create.of(splitGenerator.generateRingRanges(5, tokens)));
          return ranges.apply("map to grouping function", MapElements
              .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptor
              .of(RingRange.class))).via(rr -> KV.of(rr.getStart().intValue() % 16, rr)))
              .apply("Grouping", GroupByKey.create())
              .apply("map back to lists", MapElements.into(TypeDescriptors.iterables(TypeDescriptor.of(RingRange.class))).via(kv -> kv.getValue()))
              .apply("parallel querying", transform);

        } else {
          LOG.warn(
              "Only Murmur3Partitioner is supported for splitting, using an unique source for "
                  + "the read");
          return null;
          //return Collections.singletonList(
          //    new CassandraIO.CassandraSource<>(spec, Collections.singletonList(buildQuery(spec))));
        }


      }
    }
  }
    public static class QueryFn<T> extends DoFn<Iterable<RingRange>, T> {

    private final Read<T> read;

    transient Cluster cluster;

    transient Session session;

    String partitionKey;

    public QueryFn(Read<T> read) {
      this.read = read;
    }

    @Setup
    public void setup() {
      this.cluster = getCluster(
          read.hosts(),
          read.port(),
          read.username(),
          read.password(),
          read.localDc(),
          read.consistencyLevel());
      this.session = this.cluster.connect(read.keyspace().get()); //FIXME: keyspace from reader
      this.partitionKey =
          cluster.getMetadata().getKeyspace(read.keyspace().get()).getTable(read.table().get())
              .getPartitionKey().stream()
              .map(ColumnMetadata::getName)
              .collect(Collectors.joining(","));
    }

    public void teardown() {
      this.session.close();
      this.cluster.close();
    }

    @ProcessElement
    public void processElement(@Element Iterable<RingRange> tokens, OutputReceiver<T> receiver) {

      Mapper<T> mapper = getMapper(this.session, read.entity());
      String query = generateRangeQuery(this.read, "");
      PreparedStatement preparedStatement = session.prepare(query);

      for (RingRange rr : tokens) {
        Token startToken = cluster.getMetadata().newToken(rr.getStart().toString());
        Token endToken = cluster.getMetadata().newToken(rr.getEnd().toString());
        ResultSet rs = session.execute(preparedStatement.bind().setToken(0, startToken).setToken(1, endToken));
        Iterator<T> iter = mapper.map(rs);
        while(iter.hasNext()) {
          receiver.output(iter.next());
        }
      }
  }

    private Mapper<T> getMapper(Session session, Class<T> entity) {
      return read.mapperFactoryFn().apply(session);
    }

    static String generateRangeQuery(
        PardoCassandraIO.Read spec, String partitionKey) {
      final String rangeFilter =
          Joiner.on(" AND ")
              .skipNulls()
              .join(
                  String.format("(token(%s) >= ?)", partitionKey),
                  String.format("(token(%s) < ?)", partitionKey));
      final String query =
          (spec.query() == null)
              ? buildQuery(spec) + " WHERE " + rangeFilter
              : buildQuery(spec) + " AND " + rangeFilter;
      LOG.debug("CassandraIO generated query : {}", query);
      return query;
    }

    static String buildQuery(PardoCassandraIO.Read spec) {
      return (spec.query() == null)
          ? String.format("SELECT * FROM %s.%s", spec.keyspace().get(), spec.table().get())
          : spec.query().get().toString();
    }


  }

}