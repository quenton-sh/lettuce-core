/*
 * Copyright 2011-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.cluster;

import java.io.Closeable;
import java.net.SocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import reactor.core.publisher.Mono;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.NodeSelectionSupport;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.topology.ClusterTopologyRefresh;
import io.lettuce.core.cluster.topology.NodeConnectionFactory;
import io.lettuce.core.cluster.topology.TopologyComparators;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.internal.Futures;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceLists;
import io.lettuce.core.models.command.CommandDetailParser;
import io.lettuce.core.output.KeyValueStreamingChannel;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.*;
import io.lettuce.core.pubsub.PubSubCommandHandler;
import io.lettuce.core.pubsub.PubSubEndpoint;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnectionImpl;
import io.lettuce.core.resource.ClientResources;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * A scalable and thread-safe <a href="http://redis.io/">Redis</a> cluster client supporting synchronous, asynchronous and
 * reactive execution models. Multiple threads may share one connection. The cluster client handles command routing based on the
 * first key of the command and maintains a view of the cluster that is available when calling the {@link #getPartitions()}
 * method.
 *
 * <p>
 * Connections to the cluster members are opened on the first access to the cluster node and managed by the
 * {@link StatefulRedisClusterConnection}. You should not use transactional commands on cluster connections since {@code MULTI},
 * {@code EXEC} and {@code DISCARD} have no key and cannot be assigned to a particular node. A cluster connection uses a default
 * connection to run non-keyed commands.
 * </p>
 * <p>
 * The Redis cluster client provides a {@link RedisAdvancedClusterCommands sync}, {@link RedisAdvancedClusterAsyncCommands
 * async} and {@link io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands reactive} API.
 * </p>
 *
 * <p>
 * Connections to particular nodes can be obtained by {@link StatefulRedisClusterConnection#getConnection(String)} providing the
 * node id or {@link StatefulRedisClusterConnection#getConnection(String, int)} by host and port.
 * </p>
 *
 * <p>
 * <a href="http://redis.io/topics/cluster-spec#multiple-keys-operations">Multiple keys operations</a> have to operate on a key
 * that hashes to the same slot. Following commands do not need to follow that rule since they are pipelined according to its
 * hash value to multiple nodes in parallel on the sync, async and, reactive API:
 * </p>
 * <ul>
 * <li>{@link RedisAdvancedClusterAsyncCommands#del(Object[]) DEL}</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#unlink(Object[]) UNLINK}</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#mget(Object[]) MGET}</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#mget(KeyValueStreamingChannel, Object[])} ) MGET with streaming}</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#mset(Map) MSET}</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#msetnx(Map) MSETNX}</li>
 * </ul>
 *
 * <p>
 * Following commands on the Cluster sync, async and, reactive API are implemented with a Cluster-flavor:
 * </p>
 * <ul>
 * <li>{@link RedisAdvancedClusterAsyncCommands#clientSetname(Object)} Executes {@code CLIENT SET} on all connections and
 * initializes new connections with the {@code clientName}.</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#flushall()} Run {@code FLUSHALL} on all master nodes.</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#flushdb()} Executes {@code FLUSHDB} on all master nodes.</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#keys(Object)} Executes {@code KEYS} on all.</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#randomkey()} Returns a random key from a random master node.</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#scriptFlush()} Executes {@code SCRIPT FLUSH} on all nodes.</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#scriptKill()} Executes {@code SCRIPT KILL} on all nodes.</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#shutdown(boolean)} Executes {@code SHUTDOWN} on all nodes.</li>
 * <li>{@link RedisAdvancedClusterAsyncCommands#scan()} Executes a {@code SCAN} on all nodes according to {@link ReadFrom}. The
 * resulting cursor must be reused across the {@code SCAN} to scan iteratively across the whole cluster.</li>
 * </ul>
 *
 * <p>
 * Cluster commands can be issued to multiple hosts in parallel by using the {@link NodeSelectionSupport} API. A set of nodes is
 * selected using a {@link java.util.function.Predicate} and commands can be issued to the node selection
 *
 * <pre class="code">
 * AsyncExecutions&lt;String&gt; ping = commands.masters().commands().ping();
 * Collection&lt;RedisClusterNode&gt; nodes = ping.nodes();
 * nodes.stream().forEach(redisClusterNode -&gt; ping.get(redisClusterNode));
 * </pre>
 *
 * </p>
 *
 * {@link RedisClusterClient} is an expensive resource. Reuse this instance or share external {@link ClientResources} as much as
 * possible.
 *
 * @author Mark Paluch
 * @since 3.0
 * @see RedisURI
 * @see StatefulRedisClusterConnection
 * @see RedisCodec
 * @see ClusterClientOptions
 * @see ClientResources
 */
public class RedisClusterClient extends AbstractRedisClient {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisClusterClient.class);

    protected final AtomicBoolean clusterTopologyRefreshActivated = new AtomicBoolean(false);

    protected final AtomicReference<ScheduledFuture<?>> clusterTopologyRefreshFuture = new AtomicReference<>();

    private final ClusterTopologyRefresh refresh = new ClusterTopologyRefresh(new NodeConnectionFactoryImpl(), getResources());

    private final ClusterTopologyRefreshScheduler clusterTopologyRefreshScheduler = new ClusterTopologyRefreshScheduler(this,
            getResources());

    private final Iterable<RedisURI> initialUris;

    private Partitions partitions;

    /**
     * Non-private constructor to make {@link RedisClusterClient} proxyable.
     */
    protected RedisClusterClient() {

        super(null);

        initialUris = Collections.emptyList();
    }

    /**
     * Initialize the client with a list of cluster URI's. All uris are tried in sequence for connecting initially to the
     * cluster. If any uri is successful for connection, the others are not tried anymore. The initial uri is needed to discover
     * the cluster structure for distributing the requests.
     *
     * @param clientResources the client resources. If {@code null}, the client will create a new dedicated instance of client
     *        resources and keep track of them.
     * @param redisURIs iterable of initial {@link RedisURI cluster URIs}. Must not be {@code null} and not empty.
     */
    protected RedisClusterClient(ClientResources clientResources, Iterable<RedisURI> redisURIs) {

        super(clientResources);

        assertNotEmpty(redisURIs);
        assertSameOptions(redisURIs);

        this.initialUris = Collections.unmodifiableList(LettuceLists.newList(redisURIs));

        setDefaultTimeout(getFirstUri().getTimeout());
        setOptions(ClusterClientOptions.builder().build());
    }

    private static void assertSameOptions(Iterable<RedisURI> redisURIs) {

        Boolean ssl = null;
        Boolean startTls = null;
        Boolean verifyPeer = null;

        for (RedisURI redisURI : redisURIs) {

            if (ssl == null) {
                ssl = redisURI.isSsl();
            }
            if (startTls == null) {
                startTls = redisURI.isStartTls();
            }
            if (verifyPeer == null) {
                verifyPeer = redisURI.isVerifyPeer();
            }

            if (ssl.booleanValue() != redisURI.isSsl()) {
                throw new IllegalArgumentException(
                        "RedisURI " + redisURI + " SSL is not consistent with the other seed URI SSL settings");
            }

            if (startTls.booleanValue() != redisURI.isStartTls()) {
                throw new IllegalArgumentException(
                        "RedisURI " + redisURI + " StartTLS is not consistent with the other seed URI StartTLS settings");
            }

            if (verifyPeer.booleanValue() != redisURI.isVerifyPeer()) {
                throw new IllegalArgumentException(
                        "RedisURI " + redisURI + " VerifyPeer is not consistent with the other seed URI VerifyPeer settings");
            }
        }
    }

    /**
     * Create a new client that connects to the supplied {@link RedisURI uri} with default {@link ClientResources}. You can
     * connect to different Redis servers but you must supply a {@link RedisURI} on connecting.
     *
     * @param redisURI the Redis URI, must not be {@code null}.
     * @return a new instance of {@link RedisClusterClient}.
     */
    public static RedisClusterClient create(RedisURI redisURI) {
        assertNotNull(redisURI);
        return create(Collections.singleton(redisURI));
    }

    /**
     * Create a new client that connects to the supplied {@link RedisURI uri} with default {@link ClientResources}. You can
     * connect to different Redis servers but you must supply a {@link RedisURI} on connecting.
     *
     * @param redisURIs one or more Redis URI, must not be {@code null} and not empty.
     * @return a new instance of {@link RedisClusterClient}.
     */
    public static RedisClusterClient create(Iterable<RedisURI> redisURIs) {
        assertNotEmpty(redisURIs);
        assertSameOptions(redisURIs);
        return new RedisClusterClient(null, redisURIs);
    }

    /**
     * Create a new client that connects to the supplied uri with default {@link ClientResources}. You can connect to different
     * Redis servers but you must supply a {@link RedisURI} on connecting.
     *
     * @param uri the Redis URI, must not be empty or {@code null}.
     * @return a new instance of {@link RedisClusterClient}.
     */
    public static RedisClusterClient create(String uri) {
        LettuceAssert.notEmpty(uri, "URI must not be empty");
        return create(RedisClusterURIUtil.toRedisURIs(URI.create(uri)));
    }

    /**
     * Create a new client that connects to the supplied {@link RedisURI uri} with shared {@link ClientResources}. You need to
     * shut down the {@link ClientResources} upon shutting down your application.You can connect to different Redis servers but
     * you must supply a {@link RedisURI} on connecting.
     *
     * @param clientResources the client resources, must not be {@code null}.
     * @param redisURI the Redis URI, must not be {@code null}.
     * @return a new instance of {@link RedisClusterClient}.
     */
    public static RedisClusterClient create(ClientResources clientResources, RedisURI redisURI) {
        assertNotNull(clientResources);
        assertNotNull(redisURI);
        return create(clientResources, Collections.singleton(redisURI));
    }

    /**
     * Create a new client that connects to the supplied uri with shared {@link ClientResources}.You need to shut down the
     * {@link ClientResources} upon shutting down your application. You can connect to different Redis servers but you must
     * supply a {@link RedisURI} on connecting.
     *
     * @param clientResources the client resources, must not be {@code null}.
     * @param uri the Redis URI, must not be empty or {@code null}.
     * @return a new instance of {@link RedisClusterClient}.
     */
    public static RedisClusterClient create(ClientResources clientResources, String uri) {
        assertNotNull(clientResources);
        LettuceAssert.notEmpty(uri, "URI must not be empty");
        return create(clientResources, RedisClusterURIUtil.toRedisURIs(URI.create(uri)));
    }

    /**
     * Create a new client that connects to the supplied {@link RedisURI uri} with shared {@link ClientResources}. You need to
     * shut down the {@link ClientResources} upon shutting down your application.You can connect to different Redis servers but
     * you must supply a {@link RedisURI} on connecting.
     *
     * @param clientResources the client resources, must not be {@code null}.
     * @param redisURIs one or more Redis URI, must not be {@code null} and not empty.
     * @return a new instance of {@link RedisClusterClient}.
     */
    public static RedisClusterClient create(ClientResources clientResources, Iterable<RedisURI> redisURIs) {
        assertNotNull(clientResources);
        assertNotEmpty(redisURIs);
        assertSameOptions(redisURIs);
        return new RedisClusterClient(clientResources, redisURIs);
    }

    /**
     * Connect to a Redis Cluster and treat keys and values as UTF-8 strings.
     * <p>
     * What to expect from this connection:
     * </p>
     * <ul>
     * <li>A <i>default</i> connection is created to the node with the lowest latency</li>
     * <li>Keyless commands are send to the default connection</li>
     * <li>Single-key keyspace commands are routed to the appropriate node</li>
     * <li>Multi-key keyspace commands require the same slot-hash and are routed to the appropriate node</li>
     * <li>Pub/sub commands are sent to the node that handles the slot derived from the pub/sub channel</li>
     * </ul>
     *
     * @return A new stateful Redis Cluster connection.
     */
    public StatefulRedisClusterConnection<String, String> connect() {
        return connect(newStringStringCodec());
    }

    /**
     * Connect to a Redis Cluster. Use the supplied {@link RedisCodec codec} to encode/decode keys and values.
     * <p>
     * What to expect from this connection:
     * </p>
     * <ul>
     * <li>A <i>default</i> connection is created to the node with the lowest latency</li>
     * <li>Keyless commands are send to the default connection</li>
     * <li>Single-key keyspace commands are routed to the appropriate node</li>
     * <li>Multi-key keyspace commands require the same slot-hash and are routed to the appropriate node</li>
     * <li>Pub/sub commands are sent to the node that handles the slot derived from the pub/sub channel</li>
     * </ul>
     *
     * @param codec Use this codec to encode/decode keys and values, must not be {@code null}.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return A new stateful Redis Cluster connection.
     */
    @SuppressWarnings("unchecked")
    public <K, V> StatefulRedisClusterConnection<K, V> connect(RedisCodec<K, V> codec) {

        if (partitions == null) {
            initializePartitions();
        }

        return getConnection(connectClusterAsync(codec));
    }

    /**
     * Connect asynchronously to a Redis Cluster. Use the supplied {@link RedisCodec codec} to encode/decode keys and values.
     * Connecting asynchronously requires an initialized topology. Call {@link #getPartitions()} first, otherwise the connect
     * will fail with a{@link IllegalStateException}.
     * <p>
     * What to expect from this connection:
     * </p>
     * <ul>
     * <li>A <i>default</i> connection is created to the node with the lowest latency</li>
     * <li>Keyless commands are send to the default connection</li>
     * <li>Single-key keyspace commands are routed to the appropriate node</li>
     * <li>Multi-key keyspace commands require the same slot-hash and are routed to the appropriate node</li>
     * <li>Pub/sub commands are sent to the node that handles the slot derived from the pub/sub channel</li>
     * </ul>
     *
     * @param codec Use this codec to encode/decode keys and values, must not be {@code null}.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return a {@link CompletableFuture} that is notified with the connection progress.
     * @since 5.1
     */
    @SuppressWarnings("unchecked")
    public <K, V> CompletableFuture<StatefulRedisClusterConnection<K, V>> connectAsync(RedisCodec<K, V> codec) {
        return transformAsyncConnectionException(connectClusterAsync(codec), getInitialUris());
    }

    /**
     * Connect to a Redis Cluster using pub/sub connections and treat keys and values as UTF-8 strings.
     * <p>
     * What to expect from this connection:
     * </p>
     * <ul>
     * <li>A <i>default</i> connection is created to the node with the least number of clients</li>
     * <li>Pub/sub commands are sent to the node with the least number of clients</li>
     * <li>Keyless commands are send to the default connection</li>
     * <li>Single-key keyspace commands are routed to the appropriate node</li>
     * <li>Multi-key keyspace commands require the same slot-hash and are routed to the appropriate node</li>
     * </ul>
     *
     * @return A new stateful Redis Cluster connection.
     */
    public StatefulRedisClusterPubSubConnection<String, String> connectPubSub() {
        return connectPubSub(newStringStringCodec());
    }

    /**
     * Connect to a Redis Cluster using pub/sub connections. Use the supplied {@link RedisCodec codec} to encode/decode keys and
     * values.
     * <p>
     * What to expect from this connection:
     * </p>
     * <ul>
     * <li>A <i>default</i> connection is created to the node with the least number of clients</li>
     * <li>Pub/sub commands are sent to the node with the least number of clients</li>
     * <li>Keyless commands are send to the default connection</li>
     * <li>Single-key keyspace commands are routed to the appropriate node</li>
     * <li>Multi-key keyspace commands require the same slot-hash and are routed to the appropriate node</li>
     * </ul>
     *
     * @param codec Use this codec to encode/decode keys and values, must not be {@code null}.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return A new stateful Redis Cluster connection.
     */
    @SuppressWarnings("unchecked")
    public <K, V> StatefulRedisClusterPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec) {

        if (partitions == null) {
            initializePartitions();
        }

        return getConnection(connectClusterPubSubAsync(codec));
    }

    /**
     * Connect asynchronously to a Redis Cluster using pub/sub connections. Use the supplied {@link RedisCodec codec} to
     * encode/decode keys and values. Connecting asynchronously requires an initialized topology. Call {@link #getPartitions()}
     * first, otherwise the connect will fail with a{@link IllegalStateException}.
     * <p>
     * What to expect from this connection:
     * </p>
     * <ul>
     * <li>A <i>default</i> connection is created to the node with the least number of clients</li>
     * <li>Pub/sub commands are sent to the node with the least number of clients</li>
     * <li>Keyless commands are send to the default connection</li>
     * <li>Single-key keyspace commands are routed to the appropriate node</li>
     * <li>Multi-key keyspace commands require the same slot-hash and are routed to the appropriate node</li>
     * </ul>
     *
     * @param codec Use this codec to encode/decode keys and values, must not be {@code null}.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return a {@link CompletableFuture} that is notified with the connection progress.
     * @since 5.1
     */
    @SuppressWarnings("unchecked")
    public <K, V> CompletableFuture<StatefulRedisClusterPubSubConnection<K, V>> connectPubSubAsync(RedisCodec<K, V> codec) {
        return transformAsyncConnectionException(connectClusterPubSubAsync(codec), getInitialUris());
    }

    StatefulRedisConnection<String, String> connectToNode(SocketAddress socketAddress) {
        return connectToNode(newStringStringCodec(), socketAddress.toString(), null, Mono.just(socketAddress));
    }

    /**
     * Create a connection to a redis socket address.
     *
     * @param codec Use this codec to encode/decode keys and values, must not be {@code null}.
     * @param nodeId the nodeId.
     * @param clusterWriter global cluster writer.
     * @param socketAddressSupplier supplier for the socket address.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return A new connection.
     */
    <K, V> StatefulRedisConnection<K, V> connectToNode(RedisCodec<K, V> codec, String nodeId, RedisChannelWriter clusterWriter,
            Mono<SocketAddress> socketAddressSupplier) {
        return getConnection(connectToNodeAsync(codec, nodeId, clusterWriter, socketAddressSupplier));
    }

    /**
     * Create a connection to a redis socket address.
     *
     * @param codec Use this codec to encode/decode keys and values, must not be {@code null}.
     * @param nodeId the nodeId.
     * @param clusterWriter global cluster writer.
     * @param socketAddressSupplier supplier for the socket address.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return A new connection.
     */
    <K, V> ConnectionFuture<StatefulRedisConnection<K, V>> connectToNodeAsync(RedisCodec<K, V> codec, String nodeId,
            RedisChannelWriter clusterWriter, Mono<SocketAddress> socketAddressSupplier) {

        assertNotNull(codec);
        assertNotEmpty(initialUris);
        LettuceAssert.notNull(socketAddressSupplier, "SocketAddressSupplier must not be null");

        ClusterNodeEndpoint endpoint = new ClusterNodeEndpoint(clientOptions, getResources(), clusterWriter);

        RedisChannelWriter writer = endpoint;

        if (CommandExpiryWriter.isSupported(clientOptions)) {
            writer = new CommandExpiryWriter(writer, clientOptions, clientResources);
        }

        StatefulRedisConnectionImpl<K, V> connection = new StatefulRedisConnectionImpl<>(writer, codec, timeout);

        ConnectionFuture<StatefulRedisConnection<K, V>> connectionFuture = connectStatefulAsync(connection, codec, endpoint,
                getFirstUri(), socketAddressSupplier, () -> new CommandHandler(clientOptions, clientResources, endpoint));

        return connectionFuture.whenComplete((conn, throwable) -> {
            if (throwable != null) {
                connection.close();
            }
        });
    }

    /**
     * Create a pub/sub connection to a redis socket address.
     *
     * @param codec Use this codec to encode/decode keys and values, must not be {@code null}.
     * @param nodeId the nodeId.
     * @param socketAddressSupplier supplier for the socket address.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return A new connection.
     */
    <K, V> ConnectionFuture<StatefulRedisPubSubConnection<K, V>> connectPubSubToNodeAsync(RedisCodec<K, V> codec, String nodeId,
            Mono<SocketAddress> socketAddressSupplier) {

        assertNotNull(codec);
        assertNotEmpty(initialUris);

        LettuceAssert.notNull(socketAddressSupplier, "SocketAddressSupplier must not be null");

        logger.debug("connectPubSubToNode(" + nodeId + ")");

        PubSubEndpoint<K, V> endpoint = new PubSubEndpoint<>(clientOptions, clientResources);

        RedisChannelWriter writer = endpoint;

        if (CommandExpiryWriter.isSupported(clientOptions)) {
            writer = new CommandExpiryWriter(writer, clientOptions, clientResources);
        }

        StatefulRedisPubSubConnectionImpl<K, V> connection = new StatefulRedisPubSubConnectionImpl<>(endpoint, writer, codec,
                timeout);

        ConnectionFuture<StatefulRedisPubSubConnection<K, V>> connectionFuture = connectStatefulAsync(connection, codec,
                endpoint, getFirstUri(), socketAddressSupplier,
                () -> new PubSubCommandHandler<>(clientOptions, clientResources, codec, endpoint));
        return connectionFuture.whenComplete((conn, throwable) -> {
            if (throwable != null) {
                connection.close();
            }
        });
    }

    /**
     * Create a clustered pub/sub connection with command distributor.
     *
     * @param codec Use this codec to encode/decode keys and values, must not be {@code null}.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return a new connection.
     */
    // SQ: 建连 0 / 4，向集群中 client 连接数最少的结点建连
    private <K, V> CompletableFuture<StatefulRedisClusterConnection<K, V>> connectClusterAsync(RedisCodec<K, V> codec) {

        if (partitions == null) {
            return Futures.failed(new IllegalStateException(
                    "Partitions not initialized. Initialize via RedisClusterClient.getPartitions()."));
        }

        // SQ: 如果设置了定时刷新视图 ClusterTopologyRefreshOptions.periodicRefreshEnabled=true，则启动定时任务
        activateTopologyRefreshIfNeeded();

        logger.debug("connectCluster(" + initialUris + ")");

        // SQ: 根据集群拓扑视图，从中选取 client 连接数最少的结点建连 (在 TopologyRefresh 用 info clients 命令 获取的结果)
        Mono<SocketAddress> socketAddressSupplier = getSocketAddressSupplier(TopologyComparators::sortByClientCount);

        DefaultEndpoint endpoint = new DefaultEndpoint(clientOptions, clientResources);
        RedisChannelWriter writer = endpoint;

        if (CommandExpiryWriter.isSupported(clientOptions)) {
            writer = new CommandExpiryWriter(writer, clientOptions, clientResources);
        }

        ClusterDistributionChannelWriter clusterWriter = new ClusterDistributionChannelWriter(clientOptions, writer,
                clusterTopologyRefreshScheduler);
        PooledClusterConnectionProvider<K, V> pooledClusterConnectionProvider = new PooledClusterConnectionProvider<>(this,
                clusterWriter, codec, clusterTopologyRefreshScheduler);

        clusterWriter.setClusterConnectionProvider(pooledClusterConnectionProvider);

        StatefulRedisClusterConnectionImpl<K, V> connection = new StatefulRedisClusterConnectionImpl<>(clusterWriter, codec,
                timeout);

        connection.setReadFrom(ReadFrom.MASTER);
        connection.setPartitions(partitions);

        Supplier<CommandHandler> commandHandlerSupplier = () -> new CommandHandler(clientOptions, clientResources, endpoint);

        Mono<StatefulRedisClusterConnectionImpl<K, V>> connectionMono = Mono
                .defer(() -> connect(socketAddressSupplier, codec, endpoint, connection, commandHandlerSupplier));

        // SQ: 添加轮询重试，向 client 连接数最少的结点建连失败，则尝试下一个结点，总共的尝试次数等于结点数，每个结点都有机会
        for (int i = 1; i < getConnectionAttempts(); i++) {
            connectionMono = connectionMono
                    .onErrorResume(t -> connect(socketAddressSupplier, codec, endpoint, connection, commandHandlerSupplier));
        }

        return connectionMono.flatMap(c -> c.reactive().command().collectList() //
                .map(CommandDetailParser::parse) //
                .doOnNext(detail -> c.setState(new RedisState(detail))) //
                .doOnError(e -> c.setState(new RedisState(Collections.emptyList()))).then(Mono.just(c))
                .onErrorResume(RedisCommandExecutionException.class, e -> Mono.just(c)))
                .doOnNext(
                        c -> connection.registerCloseables(closeableResources, clusterWriter, pooledClusterConnectionProvider))
                .map(it -> (StatefulRedisClusterConnection<K, V>) it).toFuture();
    }

    // SQ: 建连 1 / 4
    private <T, K, V> Mono<T> connect(Mono<SocketAddress> socketAddressSupplier, RedisCodec<K, V> codec,
            DefaultEndpoint endpoint, RedisChannelHandler<K, V> connection, Supplier<CommandHandler> commandHandlerSupplier) {

        ConnectionFuture<T> future = connectStatefulAsync(connection, codec, endpoint, getFirstUri(), socketAddressSupplier,
                commandHandlerSupplier);

        return Mono.fromCompletionStage(future).doOnError(t -> logger.warn(t.getMessage()));
    }

    /**
     * Create a clustered connection with command distributor.
     *
     * @param codec Use this codec to encode/decode keys and values, must not be {@code null}.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return a new connection.
     */
    private <K, V> CompletableFuture<StatefulRedisClusterPubSubConnection<K, V>> connectClusterPubSubAsync(
            RedisCodec<K, V> codec) {

        if (partitions == null) {
            return Futures.failed(new IllegalStateException(
                    "Partitions not initialized. Initialize via RedisClusterClient.getPartitions()."));
        }

        activateTopologyRefreshIfNeeded();

        logger.debug("connectClusterPubSub(" + initialUris + ")");

        Mono<SocketAddress> socketAddressSupplier = getSocketAddressSupplier(TopologyComparators::sortByClientCount);

        PubSubClusterEndpoint<K, V> endpoint = new PubSubClusterEndpoint<>(clientOptions, clientResources);
        RedisChannelWriter writer = endpoint;

        if (CommandExpiryWriter.isSupported(clientOptions)) {
            writer = new CommandExpiryWriter(writer, clientOptions, clientResources);
        }

        ClusterDistributionChannelWriter clusterWriter = new ClusterDistributionChannelWriter(clientOptions, writer,
                clusterTopologyRefreshScheduler);

        StatefulRedisClusterPubSubConnectionImpl<K, V> connection = new StatefulRedisClusterPubSubConnectionImpl<>(endpoint,
                clusterWriter, codec, timeout);

        ClusterPubSubConnectionProvider<K, V> pooledClusterConnectionProvider = new ClusterPubSubConnectionProvider<>(this,
                clusterWriter, codec, connection.getUpstreamListener(), clusterTopologyRefreshScheduler);

        clusterWriter.setClusterConnectionProvider(pooledClusterConnectionProvider);
        connection.setPartitions(partitions);

        Supplier<CommandHandler> commandHandlerSupplier = () -> new PubSubCommandHandler<>(clientOptions, clientResources,
                codec, endpoint);

        Mono<StatefulRedisClusterPubSubConnectionImpl<K, V>> connectionMono = Mono
                .defer(() -> connect(socketAddressSupplier, codec, endpoint, connection, commandHandlerSupplier));

        for (int i = 1; i < getConnectionAttempts(); i++) {
            connectionMono = connectionMono
                    .onErrorResume(t -> connect(socketAddressSupplier, codec, endpoint, connection, commandHandlerSupplier));
        }

        return connectionMono.flatMap(c -> c.reactive().command().collectList() //
                .map(CommandDetailParser::parse) //
                .doOnNext(detail -> c.setState(new RedisState(detail)))
                .doOnError(e -> c.setState(new RedisState(Collections.emptyList()))).then(Mono.just(c))
                .onErrorResume(RedisCommandExecutionException.class, e -> Mono.just(c)))
                .doOnNext(
                        c -> connection.registerCloseables(closeableResources, clusterWriter, pooledClusterConnectionProvider))
                .map(it -> (StatefulRedisClusterPubSubConnection<K, V>) it).toFuture();
    }

    private int getConnectionAttempts() {
        return Math.max(1, partitions.size());
    }

    /**
     * Initiates a channel connection considering {@link ClientOptions} initialization options, authentication and client name
     * options.
     */
    // SQ: 建连 2 / 4
    private <K, V, T extends RedisChannelHandler<K, V>, S> ConnectionFuture<S> connectStatefulAsync(T connection,
            RedisCodec<K, V> codec, DefaultEndpoint endpoint, RedisURI connectionSettings,
            Mono<SocketAddress> socketAddressSupplier, Supplier<CommandHandler> commandHandlerSupplier) {

        ConnectionBuilder connectionBuilder = createConnectionBuilder(connection, endpoint, connectionSettings,
                socketAddressSupplier, commandHandlerSupplier);

        if (clientOptions.isPingBeforeActivateConnection()) {
            if (hasPassword(connectionSettings)) {
                connectionBuilder.enableAuthPingBeforeConnect();
            } else {
                connectionBuilder.enablePingBeforeConnect();
            }
        }

        ConnectionFuture<RedisChannelHandler<K, V>> future = initializeChannelAsync(connectionBuilder);
        ConnectionFuture<?> sync = future;

        if (!clientOptions.isPingBeforeActivateConnection() && hasPassword(connectionSettings)) {

            sync = sync.thenCompose(channelHandler -> {

                // SQ: 如果设置了密码，则在建连完成后发送 AUTH 命令，取 AUTH 命令的结果作为返回值
                CommandArgs<K, V> args = new CommandArgs<>(codec).add(connectionSettings.getPassword());
                AsyncCommand<K, V, String> command = new AsyncCommand<>(
                        new Command<>(CommandType.AUTH, new StatusOutput<>(codec), args));

                if (connection instanceof StatefulRedisClusterConnectionImpl) {
                    ((StatefulRedisClusterConnectionImpl) connection).dispatch(command);
                }

                if (connection instanceof StatefulRedisConnectionImpl) {
                    ((StatefulRedisConnectionImpl) connection).dispatch(command);
                }

                return command;
            });

        }

        if (LettuceStrings.isNotEmpty(connectionSettings.getClientName())) {
            sync = sync.thenApply(channelHandler -> {

                if (connection instanceof StatefulRedisClusterConnectionImpl) {
                    ((StatefulRedisClusterConnectionImpl) connection).setClientName(connectionSettings.getClientName());
                }

                if (connection instanceof StatefulRedisConnectionImpl) {
                    ((StatefulRedisConnectionImpl) connection).setClientName(connectionSettings.getClientName());
                }
                return channelHandler;
            });
        }

        return sync.thenApply(channelHandler -> (S) connection);
    }

    private boolean hasPassword(RedisURI connectionSettings) {
        return connectionSettings.getPassword() != null && connectionSettings.getPassword().length != 0;
    }

    private <K, V> ConnectionBuilder createConnectionBuilder(RedisChannelHandler<K, V> connection, DefaultEndpoint endpoint,
            RedisURI connectionSettings, Mono<SocketAddress> socketAddressSupplier,
            Supplier<CommandHandler> commandHandlerSupplier) {

        ConnectionBuilder connectionBuilder;
        if (connectionSettings.isSsl()) {
            SslConnectionBuilder sslConnectionBuilder = SslConnectionBuilder.sslConnectionBuilder();
            sslConnectionBuilder.ssl(connectionSettings);
            connectionBuilder = sslConnectionBuilder;
        } else {
            connectionBuilder = ConnectionBuilder.connectionBuilder();
        }

        connectionBuilder.reconnectionListener(new ReconnectEventListener(clusterTopologyRefreshScheduler));
        connectionBuilder.clientOptions(clientOptions);
        connectionBuilder.connection(connection);
        connectionBuilder.clientResources(clientResources);
        connectionBuilder.endpoint(endpoint);
        connectionBuilder.commandHandler(commandHandlerSupplier);
        connectionBuilder(socketAddressSupplier, connectionBuilder, connectionSettings);
        channelType(connectionBuilder, connectionSettings);
        return connectionBuilder;
    }

    /**
     * Reload partitions and re-initialize the distribution table.
     */
    // SQ: 给 ClusterTopologyRefreshOptions.periodicRefreshEnabled=true 定时任务调用的方法
    public void reloadPartitions() {

        if (partitions == null) {
            initializePartitions();
            partitions.updateCache();
        } else {

            Partitions loadedPartitions = loadPartitions();
            if (TopologyComparators.isChanged(getPartitions(), loadedPartitions)) {

                logger.debug("Using a new cluster topology");

                List<RedisClusterNode> before = new ArrayList<>(getPartitions());
                List<RedisClusterNode> after = new ArrayList<>(loadedPartitions);

                getResources().eventBus().publish(new ClusterTopologyChangedEvent(before, after));
            }

            this.partitions.reload(loadedPartitions.getPartitions());
        }

        // SQ: 更新所有已经创建的连接对 partitions 的引用
        updatePartitionsInConnections();
    }

    protected void updatePartitionsInConnections() {

        forEachClusterConnection(input -> {
            input.setPartitions(partitions);
        });

        forEachClusterPubSubConnection(input -> {
            input.setPartitions(partitions);
        });
    }

    protected void initializePartitions() {
        this.partitions = loadPartitions();
    }

    /**
     * Retrieve the cluster view. Partitions are shared amongst all connections opened by this client instance.
     *
     * @return the partitions.
     */
    public Partitions getPartitions() {
        if (partitions == null) {
            initializePartitions();
        }
        return partitions;
    }

    /**
     * Retrieve partitions. Nodes within {@link Partitions} are ordered by latency. Lower latency nodes come first.
     *
     * @return Partitions.
     */
    protected Partitions loadPartitions() {

        // SQ: 挑选出一组 seed 结点用于获取 集群拓扑 信息
        Iterable<RedisURI> topologyRefreshSource = getTopologyRefreshSource();

        try {
            // SQ: 获取集群拓扑
            return doLoadPartitions(topologyRefreshSource);
        } catch (RedisException e) {

            // Attempt recovery using initial seed nodes
            if (useDynamicRefreshSources() && topologyRefreshSource != initialUris) {

                try {
                    // SQ: 如果动态获取 seed 方式失败则用用户初始传入的静态结点列表再试一次
                    return doLoadPartitions(initialUris);
                } catch (RedisConnectionException e2) {

                    RedisException exception = new RedisException(getTopologyRefreshErrorMessage(initialUris), e2);
                    exception.addSuppressed(e);

                    throw exception;
                }
            }

            if (e.getClass().equals(RedisException.class)) {
                throw e;
            }

            throw new RedisException(getTopologyRefreshErrorMessage(topologyRefreshSource), e);
        }
    }

    private Partitions doLoadPartitions(Iterable<RedisURI> topologyRefreshSource) {

        // SQ: 向所有结点发 cluster nodes 和 info clients 命令，获取集群拓扑信息
        Map<RedisURI, Partitions> partitions = refresh.loadViews(topologyRefreshSource,
                getClusterClientOptions().getSocketOptions().getConnectTimeout(), useDynamicRefreshSources());

        if (partitions.isEmpty()) {
            throw new RedisException(getTopologyRefreshErrorMessage(topologyRefreshSource));
        }

        // SQ: 从来自各结点的拓扑视图中选出一份，
        //  原则：第一次选 connected 结点最多的；之后选与前一次 refresh 结果最接近的
        Partitions loadedPartitions = determinePartitions(this.partitions, partitions);

        // SQ: 上一步选出来的拓扑视图来自哪个结点
        RedisURI viewedBy = refresh.getViewedBy(partitions, loadedPartitions);

        // SQ: 对所有结点的连接参数都取该结点的配置
        //  比如有 A、B、C 三个结点，最终拓扑视图选择 B 结点的，则对 A、C 结点的连接参数都设置为与对 B 结点的连接参数相同
        for (RedisClusterNode partition : loadedPartitions) {
            if (viewedBy != null) {
                RedisURI uri = partition.getUri();
                RedisClusterURIUtil.applyUriConnectionSettings(viewedBy, uri);
            }
        }

        // SQ: 如果设置了定时刷新视图 ClusterTopologyRefreshOptions.periodicRefreshEnabled=true，则启动定时任务
        activateTopologyRefreshIfNeeded();

        return loadedPartitions;
    }

    private static String getTopologyRefreshErrorMessage(Iterable<RedisURI> topologyRefreshSource) {
        return "Cannot retrieve initial cluster partitions from initial URIs " + topologyRefreshSource;
    }

    /**
     * Determines a {@link Partitions topology view} based on the current and the obtain topology views.
     *
     * @param current the current topology view. May be {@code null} if {@link RedisClusterClient} has no topology view yet.
     * @param topologyViews the obtain topology views.
     * @return the {@link Partitions topology view} to use.
     */
    protected Partitions determinePartitions(Partitions current, Map<RedisURI, Partitions> topologyViews) {

        if (current == null) {
            return PartitionsConsensus.HEALTHY_MAJORITY.getPartitions(null, topologyViews);
        }

        return PartitionsConsensus.KNOWN_MAJORITY.getPartitions(current, topologyViews);
    }

    private void activateTopologyRefreshIfNeeded() {

        if (getOptions() instanceof ClusterClientOptions) {
            ClusterClientOptions options = (ClusterClientOptions) getOptions();
            ClusterTopologyRefreshOptions topologyRefreshOptions = options.getTopologyRefreshOptions();

            if (!topologyRefreshOptions.isPeriodicRefreshEnabled() || clusterTopologyRefreshActivated.get()) {
                return;
            }

            if (clusterTopologyRefreshActivated.compareAndSet(false, true)) {
                ScheduledFuture<?> scheduledFuture = genericWorkerPool.scheduleAtFixedRate(clusterTopologyRefreshScheduler,
                        options.getRefreshPeriod().toNanos(), options.getRefreshPeriod().toNanos(), TimeUnit.NANOSECONDS);
                clusterTopologyRefreshFuture.set(scheduledFuture);
            }
        }
    }

    /**
     * Sets the new cluster topology. The partitions are not applied to existing connections.
     *
     * @param partitions partitions object.
     */
    public void setPartitions(Partitions partitions) {
        this.partitions = partitions;
    }

    /**
     * Returns the {@link ClientResources} which are used with that client.
     *
     * @return the {@link ClientResources} for this client.
     */
    public ClientResources getResources() {
        return clientResources;
    }

    /**
     * Shutdown this client and close all open connections asynchronously. The client should be discarded after calling
     * shutdown.
     *
     * @param quietPeriod the quiet period as described in the documentation.
     * @param timeout the maximum amount of time to wait until the executor is shutdown regardless if a task was submitted
     *        during the quiet period.
     * @param timeUnit the unit of {@code quietPeriod} and {@code timeout}.
     * @since 4.4
     */
    @Override
    public CompletableFuture<Void> shutdownAsync(long quietPeriod, long timeout, TimeUnit timeUnit) {

        if (clusterTopologyRefreshActivated.compareAndSet(true, false)) {

            ScheduledFuture<?> scheduledFuture = clusterTopologyRefreshFuture.get();

            try {
                scheduledFuture.cancel(false);
                clusterTopologyRefreshFuture.set(null);
            } catch (Exception e) {
                logger.debug("Could not cancel Cluster topology refresh", e);
            }
        }

        return super.shutdownAsync(quietPeriod, timeout, timeUnit);
    }

    /**
     * Set the {@link ClusterClientOptions} for the client.
     *
     * @param clientOptions client options for the client and connections that are created after setting the options.
     */
    public void setOptions(ClusterClientOptions clientOptions) {
        super.setOptions(clientOptions);
    }

    // -------------------------------------------------------------------------
    // Implementation hooks and helper methods
    // -------------------------------------------------------------------------

    /**
     * Returns the first {@link RedisURI} configured with this {@link RedisClusterClient} instance.
     *
     * @return the first {@link RedisURI}.
     */
    protected RedisURI getFirstUri() {
        assertNotEmpty(initialUris);
        Iterator<RedisURI> iterator = initialUris.iterator();
        return iterator.next();
    }

    /**
     * Returns a {@link Supplier} for {@link SocketAddress connection points}.
     *
     * @param sortFunction Sort function to enforce a specific order. The sort function must not change the order or the input
     *        parameter but create a new collection with the desired order, must not be {@code null}.
     * @return {@link Supplier} for {@link SocketAddress connection points}.
     */
    protected Mono<SocketAddress> getSocketAddressSupplier(Function<Partitions, Collection<RedisClusterNode>> sortFunction) {

        LettuceAssert.notNull(sortFunction, "Sort function must not be null");

        final RoundRobinSocketAddressSupplier socketAddressSupplier = new RoundRobinSocketAddressSupplier(partitions,
                sortFunction, clientResources);

        return Mono.defer(() -> {

            if (partitions.isEmpty()) {
                return Mono.fromCallable(() -> {
                    SocketAddress socketAddress = clientResources.socketAddressResolver().resolve(getFirstUri());
                    logger.debug("Resolved SocketAddress {} using {}", socketAddress, getFirstUri());
                    return socketAddress;
                });
            }

            return Mono.fromCallable(socketAddressSupplier::get);
        });
    }

    /**
     * Returns an {@link Iterable} of the initial {@link RedisURI URIs}.
     *
     * @return the initial {@link RedisURI URIs}.
     */
    protected Iterable<RedisURI> getInitialUris() {
        return initialUris;
    }

    /**
     * Apply a {@link Consumer} of {@link StatefulRedisClusterConnectionImpl} to all active connections.
     *
     * @param function the {@link Consumer}.
     */
    protected void forEachClusterConnection(Consumer<StatefulRedisClusterConnectionImpl<?, ?>> function) {
        forEachCloseable(input -> input instanceof StatefulRedisClusterConnectionImpl, function);
    }

    /**
     * Apply a {@link Consumer} of {@link StatefulRedisClusterPubSubConnectionImpl} to all active connections.
     *
     * @param function the {@link Consumer}.
     */
    protected void forEachClusterPubSubConnection(Consumer<StatefulRedisClusterPubSubConnectionImpl<?, ?>> function) {
        forEachCloseable(input -> input instanceof StatefulRedisClusterPubSubConnectionImpl, function);
    }

    /**
     * Apply a {@link Consumer} of {@link Closeable} to all active connections.
     *
     * @param <T>
     * @param function the {@link Consumer}.
     */
    @SuppressWarnings("unchecked")
    protected <T extends Closeable> void forEachCloseable(Predicate<? super Closeable> selector, Consumer<T> function) {
        for (Closeable c : closeableResources) {
            if (selector.test(c)) {
                function.accept((T) c);
            }
        }
    }

    /**
     * Returns the seed {@link RedisURI} for the topology refreshing. This method is called before each topology refresh to
     * provide an {@link Iterable} of {@link RedisURI} that is used to perform the next topology refresh.
     * <p>
     * Subclasses of {@link RedisClusterClient} may override that method.
     *
     * @return {@link Iterable} of {@link RedisURI} for the next topology refresh.
     */
    // SQ: 挑选出一组结点用于获取获取 集群拓扑 信息，
    //  最终会从各结点返回的拓扑视图中挑选出一个作为最终视图（挑选策略在 determinePartitions 方法中）
    protected Iterable<RedisURI> getTopologyRefreshSource() {

        boolean initialSeedNodes = !useDynamicRefreshSources();

        // SQ: 初始刷新(partitions == null) 时，直接使用用户传入的 seed 列表；
        //  后续再刷新时：
        //    如果 useDynamicRefreshSources==false ，则仍使用用户传入的 seed 列表，
        //    如果 useDynamicRefreshSources==true，则使用前一次加载到的拓扑视图中的全部结点（按 URI 排序）
        Iterable<RedisURI> seed;
        if (initialSeedNodes || partitions == null || partitions.isEmpty()) {
            seed = this.initialUris;
        } else {
            List<RedisURI> uris = new ArrayList<>();
            for (RedisClusterNode partition : TopologyComparators.sortByUri(partitions)) {
                uris.add(partition.getUri());
            }
            seed = uris;
        }
        return seed;
    }

    /**
     * Returns {@code true} if {@link ClusterTopologyRefreshOptions#useDynamicRefreshSources() dynamic refresh sources} are
     * enabled.
     * <p>
     * Subclasses of {@link RedisClusterClient} may override that method.
     *
     * @return {@code true} if dynamic refresh sources are used.
     * @see ClusterTopologyRefreshOptions#useDynamicRefreshSources()
     */
    protected boolean useDynamicRefreshSources() {

        if (getClusterClientOptions() != null) {
            ClusterTopologyRefreshOptions topologyRefreshOptions = getClusterClientOptions().getTopologyRefreshOptions();

            return topologyRefreshOptions.useDynamicRefreshSources();
        }
        return true;
    }

    /**
     * Returns a {@link String} {@link RedisCodec codec}.
     *
     * @return a {@link String} {@link RedisCodec codec}.
     * @see StringCodec#UTF8
     */
    protected RedisCodec<String, String> newStringStringCodec() {
        return StringCodec.UTF8;
    }

    ClusterClientOptions getClusterClientOptions() {
        if (getOptions() instanceof ClusterClientOptions) {
            return (ClusterClientOptions) getOptions();
        }
        return null;
    }

    boolean expireStaleConnections() {
        return getClusterClientOptions() == null || getClusterClientOptions().isCloseStaleConnections();
    }

    protected static <T> CompletableFuture<T> transformAsyncConnectionException(CompletionStage<T> future,
            Iterable<RedisURI> target) {

        return ConnectionFuture.from(null, future.toCompletableFuture()).thenCompose((v, e) -> {

            if (e != null) {
                return Futures.failed(RedisConnectionException.create(target.toString(), e));
            }

            return CompletableFuture.completedFuture(v);
        }).toCompletableFuture();
    }

    private static <K, V> void assertNotNull(RedisCodec<K, V> codec) {
        LettuceAssert.notNull(codec, "RedisCodec must not be null");
    }

    private static void assertNotEmpty(Iterable<RedisURI> redisURIs) {
        LettuceAssert.notNull(redisURIs, "RedisURIs must not be null");
        LettuceAssert.isTrue(redisURIs.iterator().hasNext(), "RedisURIs must not be empty");
    }

    private static RedisURI assertNotNull(RedisURI redisURI) {
        LettuceAssert.notNull(redisURI, "RedisURI must not be null");
        return redisURI;
    }

    private static void assertNotNull(ClientResources clientResources) {
        LettuceAssert.notNull(clientResources, "ClientResources must not be null");
    }

    private class NodeConnectionFactoryImpl implements NodeConnectionFactory {

        @Override
        public <K, V> StatefulRedisConnection<K, V> connectToNode(RedisCodec<K, V> codec, SocketAddress socketAddress) {
            return RedisClusterClient.this.connectToNode(codec, socketAddress.toString(), null, Mono.just(socketAddress));
        }

        @Override
        public <K, V> ConnectionFuture<StatefulRedisConnection<K, V>> connectToNodeAsync(RedisCodec<K, V> codec,
                SocketAddress socketAddress) {
            return RedisClusterClient.this.connectToNodeAsync(codec, socketAddress.toString(), null, Mono.just(socketAddress));
        }

    }

}
