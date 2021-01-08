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
package io.lettuce.core.cluster.topology;

import java.io.IOException;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.topology.TopologyComparators.SortAction;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.ClientResources;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Utility to refresh the cluster topology view based on {@link Partitions}.
 *
 * @author Mark Paluch
 */
public class ClusterTopologyRefresh {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ClusterTopologyRefresh.class);

    private final NodeConnectionFactory nodeConnectionFactory;

    private final ClientResources clientResources;

    public ClusterTopologyRefresh(NodeConnectionFactory nodeConnectionFactory, ClientResources clientResources) {
        this.nodeConnectionFactory = nodeConnectionFactory;
        this.clientResources = clientResources;
    }

    /**
     * Load partition views from a collection of {@link RedisURI}s and return the view per {@link RedisURI}. Partitions contain
     * an ordered list of {@link RedisClusterNode}s. The sort key is latency. Nodes with lower latency come first.
     *
     * @param seed collection of {@link RedisURI}s.
     * @param connectTimeout connect timeout.
     * @param discovery {@code true} to discover additional nodes.
     * @return mapping between {@link RedisURI} and {@link Partitions}.
     */
    // SQ: 从指定 seed 结点中获取集群拓扑信息，
    //  如果 ClusterTopologyRefreshOptions.useDynamicRefreshSources=true 则会从所有结点中获取集群拓扑
    public Map<RedisURI, Partitions> loadViews(Iterable<RedisURI> seed, Duration connectTimeout, boolean discovery) {

        if (!isEventLoopActive()) {
            return Collections.emptyMap();
        }

        long commandTimeoutNs = getCommandTimeoutNs(seed);

        Connections seedConnections = null;
        Connections discoveredConnections = null;
        try {
            // SQ: 调用 RedisClusterClient.connectToNodeAsync 方法向所有 seed 结点建连，在 finally 中会把这些连接 close 掉
            seedConnections = getConnections(seed)
                .get(commandTimeoutNs + connectTimeout.toNanos(), TimeUnit.NANOSECONDS);

            if (!isEventLoopActive()) {
                return Collections.emptyMap();
            }

            // SQ: 通过各 connection 发送命令
            Requests requestedTopology = seedConnections.requestTopology();
            Requests requestedClients = seedConnections.requestClients();

            // SQ: 解析命令结果
            NodeTopologyViews nodeSpecificViews = getNodeSpecificViews(requestedTopology, requestedClients, commandTimeoutNs);

            if (discovery && isEventLoopActive()) {

                // SQ: 所有通过各 seed 可以看到的 node 集合
                Set<RedisURI> allKnownUris = nodeSpecificViews.getClusterNodes();
                // SQ: 减去 seed 后，需要额外查看的 node 集合
                Set<RedisURI> discoveredNodes = difference(allKnownUris, toSet(seed));

                if (!discoveredNodes.isEmpty()) {
                    // SQ: 向额外 node 建连
                    discoveredConnections = getConnections(discoveredNodes).optionalGet(commandTimeoutNs,
                            TimeUnit.NANOSECONDS);
                    Connections connections = seedConnections.mergeWith(discoveredConnections);

                    if (isEventLoopActive()) {
                        // SQ: 向所有 node (包含 seed 和 额外 node) 做查询
                        requestedTopology = requestedTopology.mergeWith(connections.requestTopology());
                        requestedClients = requestedClients.mergeWith(connections.requestClients());

                        nodeSpecificViews = getNodeSpecificViews(requestedTopology, requestedClients, commandTimeoutNs);
                    }

                    if (nodeSpecificViews.isEmpty()) {
                        tryFail(requestedTopology, seed);
                    }

                    return nodeSpecificViews.toMap();
                }
            }

            if (nodeSpecificViews.isEmpty()) {
                tryFail(requestedTopology, seed);
            }

            return nodeSpecificViews.toMap();
        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
            throw new RedisCommandInterruptedException(e);
        } finally {
            // SQ: 把连接 close 掉
            if (seedConnections != null) {
                try {
                    seedConnections.close();
                } catch (Exception e) {
                    logger.debug("Cannot close ClusterTopologyRefresh connections", e);
                }
            }

            if (discoveredConnections != null) {
                try {
                    discoveredConnections.close();
                } catch (Exception e) {
                    logger.debug("Cannot close ClusterTopologyRefresh connections", e);
                }
            }
        }
    }

    private void tryFail(Requests requestedTopology, Iterable<RedisURI> seed) {

        RedisException exception = null;

        for (RedisURI node : requestedTopology.nodes()) {

            TimedAsyncCommand<String, String, String> request = requestedTopology.getRequest(node);
            if (request != null && request.isCompletedExceptionally()) {

                Throwable cause = RefreshFutures.getException(request);
                if (exception == null) {
                    exception = new RedisException("Cannot retrieve initial cluster partitions from initial URIs " + seed,
                            cause);
                } else if (cause != null) {
                    exception.addSuppressed(cause);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    private Set<RedisURI> toSet(Iterable<RedisURI> seed) {
        return StreamSupport.stream(seed.spliterator(), false).collect(Collectors.toCollection(HashSet::new));
    }

    NodeTopologyViews getNodeSpecificViews(Requests requestedTopology, Requests requestedClients, long commandTimeoutNs)
            throws InterruptedException {

        // SQ: 所有 seed 结点看到的所有结点综合，
        //  比如集群有 5 个节点，选出 3 个 seed 节点，每个 seed 结点都会看到 5 个 结点信息，所以 allNodes 数组中总共有 3 * 5 = 15 个元素
        List<RedisClusterNodeSnapshot> allNodes = new ArrayList<>();

        Map<String, Long> latencies = new HashMap<>();
        Map<String, Integer> clientCountByNodeId = new HashMap<>();

        // SQ: 等待发往各 seed 结点的 cluster nodes 命令全部执行完毕
        long waitTime = requestedTopology.await(commandTimeoutNs, TimeUnit.NANOSECONDS);
        // SQ: 等待发往各 seed 结点的 info clients 命令全部执行完毕
        requestedClients.await(commandTimeoutNs - waitTime, TimeUnit.NANOSECONDS);

        Set<RedisURI> nodes = requestedTopology.nodes();

        // SQ: 遍历各 seed 结点，解析前面两个命令的结果
        List<NodeTopologyView> views = new ArrayList<>();
        for (RedisURI nodeUri : nodes) {

            try {
                // SQ: 解析命令结果
                NodeTopologyView nodeTopologyView = NodeTopologyView.from(nodeUri, requestedTopology, requestedClients);

                if (!nodeTopologyView.isAvailable()) {
                    continue;
                }

                RedisClusterNode node = nodeTopologyView.getOwnPartition();
                if (node.getUri() == null) {
                    node.setUri(nodeUri);
                } else {
                    node.addAlias(nodeUri);
                }

                List<RedisClusterNodeSnapshot> nodeWithStats = new ArrayList<>(nodeTopologyView.getPartitions().size());

                // SQ: 将每个 RedisClusterNode 包装为 RedisClusterNodeSnapshot
                for (RedisClusterNode partition : nodeTopologyView.getPartitions()) {

                    if (validNode(partition)) {
                        RedisClusterNodeSnapshot redisClusterNodeSnapshot = new RedisClusterNodeSnapshot(partition);
                        nodeWithStats.add(redisClusterNodeSnapshot);

                        if (partition.is(RedisClusterNode.NodeFlag.MYSELF)) {

                            // record latency for later partition ordering
                            // SQ: 收集 lettuce 结点到该 seed 结点的延时 和该 seed 结点上的 client 连接数
                            //  其实 NodeTopologyView 中就有延时信息，放到这里才收集是因为需要 partition 中的 nodeId
                            latencies.put(partition.getNodeId(), nodeTopologyView.getLatency());
                            clientCountByNodeId.put(partition.getNodeId(), nodeTopologyView.getConnectedClients());
                        }
                    }
                }

                allNodes.addAll(nodeWithStats);

                Partitions partitions = new Partitions();
                partitions.addAll(nodeWithStats);

                // SQ: 将该 seed 结点的 Partitions 中的各 RedisClusterNode 替换为 RedisClusterNodeSnapshot
                nodeTopologyView.setPartitions(partitions);

                views.add(nodeTopologyView);
            } catch (ExecutionException e) {
                logger.warn(String.format("Cannot retrieve partition view from %s, error: %s", nodeUri, e));
            }
        }

        // SQ: allNodes 中只有各个 self 结点有 latency 信息，此处用这些信息给所有 nodeId 相同的 node 赋值，
        //  保证各 RedisClusterNodeSnapshot 对象中都有 latency
        for (RedisClusterNodeSnapshot node : allNodes) {
            node.setConnectedClients(clientCountByNodeId.get(node.getNodeId()));
            node.setLatencyNs(latencies.get(node.getNodeId()));
        }

        // SQ: 给各个 NodeTopologyView 中的 nodes 重新排序，默认按 latency (RedisClusterNodeSnapshot.latencyNs 字段) 排序
        SortAction sortAction = SortAction.getSortAction();
        for (NodeTopologyView view : views) {

            sortAction.sort(view.getPartitions());
            view.getPartitions().updateCache();
        }

        return new NodeTopologyViews(views);
    }

    private static boolean validNode(RedisClusterNode redisClusterNode) {

        if (redisClusterNode.is(RedisClusterNode.NodeFlag.NOADDR)) {
            return false;
        }

        if (redisClusterNode.getUri() == null || redisClusterNode.getUri().getPort() == 0
                || LettuceStrings.isEmpty(redisClusterNode.getUri().getHost())) {
            return false;
        }

        return true;
    }

    /*
     * Open connections where an address can be resolved.
     */
    private AsyncConnections getConnections(Iterable<RedisURI> redisURIs) {

        AsyncConnections connections = new AsyncConnections();

        for (RedisURI redisURI : redisURIs) {
            if (redisURI.getHost() == null || connections.connectedNodes().contains(redisURI) || !isEventLoopActive()) {
                continue;
            }

            try {
                SocketAddress socketAddress = clientResources.socketAddressResolver().resolve(redisURI);

                // SQ: 通过 NodeConnectionFactory 建连接，最终会调用 RedisCluterClient 的 connectToNodeAsync 方法
                ConnectionFuture<StatefulRedisConnection<String, String>> connectionFuture = nodeConnectionFactory
                        .connectToNodeAsync(StringCodec.UTF8, socketAddress);

                CompletableFuture<StatefulRedisConnection<String, String>> sync = new CompletableFuture<>();

                connectionFuture.whenComplete((connection, throwable) -> {

                    if (throwable != null) {

                        Throwable throwableToUse = throwable;
                        if (throwable instanceof CompletionException) {
                            throwableToUse = throwableToUse.getCause();
                        }

                        String message = String.format("Unable to connect to [%s]: %s", socketAddress,
                                throwableToUse.getMessage() != null ? throwableToUse.getMessage() : throwableToUse.toString());
                        if (throwableToUse instanceof RedisConnectionException || throwableToUse instanceof IOException) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(message, throwableToUse);
                            } else {
                                logger.warn(message);
                            }
                        } else {
                            logger.warn(message, throwableToUse);
                        }

                        sync.completeExceptionally(new RedisConnectionException(message, throwableToUse));
                    } else {
                        // SQ: 同步：对外返回 sync 对象
                        // SQ: 异步：建连成功后把结果 (StatefulRedisConnection 对象) 放到 sync 对象中
                        connection.async().clientSetname("lettuce#ClusterTopologyRefresh");
                        sync.complete(connection);
                    }
                });

                connections.addConnection(redisURI, sync);
            } catch (RuntimeException e) {
                logger.warn(String.format("Unable to connect to [%s]", redisURI), e);
            }
        }

        return connections;
    }

    private boolean isEventLoopActive() {

        EventExecutorGroup eventExecutors = clientResources.eventExecutorGroup();

        return !eventExecutors.isShuttingDown();
    }

    /**
     * Resolve a {@link RedisURI} from a map of cluster views by {@link Partitions} as key.
     *
     * @param map the map.
     * @param partitions the key.
     * @return a {@link RedisURI} or null.
     */
    public RedisURI getViewedBy(Map<RedisURI, Partitions> map, Partitions partitions) {

        for (Map.Entry<RedisURI, Partitions> entry : map.entrySet()) {
            if (entry.getValue() == partitions) {
                return entry.getKey();
            }
        }

        return null;
    }

    private static Set<RedisURI> difference(Set<RedisURI> allKnown, Set<RedisURI> seed) {

        Set<RedisURI> result = new TreeSet<>(TopologyComparators.RedisURIComparator.INSTANCE);

        for (RedisURI e : allKnown) {
            if (!seed.contains(e)) {
                result.add(e);
            }
        }

        return result;
    }

    private long getCommandTimeoutNs(Iterable<RedisURI> redisURIs) {

        RedisURI redisURI = redisURIs.iterator().next();
        return redisURI.getTimeout().toNanos();
    }

}
