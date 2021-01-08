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

import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.models.partitions.ClusterPartitionParser;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;

/**
 * @author Mark Paluch
 * @author Xujs
 */
// SQ: 某一个 seed 结点所看到的集群拓扑状态，即对这个结点执行 cluster nodes 和 info clients 两个命令的结果
class NodeTopologyView {

    private static final Pattern NUMBER = Pattern.compile("(\\d+)");

    // SQ: 两个命令都执行成功了，就认为该 seed 结点是 available
    private final boolean available;

    private final RedisURI redisURI;

    private Partitions partitions;

    private final int connectedClients;

    // SQ: 这个变量的值是执行 cluster nodes 命令所花费的时间，作为从 lettuce 结点到该 seed 结点的延时
    private final long latency;

    private final String clusterNodes;

    private final String clientList;

    private NodeTopologyView(RedisURI redisURI) {

        this.available = false;
        this.redisURI = redisURI;
        this.partitions = new Partitions();
        this.connectedClients = 0;
        this.clusterNodes = null;
        this.clientList = null;
        this.latency = 0;
    }

    NodeTopologyView(RedisURI redisURI, String clusterNodes, String clientList, long latency) {

        this.available = true;
        this.redisURI = redisURI;

        // SQ: 解析 cluster nodes 命令的结果
        this.partitions = ClusterPartitionParser.parse(clusterNodes);

        // SQ: 解析 info clients 命令的结果
        this.connectedClients = clientList != null ? getClients(clientList) : 0;

        this.clusterNodes = clusterNodes;
        this.clientList = clientList;
        this.latency = latency;
    }

    static NodeTopologyView from(RedisURI redisURI, Requests clusterNodesRequests, Requests clientListRequests)
            throws ExecutionException, InterruptedException {

        TimedAsyncCommand<String, String, String> nodes = clusterNodesRequests.getRequest(redisURI);
        TimedAsyncCommand<String, String, String> clients = clientListRequests.getRequest(redisURI);

        if (resultAvailable(nodes) && resultAvailable(clients)) {
            return new NodeTopologyView(redisURI, nodes.get(), optionallyGet(clients), nodes.duration());
        }
        return new NodeTopologyView(redisURI);
    }

    private static <T> T optionallyGet(TimedAsyncCommand<?, ?, T> command) throws ExecutionException, InterruptedException {

        if (command.isCompletedExceptionally()) {
            return null;
        }
        return command.get();
    }

    private static boolean resultAvailable(RedisFuture<?> redisFuture) {

        if (redisFuture != null && redisFuture.isDone() && !redisFuture.isCancelled()) {
            return true;
        }

        return false;
    }

    private int getClients(String rawClientsOutput) {
        String[] rows = rawClientsOutput.trim().split("\\n");
        for (String row : rows) {

            Matcher matcher = NUMBER.matcher(row);
            if (matcher.find()) {
                return Integer.parseInt(matcher.group(1));
            }
        }
        return 0;
    }

    long getLatency() {
        return latency;
    }

    boolean isAvailable() {
        return available;
    }

    Partitions getPartitions() {
        return partitions;
    }

    int getConnectedClients() {
        return connectedClients;
    }

    String getNodeId() {
        return getOwnPartition().getNodeId();
    }

    RedisURI getRedisURI() {

        if (partitions.isEmpty()) {
            return redisURI;
        }

        return getOwnPartition().getUri();
    }

    RedisClusterNode getOwnPartition() {
        for (RedisClusterNode partition : partitions) {
            if (partition.is(RedisClusterNode.NodeFlag.MYSELF)) {
                return partition;
            }
        }

        throw new IllegalStateException("Cannot determine own partition");
    }

    String getClientList() {
        return clientList;
    }

    String getClusterNodes() {
        return clusterNodes;
    }

    void setPartitions(Partitions partitions) {
        this.partitions = partitions;
    }

}
