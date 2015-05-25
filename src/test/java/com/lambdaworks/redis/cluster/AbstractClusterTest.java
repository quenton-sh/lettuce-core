package com.lambdaworks.redis.cluster;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;

import com.google.common.collect.ImmutableList;
import com.lambdaworks.redis.RedisClusterAsyncConnection;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.TestSettings;

/**
 * @author <a href="mailto:mpaluch@paluch.biz">Mark Paluch</a>
 */
public class AbstractClusterTest {

    public static final String host = TestSettings.hostAddr();
    public static final int port1 = 7379;
    public static final int port2 = 7380;
    public static final int port3 = 7381;
    public static final int port4 = 7382;

    protected static RedisClusterClient clusterClient;

    protected Logger log = Logger.getLogger(getClass());

    protected RedisClusterAsyncConnection<String, String> redis1;

    protected String key = "key";
    protected String value = "value";

    @Rule
    public ClusterRule clusterRule = new ClusterRule(clusterClient, port1, port2, port3, port4);

    @BeforeClass
    public static void setupClusterClient() throws Exception {
        clusterClient = new RedisClusterClient(ImmutableList.of(RedisURI.Builder.redis(host, port1).build()));
    }

    @AfterClass
    public static void shutdownClusterClient() {
        clusterClient.shutdown(0, 0, TimeUnit.MILLISECONDS);
    }

    public static int[] createSlots(int from, int to) {
        int[] result = new int[to - from];
        int counter = 0;
        for (int i = from; i < to; i++) {
            result[counter++] = i;

        }
        return result;
    }

}
