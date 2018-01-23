package cn.edu.nju.pasalab.graph.util.db.redis;

import cn.edu.nju.pasalab.graph.util.PropertyFileLoader;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.File;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Redis client pools for a group of servers in a cluster.
 * Created by wangzhaokang on 3/15/17.
 */
public class RedisClusterClientPools {
    private static final Logger logger = Logger.getLogger("RedisClientPool");

    private static final String CONFIG_FILE_PATH = "redis.cluster.conf";
    private static boolean isValid = false;

    int serverNum;
    int poolSize;
    String servers[];
    private JedisPool[] pools;

    /**
     * Setup: 1. Load server list from configuration files
     * 2. Load pool size for each redis server
     */
    private RedisClusterClientPools () {
        if (new File(CONFIG_FILE_PATH).exists()) {
            Properties config = PropertyFileLoader.load(CONFIG_FILE_PATH);
            servers = config.getProperty("servers", "localhost").split(",");
            serverNum = servers.length;
            poolSize = Integer.parseInt(config.getProperty("pool.size", "3"));
            System.err.println("[Redis Pool]" + serverNum + " servers with " + poolSize + " pool size.");
            buildPool();
        }
    }

    private void buildPool() {
        logger.info("Start building redis client pool...");
        pools = new JedisPool[serverNum];
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(poolSize);
        for (int i = 0; i < serverNum; i++) {
            pools[i] = new JedisPool(poolConfig, servers[i], 6379, 200000);
        }
        logger.info("redis client pool get!");
    }

    private static RedisClusterClientPools instance = new RedisClusterClientPools();

    /**
     * Get a group of jedis clients for all servers.
     * This function call is thread safe.
     * @return
     */
    public static Jedis[] getClusterClients() {
        Jedis clients[] = new Jedis[instance.serverNum];
        for (int i = 0; i < instance.serverNum; i++) {
            clients[i] = instance.pools[i].getResource();
        }
        return clients;
    }

    public static void returnResources(Jedis clients[]) {
        for (Jedis client : clients) {
            client.close();
        }
    }
}
