package cn.edu.nju.pasalab.graph.util.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.*;

/**
 * A process level cache with Caffeine backend
 * Created by wangzhaokang on 3/15/17.
 */
public class CaffeineProcessLevelCache<Key, Value> implements KVCacheSystem<Key, Value> {

    private final static String CONFIGURATION_FILE_NAME = "caffeine.config";

    private static Logger logger = Logger.getLogger("CaffeineCache");
    private long capacity = 10000;
    private Cache<Key, Value> cache;

    private static CaffeineProcessLevelCache ourInstance = new CaffeineProcessLevelCache();

    public static CaffeineProcessLevelCache getInstance() {
        return ourInstance;
    }

    private void loadConfiguration() {
        File configureFile = new File(CONFIGURATION_FILE_NAME);
        if (configureFile.exists()) {
            Properties config = new Properties();
            try {
                FileInputStream in = new FileInputStream(configureFile);
                config.load(in);
                in.close();
                this.capacity = Long.parseLong(config.getProperty("capacity"));
            } catch (IOException e) {
                logger.warning("Fail to load configuration file: " + e.toString());
            }
        }
    }

    private CaffeineProcessLevelCache() {
        loadConfiguration();
        cache = Caffeine.newBuilder().maximumWeight(capacity)
                .weigher(new MyWeigher<Key,Value>())
                .build();
    }

    @Override
    public void put(Key k, Value v) {
        cache.put(k, v);
    }

    @Override
    public Value get(Key k) {
        return cache.getIfPresent(k);
    }

    /**
     * Get cache capacity
     */
    @Override
    public long getCapacity() {
        return ourInstance.capacity;
    }

    class MyWeigher<Key,Value> implements Weigher<Key,Value> {

        @Override
        public int weigh(@Nonnull Key key, @Nonnull Value value) {
            if (value instanceof long[]) {
                long arr[] = (long[])value;
                return arr.length * Long.BYTES;
            }
            return 1;
        }
    }
}
