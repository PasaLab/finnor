package cn.edu.nju.pasalab.analysis.mutualfriend;

import cn.edu.nju.pasalab.graph.util.db.redis.RedisBatchConnector;
import com.github.benmanes.caffeine.cache.*;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by wangzhaokang on 5/24/17.
 */
public class RedisDegreeLookupCache {
    public LoadingCache<Long, Integer> cache = null;

    public RedisDegreeLookupCache(long size) {
        cache = Caffeine.newBuilder().maximumSize(size)
                .build(new CacheLoader<Long, Integer>() {
                    @Override
                    public Integer load(@Nonnull Long key) throws Exception {
                        ArrayList<Long> keyIterator = new ArrayList<Long>();
                        keyIterator.add(key);
                        Map<Long,Integer> result = loadAll(keyIterator);
                        return result.get(key);
                    }

                    @Override
                    public Map<Long, Integer> loadAll(@Nonnull Iterable<? extends Long> keys) {
                        HashMap<Long, Integer> results = new HashMap<>();
                        RedisBatchConnector connector = new RedisBatchConnector();
                        connector.setup(2);
                        int resultArray[] = connector.queryLongIntPairs(keys);
                        Iterator<? extends Long> keyIterator = keys.iterator();
                        for (int i = 0; i < resultArray.length; i++) {
                            Long key = keyIterator.next();
                            results.put(key, resultArray[i]);
                        }
                        connector.cleanup();
                        return results;
                    }
                });
    }
}
