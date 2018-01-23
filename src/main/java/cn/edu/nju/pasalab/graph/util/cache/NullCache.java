package cn.edu.nju.pasalab.graph.util.cache;

/**
 * Created by wangzhaokang on 7/4/17.
 */
public class NullCache<Key,Value> implements KVCacheSystem<Key,Value> {
    /**
     * put into the cache
     *
     * @param k
     * @param v
     */
    @Override
    public void put(Key k, Value v) {

    }

    /**
     * Get from cache. `null` if the key is not in the cache.
     *
     * @param k
     */
    @Override
    public Value get(Key k) {
        return null;
    }

    /**
     * Get cache capacity
     */
    @Override
    public long getCapacity() {
        return 0;
    }
}
