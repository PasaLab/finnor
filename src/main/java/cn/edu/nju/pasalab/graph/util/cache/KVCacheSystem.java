package cn.edu.nju.pasalab.graph.util.cache;

/**
 * The cache system abstract.
 * Created by wangzhaokang on 3/15/17.
 */
public interface KVCacheSystem<Key,Value> {

    /** put into the cache */
    public abstract void put(Key k, Value v);
    /** Get from cache. `null` if the key is not in the cache. */
    public abstract Value get(Key k);

    /** Get cache capacity */
    public abstract long getCapacity();
}
