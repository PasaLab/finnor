package cn.edu.nju.pasalab.graph.util.db.batchaccess;

import java.util.Iterator;
import java.util.List;

/**
 * Access database in the batch mode.
 * Created by wangzhaokang on 3/15/17.
 */
public interface BatchKeyValueDBConnector<Key,Value> {
    public void setup();
    public List<Value> query(Iterator<Key> keys);
    public void set(Iterator<Key> keys, Iterator<Value> values);
    public void cleanup();
}
