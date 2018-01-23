package cn.edu.nju.pasalab.graph.util.db.batchaccess;

import cn.edu.nju.pasalab.graph.util.cache.KVCacheSystem;
import cn.edu.nju.pasalab.graph.util.db.batchaccess.BatchKeyValueDBConnector;

import com.carrotsearch.hppc.LongObjectHashMap;
import wzk.akkalogger.client.AkkaLoggerClient;
import wzk.akkalogger.client.ProcessLevelClient;

import java.util.*;

/**
 * The class to access adjacency list in the batch mode to speedup performance.
 * Created by wangzhaokang on 3/15/17.
 */
public class BatchAdjListAccess {

    /** Store results for current batch */
    //HashMap<Long, long[]> results;
    LongObjectHashMap<long[]> results;
    /** Store vertex ids that need to query database */
    HashSet<Long> requestsVid;

    BatchKeyValueDBConnector<Long, long[]> db;
    KVCacheSystem<Long, long[]> cache;

    AkkaLoggerClient logClient = ProcessLevelClient.client();

    public class Statics {
        public long requestCount = 0;
        public long queryCount = 0;
        public long actualQueryCount = 0;
        public long hitCount = 0;
        public long queryTimeInMilliSecond = 0;
        public long getResourceTimeInMilliSecond = 0;
        public long initedTime = System.nanoTime();
        public long finishTime = 0;

        public Map<String, Long> getStaticsMap() {
            HashMap<String, Long> map = new HashMap<>();
            map.put("BatchAccess-ActualQueryCount", actualQueryCount);
            map.put("BatchAccess-DBQueryTime(ms)", queryTimeInMilliSecond);
            map.put("BatchAccess-GetDBClientsTime(ms)", getResourceTimeInMilliSecond);
            map.put("BatchAccess-QPS", (long)(actualQueryCount * 1000.0 / queryTimeInMilliSecond));
            map.put("BatchAccess-Overall Hit Rate%", (long)(100.0 * hitCount / requestCount));
            map.put("BatchAccess-Overall Average Query Time(us)", (long)((finishTime - initedTime) / (double)1000.0 / requestCount));

            return map;
        }

    }
    Statics statics = new Statics();

    public BatchAdjListAccess(BatchKeyValueDBConnector<Long, long[]> db, KVCacheSystem<Long, long[]> cacheSystem) {
        results = new LongObjectHashMap<long[]>();
        requestsVid = new HashSet<Long>();
        this.db = db;
        this.cache = cacheSystem;
    }

    /**
     * Add a adjacency list access request to the batch.
     * @param vid
     */
    final public void addRequest(long vid) {
        long[] valueInCache = cache.get(vid);
        if ( valueInCache != null) {
            results.put(vid, valueInCache);
            statics.hitCount++;
        } else {
            requestsVid.add(vid);
            statics.queryCount++;
        }
        statics.requestCount++;
    }

    /**
     * Execute the batch query.
     * @throws Exception
     */
    final public void exec() throws Exception {
        long t1 = System.currentTimeMillis();
        db.setup();
        long t2 = System.currentTimeMillis();
        List<long[]> dbResults = db.query(requestsVid.iterator());
        Iterator<Long> requestsVidIter = requestsVid.iterator();
        Iterator<long[]> resultsIter = dbResults.iterator();
        while (requestsVidIter.hasNext()) {
            results.put(requestsVidIter.next(), resultsIter.next());
        }
        if (resultsIter.hasNext()) {
            throw new Exception("Get more result than we quest!");
        }
        long t3 = System.currentTimeMillis();
        statics.getResourceTimeInMilliSecond = (t2 - t1);
        statics.queryTimeInMilliSecond = (t3 - t2);
        statics.actualQueryCount = requestsVid.size();
        statics.finishTime = System.nanoTime();
        logClient.logMetrics(statics.getStaticsMap());
    }

    final public long[] getAdjList(long vid) {
        long neighbors[] = results.get(vid);
        cache.put(vid, neighbors);
        return results.get(vid);
    }

    final public void cleanup() {
        db.cleanup();
    }

    final public Statics getStatics() {return statics;}

}
