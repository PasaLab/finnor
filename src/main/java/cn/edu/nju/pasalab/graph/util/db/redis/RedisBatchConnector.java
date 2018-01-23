package cn.edu.nju.pasalab.graph.util.db.redis;

import cn.edu.nju.pasalab.graph.util.SerializeHelper;
import cn.edu.nju.pasalab.graph.util.db.batchaccess.BatchKeyValueDBConnector;
import com.carrotsearch.hppc.IntArrayList;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Shareded Redis Accessor with Long key type and long[] value type.
 * This object should be used as a thread's local object and should not share among threads.
 * Created by wangzhaokang on 3/15/17.
 */
public class RedisBatchConnector implements BatchKeyValueDBConnector<Long, long[]> {

    private final static String CONFIG_FILE_NAME = "redis.cluster.conf";
    private final static int DB_INDEX = 3;
    private final static Logger logger = Logger.getLogger("cn.edu.nju.pasalab");

    private int serverNum;
    private Jedis[] clients;
    private Pipeline[] pipelines;
    private int[] shuffledIndex;

    private ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);


    private static int calcShardClusterId(long key, int serverNum) {
        return (int)Math.abs(key) % serverNum;
    }


    /** Apply for resources */
    @Override
    public void setup() {
        setup(DB_INDEX);
    }

    public void setup(int dbIndex) {
        clients = RedisClusterClientPools.getClusterClients();
        serverNum = clients.length;
        pipelines = new Pipeline[serverNum];
        shuffledIndex = new int[serverNum];
        for (int i = 0; i < serverNum; i++) {
            Jedis jedis = clients[i];
            pipelines[i] = jedis.pipelined();
            pipelines[i].select(dbIndex);
            shuffledIndex[i] = i;
        }
        Collections.shuffle(Arrays.asList(shuffledIndex));
    }

    @Override
    public List<long[]> query(Iterator<Long> keys) {
        ArrayList<Integer> keyToServerIdList = new ArrayList<Integer>();
        List<Response<byte[]>> serverResponses[] = new List[serverNum];

        for (int i = 0; i < serverNum; i++) {
            serverResponses[i] = new ArrayList<Response<byte[]>>();
        }

        while (keys.hasNext()) {
            long key = keys.next();
            int serverId = calcShardClusterId(key, this.serverNum);
            keyToServerIdList.add(serverId);
            serverResponses[serverId].add(pipelines[serverId].get(SerializeHelper.longToBytes(key)));
        }

        for (int i = 0; i < serverNum; i++) {
            pipelines[shuffledIndex[i]].sync();
        }

        // Now we can get the result
        List<long[]> results = new ArrayList<long[]>();
        Iterator<Response<byte[]>> responses[] = new Iterator[serverNum];
        for (int i = 0; i < serverNum; i++)
            responses[i] = serverResponses[i].iterator();

        for (int i = 0; i < keyToServerIdList.size(); i++) {
            int serverId = keyToServerIdList.get(i);
            byte[] currentResultByteArray = responses[serverId].next().get();
            long[] longArray = SerializeHelper.bytesToLongs(currentResultByteArray);
            results.add(longArray);
        }

        return results;
    }

    public int[] queryLongIntPairs(Iterable<? extends Long> queryKeys) {
          ArrayList<Integer> keyToServerIdList = new ArrayList<Integer>();
        List<Response<byte[]>> serverResponses[] = new List[serverNum];

        for (int i = 0; i < serverNum; i++) {
            serverResponses[i] = new ArrayList<Response<byte[]>>();
        }

        int k = 0;
        Iterator<? extends Long> iterator = queryKeys.iterator();

        while (iterator.hasNext()) {
            long key = iterator.next();
            int serverId = calcShardClusterId(key, this.serverNum);
            keyToServerIdList.add(serverId);
            serverResponses[serverId].add(pipelines[serverId].get(SerializeHelper.longToBytes(key)));
        }

        for (int i = 0; i < serverNum; i++) {
            pipelines[shuffledIndex[i]].sync();
        }

        // Now we can get the result
        IntArrayList results = new IntArrayList();
        Iterator<Response<byte[]>> responses[] = new Iterator[serverNum];
        for (int i = 0; i < serverNum; i++)
            responses[i] = serverResponses[i].iterator();

        for (int i = 0; i < keyToServerIdList.size(); i++) {
            int serverId = keyToServerIdList.get(i);
            byte[] currentResultByteArray = responses[serverId].next().get();
            int intValue = (int)SerializeHelper.bytesToLong(currentResultByteArray);
            results.add(intValue);
        }

        return results.toArray();
    }

    @Override
    public void set(Iterator<Long> keys, Iterator<long[]> values) {
        int count;
        while (keys.hasNext()) {
            long key = keys.next();
            long[] value = values.next();
            int serverId = calcShardClusterId(key, this.serverNum);
            pipelines[serverId].set(SerializeHelper.longToBytes(key), SerializeHelper.longsToBytes(value));
        }
        for (int i = 0; i < serverNum; i++) {
            pipelines[shuffledIndex[i]].sync();
        }
    }


    public void setLongIntPair(Iterator<Long> keys, Iterator<Integer> values) {
        while (keys.hasNext()) {
            long key = keys.next();
            int value = values.next();
            int serverId = calcShardClusterId(key, this.serverNum);
            pipelines[serverId].set(SerializeHelper.longToBytes(key), SerializeHelper.longToBytes(value));
        }
        for (int i = 0; i < serverNum; i++) {
            pipelines[shuffledIndex[i]].sync();
        }
    }

    class QueryTask implements Callable<Void>  {
        Pipeline pipeline;

        QueryTask(Pipeline p) {
            this.pipeline = p;
        }

        @Override
        public Void call() throws Exception {
            pipeline.sync();
            return null;
        }
    }

    @Override
    public void cleanup() {
        for (Pipeline pipeline : pipelines) {
            try {
                pipeline.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.log(Level.SEVERE, e.toString());
            }

        }
        RedisClusterClientPools.returnResources(clients);
    }
}
