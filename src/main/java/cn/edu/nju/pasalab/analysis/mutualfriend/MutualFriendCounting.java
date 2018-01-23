package cn.edu.nju.pasalab.analysis.mutualfriend;

import cn.edu.nju.pasalab.graph.util.cache.CaffeineProcessLevelCache;
import cn.edu.nju.pasalab.graph.util.cache.KVCacheSystem;
import cn.edu.nju.pasalab.graph.util.cache.NullCache;
import cn.edu.nju.pasalab.graph.util.db.batchaccess.BatchAdjListAccess;
import cn.edu.nju.pasalab.graph.util.db.redis.RedisBatchConnector;
import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import wzk.akkalogger.client.AkkaLoggerClient;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;

/**
 * Created by wangzhaokang on 3/16/17.
 */
public class MutualFriendCounting {
    private static AkkaLoggerClient loggerClient = AkkaLoggerClient.processLevelClient();

    static class AccumulatorGroup implements Serializable {

        LongAccumulator clientSetupCountAccumu;
        LongAccumulator hitCountAccumu;
        LongAccumulator queryCountAccumu;
        LongAccumulator requestCount;
        LongAccumulator resourceGetTimeAccumu;
        LongAccumulator queryTimeAccumu;

        public AccumulatorGroup(LongAccumulator cs,LongAccumulator  hc,LongAccumulator  qc,
                                LongAccumulator  rc,LongAccumulator  rgt,LongAccumulator  qta) {
            this.clientSetupCountAccumu = cs;
            this.hitCountAccumu = hc;
            this.queryCountAccumu = qc;
            this.requestCount =rc;
            this.resourceGetTimeAccumu = rgt;
            this.queryTimeAccumu = qta;
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        String graphAdjFile = args[0];
        String outputFile = args[1];
        int partitionNum = Integer.parseInt(args[2]);
        int maxBatchSize = Integer.parseInt(args[3]);
        boolean onlyOneHop = java.lang.Boolean.parseBoolean(args[4]);
        boolean trueOutput = Boolean.parseBoolean(args[5]);

        SparkConf conf = new SparkConf().setAppName("Mutual Friend Counting");
        System.err.println(conf.toDebugString());


        loggerClient.clearPreviousMetrics("New spark program start");

        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator clientSetupCountAccumu =  sc.sc().longAccumulator("client setup count");
        LongAccumulator hitCountAccumu = sc.sc().longAccumulator("hit count");
        LongAccumulator queryCountAccumu = sc.sc().longAccumulator("query count");
        LongAccumulator requestCount = sc.sc().longAccumulator("request count");
        LongAccumulator resourceGetTimeAccumu = sc.sc().longAccumulator("resource get time");
        LongAccumulator queryTimeAccumu = sc.sc().longAccumulator("query time");

        AccumulatorGroup accumulators = new AccumulatorGroup(clientSetupCountAccumu, hitCountAccumu,
                queryCountAccumu, requestCount, resourceGetTimeAccumu, queryTimeAccumu);

        JavaRDD<String> adjTextFileRDD = sc.textFile(graphAdjFile, partitionNum);
        JavaPairRDD<Long, long[]> adjListRDD = adjTextFileRDD.mapToPair(
                line -> {
                    String fields[] = line.split(" ");
                    long vid = Long.parseLong(fields[0]);
                    long neighbors[] = new long[fields.length - 1];
                    for (int i = 0; i < neighbors.length; i++) {
                        neighbors[i] = Long.parseLong(fields[i + 1]);
                    }
                    Arrays.sort(neighbors);
                    return Tuple2.apply(vid, neighbors);
                }).setName("AdjListRDD").cache();
        System.err.printf("|V|=%d.\n", adjListRDD.count());

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(outputFile), true);
        boolean ret = fs.mkdirs(new Path(outputFile));
        if (ret != true) {
            System.err.println("Can not delete output dir.");
            System.exit(1);
        }

        long t1 = System.currentTimeMillis();
        adjListRDD.foreachPartition(iterator -> {
            ArrayList<Long> vidList = new ArrayList<>();
            ArrayList<long[]> neighborList = new ArrayList<>();
            FileSystem hdfs = FileSystem.get(new Configuration());
            DataOutputStream out = null;
            PrintWriter writer = null;

            while (iterator.hasNext()) {
                Tuple2<Long, long[]> pair = iterator.next();
                long vid = pair._1();
                long[] neighbor = pair._2();
                vidList.add(vid);
                neighborList.add(neighbor);
                if (trueOutput && out == null) {
                    out = hdfs.create(new Path(outputFile, "part-" + vid));
                    writer = new PrintWriter(out);
                }

                if (vidList.size() > maxBatchSize) {
                    calcMutualFriendInBatch(vidList, neighborList, writer, onlyOneHop, trueOutput, accumulators);
                    vidList.clear();
                    neighborList.clear();
                } // end of batch process
            }

            if (vidList.size() > 0) {
                calcMutualFriendInBatch(vidList, neighborList, writer, onlyOneHop, trueOutput, accumulators);
                vidList.clear();
                neighborList.clear();
            }
            if (out != null) {
                writer.close();
                out.close();
            }
        });
        long t2 = System.currentTimeMillis();

        System.out.println("Time consuming(ms):" + (t2 - t1));

        System.out.printf("Accumulators:\n" +
                "Client setup count: %d\n" +
                        "Request count: %d\n" +
                        "Hit count:%d\n" +
                        "Query count:%d\n" +
                        "Get resource time (ms):%d\n" +
                        "Query time (ms):%d\n",
                accumulators.clientSetupCountAccumu.value(),
                accumulators.requestCount.value(),
                accumulators.hitCountAccumu.value(),
                accumulators.queryCountAccumu.value(),
                accumulators.resourceGetTimeAccumu.value(),
                accumulators.queryTimeAccumu.value()
                );

        sc.close();
        loggerClient.writeMetricToFile("pasa.mutualfriend.metrics.log");
        Thread.sleep(1000);
        System.exit(0);
    }


    private static void calcMutualFriendInBatch(List<Long> vidList, List<long[]> neighborList, PrintWriter writer,
                                               boolean onlyOneHop, boolean trueOutput,
                                                AccumulatorGroup accumulators) throws Exception {
        long t1,t2,t3 = 0;

        t1 = System.nanoTime();
        BatchAdjListAccess client = getAdjAccess();

        LongScatterSet requestVidsSet = new LongScatterSet();
        long totalRequestCount = 0;

        for (int i = 0; i < vidList.size(); i++) {
            long neighbor[] = neighborList.get(i);
            for (int j = 0; j < neighbor.length; j++) {
                client.addRequest(neighbor[j]);
                requestVidsSet.add(neighbor[j]);
                totalRequestCount++;
            }
        }

        double batchCompressionRatio = 100.0 * requestVidsSet.size() / totalRequestCount;

        client.exec();
        t2 = System.nanoTime();

        long NORelationNum = 0;
        long TotalSum = 0;

        for (int i = 0; i < vidList.size(); i++) {
            final long currentVid = vidList.get(i);
            long neighbor[] = neighborList.get(i);
            LongIntMap countMap = new LongIntHashMap();
            for (int j = 0; j < neighbor.length; j++) {
                long[] neighborOfNeighbor = client.getAdjList(neighbor[j]);
                if (!onlyOneHop) {
                    for (int k = 0; k < neighborOfNeighbor.length; k++) {
                        if (neighborOfNeighbor[k] > currentVid) {
                            countMap.addTo(neighborOfNeighbor[k], 1);
                            TotalSum += 1;
                        }
                    }
                } else {
                    if (neighbor[j] < currentVid) continue;
                    int count = insertsectionCount(neighbor, neighborOfNeighbor);
                    if (count > 0) {
                        countMap.put(neighbor[j], count);
                    }

                }
            }
            NORelationNum += countMap.size();
            if (trueOutput) {
                Iterator<LongIntCursor> iterator = countMap.iterator();
                while (iterator.hasNext()) {
                    LongIntCursor cursor = iterator.next();
                    if (cursor.key != currentVid) {
                        writer.println(currentVid + "," + cursor.key + " " + cursor.value);
                    }
                }
            }
        } // calc mutual friend

        if (writer != null)
            writer.flush();
        // cleanup
        client.cleanup();
        t3 = System.nanoTime();

        HashMap<String,Long> metricsMap = new HashMap<>();
        metricsMap.put("Query Time (us)", (t2 - t1)/1000);
        metricsMap.put("Compute Time (us)", (t3 - t2)/1000);
        metricsMap.put("Batch compression ratio%", (long)batchCompressionRatio);
        metricsMap.put("NO Relation", NORelationNum);
        metricsMap.put("Addition Op", TotalSum);
        loggerClient.logMetrics(metricsMap);

        accumulators.queryTimeAccumu.add(client.getStatics().queryTimeInMilliSecond);
        accumulators.clientSetupCountAccumu.add(1);
        accumulators.resourceGetTimeAccumu.add(client.getStatics().getResourceTimeInMilliSecond);
        accumulators.requestCount.add(client.getStatics().requestCount);
        accumulators.hitCountAccumu.add(client.getStatics().hitCount);
        accumulators.queryCountAccumu.add(client.getStatics().queryCount);
    }

    private static int insertsectionCount(long a[], long b[]) {
        int result = 0;
        int i = 0, j = 0;
        while (i < a.length && j < b.length) {
            if (a[i] < b[j]) i++;
            else if (b[j] < a[i]) j++;
            else {
                result++; i++; j++;
            }
        }
        return result;
    }


    private static BatchAdjListAccess getAdjAccess() {
        KVCacheSystem caffeineCache = CaffeineProcessLevelCache.getInstance();
        long capacity = caffeineCache.getCapacity();
        if (capacity == 0) {
            System.err.println("Now we choose NullCache interface.");
            return new BatchAdjListAccess(new RedisBatchConnector(), new NullCache<Long, long[]>());
        } else {
            return new BatchAdjListAccess(new RedisBatchConnector(),
                    CaffeineProcessLevelCache.getInstance());
        }
    }

}
