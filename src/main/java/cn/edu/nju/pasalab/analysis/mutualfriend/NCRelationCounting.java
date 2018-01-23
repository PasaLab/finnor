package cn.edu.nju.pasalab.analysis.mutualfriend;

import cn.edu.nju.pasalab.graph.util.cache.CaffeineProcessLevelCache;
import cn.edu.nju.pasalab.graph.util.db.batchaccess.BatchAdjListAccess;
import cn.edu.nju.pasalab.graph.util.db.redis.RedisBatchConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;

/**
 * Created by dell on 2017/4/7.
 */
public class NCRelationCounting {
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

    public static void main(String args[]) throws IOException {
        String graphAdjFile = args[0];
        String outputFile = args[1];
        int partitionNum = Integer.parseInt(args[2]);
        int maxBatchSize = Integer.parseInt(args[3]);
        boolean onlyOneHop = java.lang.Boolean.parseBoolean(args[4]);
        boolean trueOutput = Boolean.parseBoolean(args[5]);

        SparkConf conf = new SparkConf().setAppName("NC Relation Finding");
        System.err.println(conf.toDebugString());

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
                    calcNCRelationInBatch(vidList, neighborList, writer, onlyOneHop, trueOutput, accumulators);
                    vidList.clear();
                    neighborList.clear();
                } // end of batch process
            }

            if (vidList.size() > 0) {
                calcNCRelationInBatch(vidList, neighborList, writer, onlyOneHop, trueOutput, accumulators);
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
    }


    /**
     for each vid, calculate those vids which contain the vid;
     eg: for vid A, result NCRelationArr[B, C] means A is contained by B and C
     */

    private static void calcNCRelationInBatch(List<Long> vidList, List<long[]> neighborList, PrintWriter writer,
                                              boolean onlyOneHop, boolean trueOutput,
                                              AccumulatorGroup accumulators) throws Exception {

        BatchAdjListAccess client = new BatchAdjListAccess(new RedisBatchConnector(),
                CaffeineProcessLevelCache.getInstance());

        for (int i = 0; i < vidList.size(); i++) {
            long neighbor[] = neighborList.get(i);
            for (int j = 0; j < neighbor.length; j++) {
                client.addRequest(neighbor[j]);
            }
        }
        client.exec();

        for (int i = 0; i < vidList.size(); i++) {
            final long currentVid = vidList.get(i);
            long neighbor[] = neighborList.get(i);
            ArrayList<Long> NCRelation = new ArrayList();
            if (!onlyOneHop) {
                long nvid1 = neighbor[0];
                if (nvid1 < 0)
                    System.err.printf("nvid1 < 0, %f\n", (nvid1 / currentVid));
                long[] neighborOfFirstNeighbor = client.getAdjList(nvid1);
                LinkedList<Long> currentCandidates = initFirstNeighbor(neighborOfFirstNeighbor, nvid1,currentVid);
                int j = 1;
                while (j < neighbor.length && !currentCandidates.isEmpty()) {
                    long[] neighborOfNeighbor = client.getAdjList(neighbor[j]);
                    intersect(currentCandidates, neighborOfNeighbor, neighbor[j]);
                    j++;
                }
                NCRelation.addAll(currentCandidates);
            } else {
                // only one hop
                for (int j = 0; j < neighbor.length; j++){
                    long[] neighborOfNeighbor = client.getAdjList(neighbor[j]);
                    //isNCRelation(A,B) represents A is contained by B
                    if(isNCRelation(neighbor,neighborOfNeighbor))
                        NCRelation.add(neighbor[j]);
                }
            }
            if (trueOutput) {
                for (int j = 0; j < NCRelation.size(); j++) {
                    long nvid = NCRelation.get(j);
                    writer.println(currentVid + " " + nvid);
                }
            }
        }

        if (writer != null)
            writer.flush();
        // cleanup
        client.cleanup();
        accumulators.queryTimeAccumu.add(client.getStatics().queryTimeInMilliSecond);
        accumulators.clientSetupCountAccumu.add(1);
        accumulators.resourceGetTimeAccumu.add(client.getStatics().getResourceTimeInMilliSecond);
        accumulators.requestCount.add(client.getStatics().requestCount);
        accumulators.hitCountAccumu.add(client.getStatics().hitCount);
        accumulators.queryCountAccumu.add(client.getStatics().queryCount);
    }

    private static LinkedList<Long> initFirstNeighbor(long[] neighbor, long nvid, long cvid){
        LinkedList<Long> result = new LinkedList<>();
        int i = 0;
        boolean flag = false;
        while (neighbor != null && i < neighbor.length) {
            if (!flag && neighbor[i] > nvid) {
                result.add(nvid);
                flag = true;
            }
            if (neighbor[i] != cvid) {
                result.add(neighbor[i]);
            }
            i++;
        }
        if (!flag) {
            result.add(nvid);
        }
        return result;
    }

    private static void intersect(LinkedList<Long> local, long[] list, long nvid){
        Iterator<Long> localIter = local.iterator();
        int pos = 0;
        while (localIter.hasNext()) {
            long vid = localIter.next();
            if (vid != nvid) {
                // find list[pos] == vid
                if (pos == list.length) {
                    // all elements in list have been visited
                    localIter.remove();
                } else if (list[pos] > vid) {
                    localIter.remove();
                } else if (list[pos] == vid) {
                    // reserve this vid and move to next one in list
                    pos += 1;
                } else {
                    // list[pos] < vid, then skip all elements in list that are less than vid
                    while (pos < list.length && list[pos] < vid) {
                        pos++;
                    }
                    if (pos == list.length || list[pos] > vid) {
                        // can not find list[pos] == vid, remove vid from local
                        localIter.remove();
                    } else {
                        // find list[pos] == vid, reserve it
                        pos++;
                    }
                }
            } // end of outer-if
        } // end of while
    } // end of method

    private static boolean isNCRelation(long a[], long b[]) {
        int i = 0;
        int j = 0;
        while (i < a.length && j < b.length){
            if (a[i] < b[j])
                return false;
            else if (a[i] > b[j])
                j ++;
            else if (a[i] == b[j]){
                i ++;
                j ++;
            }
        }
        if (i == a.length)
            return true;
        else
            return false;
    }
}
