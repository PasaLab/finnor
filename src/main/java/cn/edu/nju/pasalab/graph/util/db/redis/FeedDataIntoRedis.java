package cn.edu.nju.pasalab.graph.util.db.redis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wangzhaokang on 3/16/17.
 */
public class FeedDataIntoRedis {
    public static void main(String args[]) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Feed Adj Data Into Redis");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputFilePath = args[0];
        int partitionNum = Integer.parseInt(args[1]);
        int batchSize = Integer.parseInt(args[2]);

        JavaPairRDD<Long, long[]> vertexRDD = sc.textFile(inputFilePath, partitionNum).mapToPair(line -> {
           String fields[] = line.split(" ");
           Long vid = Long.parseLong(fields[0]);
           long[] neighbor = new long[fields.length - 1];
            for (int i = 0; i < neighbor.length; i++) {
                neighbor[i] = Long.parseLong(fields[1+i]);
            }
           return new Tuple2<>(vid, neighbor);
        });
        feedGraphToRedis(vertexRDD, batchSize);
        sc.close();
    }


    public static void feedGraphToRedis(JavaPairRDD<Long, long[]> inputGraph) {
        feedGraphToRedis(inputGraph, 2000);
    }

    public static void feedGraphToRedis(JavaPairRDD<Long, long[]> inputGraph,
                                        int batchInsertSize) {
        inputGraph.foreachPartition(iterator -> {
            List<Long> vidList = new ArrayList<>();
            List<long[]> neighborList = new ArrayList<>();
            while (iterator.hasNext()) {
                Tuple2<Long, long[]> tuple = iterator.next();
                Long vid = tuple._1;
                long[] neighbor = tuple._2;
                Arrays.sort(neighbor);
                vidList.add(vid);
                neighborList.add(neighbor);
                if (batchInsertSize < vidList.size()) {
                    feedToRedis(vidList, neighborList);
                    vidList.clear();
                    neighborList.clear();
                }
            }
            if (vidList.size() > 0) {
                feedToRedis(vidList, neighborList);
                vidList.clear();
                neighborList.clear();
            }

        });
    }

    private static void feedToRedis(List<Long> vidList, List<long[]> neighborList) {
        RedisBatchConnector connector = new RedisBatchConnector();
        connector.setup();
        connector.set(vidList.iterator(), neighborList.iterator());
        connector.cleanup();
    }
}
