# FinNOR: Toward Large-scale Pairwise Vertex Similarity Calculation on Distributed Data-parallel Platforms

- [FinNOR: Toward Large-scale Pairwise Vertex Similarity Calculation on Distributed Data-parallel Platforms](#finnor-toward-large-scale-pairwise-vertex-similarity-calculation-on-distributed-data-parallel-platforms)
    - [Prepare](#prepare)
        - [Compile](#compile)
        - [Redis](#redis)
    - [Compile](#compile)
    - [Run](#run)
        - [Prepare input data](#prepare-input-data)
            - [Graph file organization](#graph-file-organization)
            - [Configuration files](#configuration-files)
        - [Start the log server](#start-the-log-server)
        - [Run FinNOR](#run-finnor)
            - [FinNOR-MR](#finnor-mr)
            - [FinNOR-MR-MEM](#finnor-mr-mem)
    - [Output](#output)
        - [Output files](#output-files)
        - [Log server output](#log-server-output)
            - [FinNOR-MR](#finnor-mr)
            - [FinNOR-MR-MEM](#finnor-mr-mem)
        - [Program output](#program-output)
            - [FinNOR-MR](#finnor-mr)
            - [FinNOR-MR-MEM](#finnor-mr-mem)
    - [Licence](#licence)

## Prepare

### Compile

Java 8, Scala 2.11.8 and Sbt.

The program is tested under Spark 2.0.2 and Hadoop 2.7.2.

### Redis

Redis is installed on each worker node in the cluster.

Run a Redis instance on each worker node with the configuration file `redis.conf` in this directory.

The default port 6379 must be accessible.

## Compile

Run `sbt assembly` in the root directory.

> Note: A precompiled jar is provided in the `lib` directory. The jar is compiled with `sbt assembly` from [this Github repo](https://github.com/wangzk/my-util-codebase/tree/master/scala/AkkaBasedRemoteLogger).

The compiled jar is available at `target/scala-2.11/FinNOR-assembly-1.0.jar`

## Run

### Prepare input data

#### Graph file organization

Each graph should have its own directory on HDFS.

Under the directory, there should a text file (or a directory of several text files) containing the adjacency sets of the graph.

The adjacency file has the following format in each line:

    VID Adj1 Adj2 Adj3 ...

Each field is separated by a space.

`VID` represents the vertex ID of the current line.

`Adj1 Adj2 Adj3 ...` represents the adjacent vertex IDs of the current vertex. 

The graph must be undirected. If vertex A appears in vertex B's adjacency set, vertex B must appear in vertex A's adjacency set.

The demo graph in the paper is provided under the `demo_graph` directory.

**Note:** In FinNOR, we do not further repartition the file. Therefore, to achieve a high parallelism, split the adjacency file to small equal-size files and store them in a directory. Since we read the graph by `sc.textFile(inputPath)` in Spark, a directory is acceptable as the input adjacency file path. The number of the small files is recommended equal to or larger than the number of total cores of the Spark cluster.

**Note for FinNOR-MR-MEM:**  FinNOR-MR-MEM respects the file split on the HDFS. Therefore, split the adjacency file guided by a balanced smart graph partition strategy can improve the cache hit rate and the performance.

#### Configuration files

To run FinNOR, the following configuration files should be prepared in advance.

**akkalogger.conf**

Set the log server in the format of `hostname:port` where `hostname` is the host that runs the log server and the `port` is an unused port for the log server.

We recommend running the log server on the master node in the cluster.

**caffeine.config**

This file configures the capacity of the process-level cache in FinNOR-MR-MEM. The unit of the `capacity` is byte.

**redis.cluster.conf**

This file configures the redis cluster information.

1.  `servers`: The list of the hosts that run Redis servers. Hosts are separated by the comma.
2.  `pool.size`: The number of redis connections opened in each Spark executor. Recommend setting the value to the number of logical cores on the node.

### Start the log server

The log server collects the debug logs from the remote Spark executors. 

**It must be started before running FinNOR.**

Start the log server with the script:

    bash ./start_log_server.sh

### Run FinNOR

#### FinNOR-MR

The entry of FinNOR-MR is defined in the scala class `cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.SparkMRDriver`.

Run FinNOR-MR with the following command:

```bash
spark-submit --master [SPARK_MASTER_ADDRESS] \
--driver-memory 50g \
--executor-memory 50g \
--total-executor-cores [Number of executors] \
--executor-cores 1 \
--conf spark.memory.fraction=0.1 \
--files akkalogger.conf,caffeine.config \
--class cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.SparkMRDriver \
FinNOR-assembly-1.0.jar \
jobconf.similarity.function.class=cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.JaccardLikeIndexComputingFunction \
jobconf.similarity.function.name=[Similarity function name] \
jobconf.input.path=[Path to the adjacency file of the graph] \
jobconf.output.path=[Path to the output file] \
partitioner.partition.number=64 \
partitioner.class.name=cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework.ModPartitioner \
graphadjstore.inmemory.compactstore=false \
graphadjstore.inmemory.memory.threshold.mb=40480 \
jobconf.consider.vertex.order=true \
jobconf.ignore.degree.table=false
```

The arguments contain the following configuration items:
1. `jobconf.similarity.function.class`, the class name of the similarity function:
    _ `cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.JaccardLikeIndexComputingFunction` for CN and Jaccard.
    _ `cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.RALikeIndexComputingFunction` for RA
2. `jobconf.similarity.function.name`, the similarity function name: `CN`, `jaccard` and `RA`. 
3. `jobconf.input.path`, the path to the input adjacency file (or directory).
4. `jobconf.output.path`, the path to the output directory. The output will be stored in a directory containing several text files. Set this to `null` if you do not want the output which is usually very large. In all our experiments in the paper, we set this to `null`.
5. `partitioner.partition.number`, the number of graph partitions.
6. `partitioner.class.name`, the graph partitioner class. To get more details, see the source code `src\main\scala\cn\edu\nju\pasalab\graph\analysis\vertexsimilarity\framework\Partitioner.scala` file.
7. `graphadjstore.inmemory.compactstore`, set this to `false`.
8. `graphadjstore.inmemory.memory.threshold.mb`, FinNOR-MR stores the graph partition to the out-of-core RocksDB storage if it is larger than the threshold here(measured in Mbytes). This feature is still experimental. Set this value larger than the available memory to avoid triggering the RocksDB storage.
9. `jobconf.consider.vertex.order`, always set it to `true` in the PVSC problem.
10. `jobconf.ignore.degree.table`, ignore the degree lookup table preparation phase. Set this to `true` if you use the CN as the similarity function.

#### FinNOR-MR-MEM

The entry of FinNOR-MR-MEM is defined in the scala class `cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.InMemoryDriver`.

**Remember to clear the Redis database before running the FinNOR-MR-MEM!**

Run FinNOR-MR-MEM with the following command:

```bash
spark-submit --master spark://slave001:7078 \
--driver-memory 50g \
--executor-memory 50g \
--total-executor-cores [Total cores in the Spark cluster] \
--executor-cores [Total available cores on a Spark worker] \
--conf spark.memory.fraction=0.1 \
--files akkalogger.conf,caffeine.config,redis.cluster.conf \
--class cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.InMemoryDriver \
FinNOR-assembly-1.0.jar \
jobconf.similarity.function.class=cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.JaccardLikeIndexComputingFunction \
jobconf.similarity.function.name=[Similarity function name] \
jobconf.input.path=${INPUT} \
jobconf.output.path=${OUTPUT} \
jobconf.db.batch.query.size=1000 \
jobconf.consider.vertex.order=true \
jobconf.ignore.degree.table=false
```

The command contains the following configuration items:

1. `jobconf.similarity.function.class`, the class name of the similarity function:
    _ `cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.JaccardLikeIndexComputingFunction` for CN and Jaccard.
    _ `cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.RALikeIndexComputingFunction` for RA
2. `jobconf.similarity.function.name`, the similarity function name: `CN`, `jaccard` and `RA`. 
3. `jobconf.input.path`, the path to the input adjacency file (or directory).
4. `jobconf.output.path`, the path to the output directory. The output will be stored in a directory containing several text files. Set this to `null` if you do not want the output which is usually very large. In all our experiments in the paper, we set this to `null`.
5. `jobconf.db.batch.query.size=1000`, the batch size. Several vertices form a group. FinNOR-MR-MEM queries the adjacency sets of the neighbors of the vertices in a group in a batch.
6. `jobconf.consider.vertex.order`, always be `true` in the PVSC problem.
7. `jobconf.ignore.degree.table`, ignore the degree lookup table preparation phase. Set this to `true` if you use the CN as the similarity function.

## Output

### Output files

The output files are text files with the following format:
`vid1,vid2 score`. It means that the similarity score of the vertex pair (`vid1`, `vid2`) is `score`.

### Log server output

The log server also output some useful information.

#### FinNOR-MR

The total number of similar vertex pairs with non-zero similarity scores will be reported in the `SUM` column of the log output:

    ===== Metrics Info =====
    2018-01-23 18:34:37.419
    METRIC  COUNT   SUM     AVERAGE
    Result Pair Num ??       [Number of Similar Vertex Pairs]      ??
    ===== END =====

#### FinNOR-MR-MEM

The `SUM` column of `Result Pair Num` item reports the total number of similar vertex pairs with non-zero similarity scores.  

The `AVERAGE` column of `BatchAccess-Overall Hit Rate%` item reports the average cache hit rate.

    ===== Metrics Info =====
    2018-01-23 18:36:18.856
    METRIC  COUNT   SUM     AVERAGE
    BatchAccess-ActualQueryCount    2       15      7.50
    BatchAccess-DBQueryTime(ms)     2       3       1.50
    BatchAccess-GetDBClientsTime(ms)        2       83      41.50
    BatchAccess-Overall Average Query Time(us)      2       7885    3942.50
    BatchAccess-Overall Hit Rate%   2       0       0.00
    BatchAccess-QPS 2       12000   6000.00
    Framework-Batch compression ratio       2       92      46.00
    Framework-Compute time (ms)     2       45      22.50
    Framework-DB query time (ms)    2       746     373.00
    Framework-Total time (ms)       2       791     395.50
    Result Pair Num 2       29      14.50
    ===== END =====

### Program output

#### FinNOR-MR

The section `Wall-clock total execution time` in the program output reports the wall-clock execution time of FinNOR-MR.

    --> Performance metrics:
        a. Wall-clock total execution time:5353 ms
        a. Wall-clock degree preparation time:3406 ms
        a. Wall-clock framework time:1947 ms

#### FinNOR-MR-MEM

The section `Wall-clock total execution time` in the program output reports the wall-clock execution time of FinNOR-MR-MEM.

    --> Performance metrics:
        a. Wall-clock total execution time:9180 ms
        a. Wall-clock degree preparation time:3534 ms
        a. Wall-clock framework time:5646 ms

## Licence

Please contact the authors for the licence info of the source code.
