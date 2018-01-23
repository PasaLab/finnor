package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity

/**
  * Created by wangzhaokang on 8/8/17.
  */
object VertexSimilarityConfiguration {
  val CONF_JOB_SIMLARIFY_FUNCTION_CLASS = "jobconf.similarity.function.class"
  val CONF_JOB_INPUT_FILE_PATH = "jobconf.input.path"
  val CONF_JOB_OUTPUT_FILE_PATH = "jobconf.output.path"
  val CONF_PARTITIONER_CLASS_NAME = "partitioner.class.name"
  val CONF_PARTITIONER_PARTITION_NUMBER = "partitioner.partition.number"
  val CONF_JOB_SIMILARITY_FUNCTION_NAME = "jobconf.similarity.function.name"
  val CONF_JOB_TOP_K = "jobconf.top.k"
  val CONF_JOB_CONSIDER_VERTEX_ORDER = "jobconf.consider.vertex.order"
  val CONF_JOB_DB_BATCH_QUERY_SIZE="jobconf.db.batch.query.size"
  val CONF_IGNORE_DEGREE_TABLE="jobconf.ignore.degree.table"


}
