name := "FinNOR"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2" % "provided"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2" % "provided"

libraryDependencies += "redis.clients" % "jedis" % "2.9.0"

libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "2.4.0"

libraryDependencies += "com.github.blemale" %% "scaffeine" % "2.0.0" % "compile"

libraryDependencies += "com.carrotsearch" % "hppc" % "0.7.2"

libraryDependencies += "org.rocksdb" % "rocksdbjni" % "5.0.1"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0"

libraryDependencies += "net.sf.trove4j" % "trove4j" % "3.0.3"

assemblyMergeStrategy in assembly := {
  case x if x.endsWith(".class") => MergeStrategy.last
  case x if x.endsWith(".properties") => MergeStrategy.last
  case x if x.contains("/resources/") => MergeStrategy.last
  case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
  case x if x.startsWith("META-INF/mimetypes.default") => MergeStrategy.first
  case x if x.startsWith("META-INF/maven/org.slf4j/slf4j-api/pom.") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    if (oldStrategy == MergeStrategy.deduplicate)
      MergeStrategy.first
    else
      oldStrategy(x)
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)
