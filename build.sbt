name := "CustomSparkWindowOperator"
organization := "io.nomad47"

version := "0.1"
scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "https://maven.atlassian.com/repository/public/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)

assemblyMergeStrategy in assembly ~= { mergeStrategy =>
  entry => {
    val strategy = mergeStrategy(entry)
    if (strategy == MergeStrategy.deduplicate) MergeStrategy.first else strategy
  }
}
assemblyJarName in assembly := name.value + "-assembly_" + scalaBinaryVersion.value + "-" + version.value + ".jar"