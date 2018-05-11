name := "spark-cherry"

version := "0.0.2"
scalaVersion := "2.11.8"

sparkVersion := "2.2.0"

sparkComponents ++= Seq("sql","hive")

libraryDependencies ++= Seq(
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.0.1_0.4.7"
)
parallelExecution in Test := false