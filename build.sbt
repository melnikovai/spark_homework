ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark_homework"
  )

val sparkVersion = "3.5.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"
libraryDependencies += "io.lemonlabs" %% "scala-uri" % "4.0.3"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"

// https://github.com/typelevel/cats/issues/3628
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
)