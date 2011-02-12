import sbt._

class ScalaCouchDBProject(info: ProjectInfo) 
  extends DefaultProject(info) {

  val scalaHttp = "syndicate42" %% "scala-http" % "1.0"
  val json = "com.twitter" %% "json" % "2.1.6-SNAPSHOT" % "compile"

  val specs = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.7" % "test"

  val jettyVersion = "7.2.2.v20101205"
  val jettyHttp = "org.eclipse.jetty" % "jetty-client" % jettyVersion % "test"
}

