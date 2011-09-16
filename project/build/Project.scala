import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo) extends StandardLibraryProject(info)
  with LibDirClasspath
  with IdeaProject {

  override def dependencyPath = "lib"
  override def compileOrder = CompileOrder.ScalaThenJava
  override def testOptions = super.testOptions ++ Seq(TestArgument(TestFrameworks.JUnit, "-q", "-v"))

  override def repositories = Set[Resolver]("cloudera" at "https://repository.cloudera.com/content/repositories/releases/")

  override def ivyXML =
    <dependencies>
      <exclude org="org.schwering.irc" module="irclib"/>
      <exclude org="org.apache.thrift" module="libthrift"/>
      <exclude org="dk.brics.automaton" module="automaton"/>
      <exclude org="org.arabidopsis.ahocorasick" module="ahocorasick"/>
    </dependencies>

  val junitInterface = "com.novocode" % "junit-interface" % "0.6" % "test->default"

  val zookeeper = "org.apache.zookeeper" % "zookeeper" % "3.3.3"
  val log4j = "log4j" % "log4j" % "1.2.15"
  val jopt = "net.sf.jopt-simple" % "jopt-simple" % "3.2"
  val flume = "com.cloudera" % "flume-core" % "0.9.4-cdh3u1"
  val zkClient = "com.github.sgroschupf" % "zkclient" % "0.1"
}
