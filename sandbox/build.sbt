scalikejdbcSettings

scalaVersion := "2.10.1"

resolvers ++= Seq(
  "Sonatype releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "com.github.seratch" %% "scalikejdbc" % "1.4.9-SNAPSHOT",
  "com.github.seratch" %% "scalikejdbc-interpolation" % "1.4.9-SNAPSHOT",
  "org.slf4j" % "slf4j-simple" % "[1.7,)",
  "org.hibernate" %  "hibernate-core" % "4.1.9.Final",
  "org.hsqldb" % "hsqldb" % "[2,)",
  "org.specs2" %% "specs2" % "1.14" % "test"
)

initialCommands := """import scalikejdbc._
import scalikejdbc.SQLInterpolation._
import scala.reflect.runtime.universe.TypeTag
// -----------------------------
Class.forName("org.hsqldb.jdbc.JDBCDriver")
ConnectionPool.singleton("jdbc:hsqldb:mem:test", "", "")
DB autoCommit { implicit s =>
  try {
    // create tables
    sql"create table member(id bigint primary key not null, name varchar(255), company_id bigint)".execute.apply()
    sql"create table company(id bigint primary key not null, name varchar(255))".execute.apply()
    // insert data
    sql"insert into member values (${1}, ${"Alice"}, null)".update.apply()
    sql"insert into member values (${2}, ${"Bob"}, ${1})".update.apply()
    sql"insert into member values (${3}, ${"Chris"}, ${1})".update.apply()
    sql"insert into company values (${1}, ${"Typesafe"})".update.apply()
  } catch { case e: Exception => println(e.getMessage) }
}
// -----------------------------
// member
case class Member(id: Long, name: Option[String], companyId: Option[Long], company: Option[Company])
object Member extends SQLSyntaxSupport[Member] { 
  def apply(u: ResultName[Member], c: ResultName[Company])(rs: WrappedResultSet)(implicit tag: TypeTag[Member]): Member = {
    (apply(u)(rs)).copy(company = rs.longOpt(c.id).map(id => Company(rs.long(c.id), rs.stringOpt(c.name))))
  }
} 
// company
case class Company(id: Long, name: Option[String])
object Company extends SQLSyntaxSupport[Company]
// -----------------------------
// Query Examples
// -----------------------------
val members: Seq[Member] = DB readOnly { implicit s =>
  val (u, c) = (Member.syntax, Company.syntax)
  sql"select ${u.result.*}, ${c.result.*} from ${Member.as(u)} left join ${Company.as(c)} on ${u.companyId} = ${c.id}"
   .map(Member(u.resultName, c.resultName)).list.apply()
}
println("-------------------")
members.foreach(member => println(member))
println("-------------------")
"""

