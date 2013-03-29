package scalikejdbc.interpolation

import org.scalatest._
import org.scalatest.matchers._

import scalikejdbc._
import scalikejdbc.SQLInterpolation._

case class Name(name: String)
case class Group(id: Int, groupName: String)
case class Member(id: Int, firstName: Option[String], lastName: Name, groupId: Option[Int], group: Option[Group], age: Int = 25)
object Member extends SQLSyntaxSupport[Member] {
  override val tableName = "applymethodgenerationspec"
  override val typeConverter: TypeConverter = { case ("lastName", s: String) => Name(s) }
}

class ApplyMethodGenerationSpec extends FlatSpec with ShouldMatchers with LogSupport {

  import scalikejdbc.SQLInterpolation._

  behavior of "ApplyMethodGeneration"

  val props = new java.util.Properties
  using(new java.io.FileInputStream("scalikejdbc-library/src/test/resources/jdbc.properties")) { in => props.load(in) }
  val driverClassName = props.getProperty("driverClassName")
  val url = props.getProperty("url")
  val user = props.getProperty("user")
  val password = props.getProperty("password")

  Class.forName(driverClassName)
  val poolSettings = new ConnectionPoolSettings(initialSize = 50, maxSize = 50, validationQuery = null)
  ConnectionPool.singleton(url, user, password, poolSettings)

  implicit val session = AutoSession

  val tableName = sqls"applymethodgenerationspec"
  try {
    sql"drop table ${tableName} if exists".execute.apply()
    sql"create table ${tableName} (id int not null, first_name varchar(256), last_name varchar(256), group_id int)".execute.apply()
  } catch {
    case e: Exception =>
      log.info(s"member table is already created (${e.getMessage})")
  }
  try {
    sql"delete from ${tableName}".execute.apply()
    sql"insert into ${tableName} values (1, 'Alice', 'Wonderland', null)".update.apply()
    sql"insert into ${tableName} values (2, 'Bob', 'Marley', null)".update.apply()
    sql"insert into ${tableName} values (3, 'Chris', 'Birchall', null)".update.apply()
  } catch {
    case e: Exception =>
      log.info(s"member records already exists (${e.getMessage})")
  }

  it should "be available" in {
    val m = Member.syntax("m")
    val members = sql"select ${m.result.*} from ${Member as m} order by ${m.id}".map(Member(m.resultName)).list.apply()
    members.size should equal(3)
    members.head.id should equal(1)
    members.head.firstName should equal(Some("Alice"))
    members.head.lastName should equal(Name("Wonderland"))
    // https://issues.scala-lang.org/browse/SI-6468
    // members.head.age should equal(25)
    members.head.age should equal(0)
  }

}
