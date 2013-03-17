package scalikejdbc.interpolation

import org.scalatest._
import org.scalatest.matchers._

import scalikejdbc.SQLInterpolation._
import scalikejdbc._

case class Group(id: Int, groupName: String)
case class Member(id: Int, firstName: Option[String], groupId: Option[Int], group: Option[Group] /*, dummy: Int = 123*/ )
object Member extends SQLSyntaxSupport[Member] {
  override val tableName = "applymethodgenerationspec"
}

class ApplyMethodGenerationSpec extends FlatSpec with ShouldMatchers with LogSupport {

  behavior of "ApplyMethodGeneration"

  import scalikejdbc.SQLInterpolation._

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
    sql"create table ${tableName} (id int not null, first_name varchar(256), group_id int)".execute.apply()
  } catch {
    case e: Exception =>
      log.info(s"member table is already created (${e.getMessage})")
  }
  try {
    sql"delete from ${tableName}".execute.apply()
    Seq((1, Some("foo"), None), (2, Some("bar"), None), (3, Some("baz"), Some(1))) foreach {
      case (id, name, groupId) => sql"insert into ${tableName} values (${id}, ${name}, ${groupId})".update.apply()
    }
  } catch {
    case e: Exception =>
      log.info(s"member records already exists (${e.getMessage})")
  }

  it should "be available" in {
    val m = Member.syntax("m")
    val members = sql"select ${m.result.*} from ${Member.as(m)}".map(Member(m.resultName)).list.apply()
    members.size should equal(3)
    //members.head.dummy should equal(123)
  }

}
