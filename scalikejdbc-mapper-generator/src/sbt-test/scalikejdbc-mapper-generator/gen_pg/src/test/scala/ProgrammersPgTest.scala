package app.models

import java.sql.SQLXML

import org.joda.time.DateTime
import org.scalatest._
import scala.util.Random
import scalikejdbc.scalatest.AutoRollback
import scalikejdbc._

class ProgrammersPgTest extends fixture.FlatSpec with Matchers with AutoRollback {
  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(
    enabled = true,
    singleLineMode = false,
    printUnprocessedStackTrace = false,
    stackTraceDepth= 15,
    logLevel = 'debug,
    warningEnabled = false,
    warningThresholdMillis = 3000L,
    warningLogLevel = 'warn
  )

  private[this] val p = ProgrammersPg.pp

  behavior of "Programmer"

  it should "be available" in { implicit session =>
    ProgrammersPg.findAll().toSet should equal(Set.empty[ProgrammersPg])
    ProgrammersPg.countAll() should equal(0)

    val programmers = List(Some("aaa"), Some("bbb"), None).map(name =>
      ProgrammersPg.create(name = name, t1 = new DateTime(2014, 12, 31, 20, 0))
    ).toSet
    programmers.foreach{ programmer =>
      ProgrammersPg.find(programmer.id) should equal(Some(programmer))
    }
    val invalidId = Int.MinValue
    ProgrammersPg.find(invalidId) should equal(None)
    ProgrammersPg.findAll().toSet should equal(programmers)
    ProgrammersPg.countAll() should equal(programmers.size)
    ProgrammersPg.findAllBy(sqls"${p.name} is not null").toSet should equal(programmers.filter(_.name.isDefined))
    ProgrammersPg.countBy(sqls"${p.name} is null") should equal(programmers.count(_.name.isEmpty))

    val destroyProgrammer = Random.shuffle(programmers.toList).head

    ProgrammersPg.destroy(destroyProgrammer)
    ProgrammersPg.findAll().toSet should equal(programmers.filter(_ != destroyProgrammer))
    ProgrammersPg.countAll() should equal(programmers.size - 1)
    ProgrammersPg.find(destroyProgrammer.id) should equal(None)

    programmers.foreach(ProgrammersPg.destroy(_))
    ProgrammersPg.findAll().toSet should equal(Set.empty[ProgrammersPg])
    ProgrammersPg.countAll() should equal(0)
  }

  it should "xml type" in { implicit session =>
    val xmls = List(
      Some("<no>12345</no>"),
      None
    ).map(_.flatMap { x =>
      val xml: SQLXML = session.connection.createSQLXML
      xml.setString(x)
      Some(xml)
    })
    val programmers = xmls.map { t =>
      ProgrammersPg.create(name = Some("aaa"), t1 = new DateTime(2014, 12, 31, 20, 0), xcol = t)
    }
    programmers.foreach { p1 =>
      val op2 = ProgrammersPg.find(p1.id)
      val x1 = p1.xcol.map(_.getString)
      val x2 = op2.flatMap(p2 =>p2.xcol.map(_.getString))
      x1 should equal(x2)
    }
  }
  }
