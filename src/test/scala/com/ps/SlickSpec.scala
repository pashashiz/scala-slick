package com.ps

import org.scalatest._
import slick.jdbc.H2Profile.api._
import com.ps.Entities._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class SlickSpec extends FlatSpec {

  def withDB(testCode: Database => Any): Unit = {
    val db = Database.forConfig("h2mem1")
    try {
      val setup = DBIO.seq(
        // Create the tables, including primary and foreign keys
        (suppliers.schema ++ coffees.schema).create,

        // Insert some suppliers
        suppliers += (101, "Acme, Inc.",      "99 Market Street", "Groundsville", "CA", "95199"),
        suppliers += ( 49, "Superior Coffee", "1 Party Place",    "Mendocino",    "CA", "95460"),
        suppliers += (150, "The High Ground", "100 Coffee Lane",  "Meadows",      "CA", "93966"),
        // Equivalent SQL code:
        // insert into SUPPLIERS(SUP_ID, SUP_NAME, STREET, CITY, STATE, ZIP) values (?,?,?,?,?,?)

        // Insert some coffees (using JDBC's batch insert feature, if supported by the DB)
        coffees ++= Seq(
          ("Colombian",         101, 7.99, 0, 0),
          ("French_Roast",       49, 8.99, 0, 0),
          ("Espresso",          150, 9.99, 0, 0),
          ("Colombian_Decaf",   101, 8.99, 0, 0),
          ("French_Roast_Decaf", 49, 9.99, 0, 0)
        )
        // Equivalent SQL code:
        // insert into COFFEES(COF_NAME, SUP_ID, PRICE, SALES, TOTAL) values (?,?,?,?,?)
      )
      val setupFuture = db.run(setup)
      Await.ready(setupFuture, Duration.Inf)
      testCode(db)
    } finally db.close
  }

  "slick" should "execute * select" in withDB { db =>
    // select COF_NAME, SUP_ID, PRICE, SALES, TOTAL from COFFEES
    println("Coffees:")
    val future = db.run(coffees.result).map(sec => sec.foreach {
      case (name, supID, price, sales, total) =>
        println("  " + name + "\t" + supID + "\t" + price + "\t" + sales + "\t" + total)
    })
    Await.ready(future, Duration.Inf)
  }

  it should "execute select with projection" in withDB { db =>
    // select '  ' || COF_NAME || '\t' || SUP_ID || '\t' || PRICE || '\t' SALES || '\t' TOTAL from COFFEES
    val query = for (c <- coffees)
      yield LiteralColumn("  ") ++ c.name ++ "\t" ++ c.supID.asColumnOf[String] ++
        "\t" ++ c.price.asColumnOf[String] ++ "\t" ++ c.sales.asColumnOf[String] ++
        "\t" ++ c.total.asColumnOf[String]
    db.stream(query.result)
    var future = db.run(query.result).map(sec => sec.foreach {
      case (projection) =>
        println(projection)
    })
    Await.ready(future, Duration.Inf)
  }

  it should "execute select with filter and monadic join" in withDB { db =>
    // select c.COF_NAME, s.SUP_NAME from COFFEES c, SUPPLIERS s where c.PRICE < 9.0 and s.SUP_ID = c.SUP_ID
    val query = for {
      c <- coffees if c.price < 9.0
      s <- suppliers if s.id === c.supID // or with foreign key s <- c.supplier
    } yield (c.name, s.name)
    var future = db.run(query.result).map(sec => sec.foreach {
      case (coffee, supplier) =>
        println(s"$coffee supplied by $supplier")
    })
    Await.ready(future, Duration.Inf)
  }

  it should "execute select with filter and applicative join" in withDB { db =>
    // select c.COF_NAME, s.SUP_NAME from COFFEES c, SUPPLIERS s where c.PRICE < 9.0 and s.SUP_ID = c.SUP_ID
    val query = for {
      (c, s) <- coffees join suppliers on (_.supID === _.id)
    } yield (c.name, s.name)
    var future = db.run(query.result).map(sec => sec.foreach {
      case (coffee, supplier) =>
        println(s"$coffee supplied by $supplier")
    })
    Await.ready(future, Duration.Inf)
  }

  it should "aggregate values" in withDB {db =>
    // select min(c."PRICE") from "COFFEES" c
    val query = coffees.map(_.price).min
    var future = db.run(query.result)
    future.onComplete({
      case Success(min) => println(min)
      case Failure(e) => println(e)
    })
    Await.ready(future, Duration.Inf)
  }

  it should "update values" in withDB { db =>
    // update "COFFEES" set "PRICE" = ? where "COFFEES"."COF_NAME" = 'Espresso'
    val query = for { c <- coffees if c.name === "Espresso" } yield c.price
    var future = db.run(query.update(10.49))
    future.onComplete({
      case Success(count) => println(count)
      case Failure(e) => println(e)
    })
    Await.ready(future, Duration.Inf)
  }

  it should "delete values" in withDB { db =>
    // delete from "COFFEES" c where c."COF_NAME" = 'Espresso'
    val query = coffees.filter(_.name === "Espresso")
    var future = db.run(query.delete)
    future.onComplete({
      case Success(count) => println(count)
      case Failure(e) => println(e)
    })
    Await.ready(future, Duration.Inf)
  }

}
