package couchdb

import org.specs._
import scala.actors._
import java.io.InputStream

class CouchDBSpec extends Specification {

  "couch db driver client" should {
    val dbName = "driver_test_db"
    val db = CouchDB(dbName)
    def failed = (e:CouchDBError) => { fail("failed with "+e) }

    "report server info" in {
      db.serverInfo() fold (
        failed
        ,(x:Map[String,Any]) => {
            x.get("version") must beSome
            x.get("couchdb") must beSome
          }
      )
    }

    "report if database exists" in {
      db.exists() fold (
        failed
        ,x => x must beFalse
      )
    }

    "create and delete database" in {
      db.createDb() fold (
        failed
        , _ => {true must beTrue}
      )
      db.createDbIfNotExists() fold (
        failed
        , _ => {true must beTrue}
      )
      db.deleteDb() fold (
        failed
        , _ => {true must beTrue}
      )
      db.createDbIfNotExists() fold (
        failed
        , _ => {true must beTrue}
      )
      db.deleteDb() fold (
        failed
        , _ => {true must beTrue}
      )
    }


  }

}


