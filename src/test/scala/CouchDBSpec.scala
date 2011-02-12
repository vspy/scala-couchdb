package couchdb

import org.specs._
import scala.actors._
import java.io.InputStream

class CouchDBSpec extends Specification {

  "couch db driver client" should {
    val dbName = "driver_test_db"
    val db = CouchDB(dbName)

    "report server info" in {
      db.serverInfo() fold (
        e => { fail("failed with "+e) }
        ,(x:Map[String,Any]) => {
            x.get("version") must beSome
            x.get("couchdb") must beSome
          }
      )
    }

    "report if database exists" in {
      db.exists() fold (
        e => { fail("failed with "+e) }
        ,x => x must beFalse
      )
    }


  }

}


