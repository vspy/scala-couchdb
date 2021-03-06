package couchdb

import http._
import java.io._
import scala.actors._
import com.twitter.json._

class CouchDBError(message:String,reason:Option[Throwable])

case class CouchDBFormatError(message:String,reason:Option[Throwable] = None)
  extends CouchDBError(message:String, reason)

case class CouchDBConnectionError(message:String,reason:Option[Throwable])
  extends CouchDBError(message:String, reason)


case class CouchDB(db:String, serverUrl:String = "http://localhost:5984/") {
  type R[T] = Either[CouchDBError,T]
  type F[T] = Future[R[T]]
  type JsonObj = Map[String,Any]
  type IdRev = Tuple2[String,String]

  /**
   * returns server information map {"couchdb":"Welcome", "version":...}
   */
  def serverInfo:F[JsonObj] =
    get(serverUrl, 
      (s:Int, h:Map[String,String], is:InputStream) =>
       statusMustBeOK(s, jsonObj(is))
    )

  /**
   * returns true if database exists
   */
  def exists:F[Boolean] = 
    get(url,
      (s:Int, h:Map[String,String], is:InputStream) =>
        s match {
          case 200 => Right(true)
          case 404 => Right(false)
          case x => Left(CouchDBFormatError("Expected status to be either 200(OK) or 404(Not found), but got "+x))
        }
    )

  /**
   * creates database
   */ 
  def createDb:F[Unit] = 
    put(url,
      (s:Int, h:Map[String,String], is:InputStream) =>
        s match {
          case 201 => Right({})
          case x => Left(CouchDBFormatError("Expected status to be 201(Created), but got "+x))
        }
    )

  /**
   * creates database if not exists
   */
  def createDbIfNotExists:F[Unit] = 
    put(url,
      (s:Int, h:Map[String,String], is:InputStream) =>
        s match {
          case 201 => Right({})
          case 412 => Right({})
          case x => Left(CouchDBFormatError("Expected status to be either 201(Created) or 412(Precondition failed), but got "+x))
        }
    )
  /**
   * delete database
   */
  def deleteDb:F[Unit] = 
    delete(url,
      (s:Int, h:Map[String,String], is:InputStream) =>
        s match {
          case 200 => Right({})
          case x => Left(CouchDBFormatError("Expected status to be 200(OK), but got "+x))
        }
    )
    

  /**
   * creates doc
   */
/*  def create(doc:JsonObj,id:Option[String] = None):F[ =
    id match {
      case None => 
        post(url,idRevResponse)
      case Some(id) =>
        put(docUrl(id),idRevResponse)
    }

  private def idRevResponse =
    (s:Int, h:Map[String,String], is:InputStream) => {
      val json = createdJson(s,h,is)
      (json.get("id"), json.get("rev")) match {
        case (Some(id:String),Some(rev:String)) => Right((id,rev))
        case _ => Left(CouchDBFormatError("Expected both id and rev in the response, but got: "+json))
      }
    }*/

  // urls
  val url = serverUrl+"/"+db+"/"

  private def docUrl(id:String) = url + id // TODO: urlencode

  // mapped future
  private case class MappedFuture[A,B](f:Future[A], fn : A=>B) extends Future[B] {
    def isSet = f.isSet
    def apply = fn(f.apply)
    def respond(k: B=>Unit) = 
      f.respond(
        (a: A)=> { k(fn(a)) }
      )
    def inputChannel = null //TODO: who needs it at all?
  }

  // Http helpers
  private def createdJson: (Int,Map[String,String], InputStream)=>R[JsonObj] =
    (s:Int, h:Map[String,String], is:InputStream) =>
      statusMustBeCreated(s, checkContentType(h, jsonObj(is) ))

  private def okJson: (Int,Map[String,String], InputStream)=>R[JsonObj] =
    (s:Int, h:Map[String,String], is:InputStream) =>
      statusMustBeOK(s, checkContentType(h, jsonObj(is) ))

  private def jsonObj(is:InputStream):R[JsonObj] =
    json(is).fold( leftIdentity, {
      case o:JsonObj => Right(o)
      case x => Left(CouchDBFormatError("Expected json obj, but got "+x))
    })

  private def json(is:InputStream):R[Any] =
    try {
      val str = stream2string(is)
      Right(Json.parse(str))
    } catch {
      case e:IOException => Left(CouchDBConnectionError("IO error when reading response", Some(e)))
      case e:JsonException => Left(CouchDBFormatError("Error reading json", Some(e)))
    } 
 
  private val defaultHeaders =
    Seq(
      "User-Agent"->"Scala CouchDB driver"
      ,"Content-Type"->"application/json"
    )

  private def get[A](
    _url:String
    ,fn:(Int, Map[String, String], InputStream)=>R[A]
  ):F[A] = MappedFuture(
            Http(
              method = "GET"
              ,url = _url
              ,onComplete = fn
              ,headers = defaultHeaders
            )
            , ( x:Either[HttpError,R[A]] ) => 
                x fold ( httpError, identity ) 
          )

  private def put[A](
    _url:String
    ,fn:(Int, Map[String, String], InputStream)=>R[A]
  ):F[A] = MappedFuture(
            Http(
              method = "PUT"
              ,url = _url
              ,onComplete = fn
              ,headers = defaultHeaders
            )
            , ( x:Either[HttpError,R[A]] ) => 
                x fold ( httpError, identity ) 
          )

  private def delete[A](
    _url:String
    ,fn:(Int, Map[String, String], InputStream)=>R[A]
  ):F[A] = MappedFuture(
            Http(
              method = "DELETE"
              ,url = _url
              ,onComplete = fn
              ,headers = defaultHeaders
            )
            , ( x:Either[HttpError,R[A]] ) => 
                x fold ( httpError, identity ) 
          )

  private def statusMustBeOK[B](a:Int, fn: =>R[B]):R[B] =
    statusMustBe(a,200,fn)

  private def statusMustBeCreated[B](a:Int, fn: =>R[B]):R[B] =
    statusMustBe(a,201,fn)

  private def statusMustBe[B](a:Int, b:Int, fn: =>R[B]):R[B] =
    mustBe("status",a,b,fn)

  private def checkContentType[B](headers:Map[String,String], fn: =>R[B])= 
    mustBe(
      "response content type"
      ,headers.get("Content-Type")
      ,"application/json"
      ,fn
    )

  private def mustBe[A,B](name:String, a:A, b:A, fn: =>R[B]):R[B] = 
    if (a==b) fn
    else Left(CouchDBFormatError("Expected "+name+" to be "+b+", but got "+a))

  private def identity[T] = (x:T)=>x

  private def leftIdentity[X,Y] = (x:X)=>Left[X,Y](x)

  private def httpError[A]:(HttpError)=>R[A] =
    (e:HttpError) => Left(CouchDBConnectionError(e.message, e.reason))

  private def stream2string(is:InputStream):String = {
    val buffer = new Array[Char](16384)
    val out = new StringBuilder
    val in = new InputStreamReader(is, "UTF-8")
    var n = -1
    while (-1 != {n = in.read(buffer);n}) out.appendAll(buffer,0,n)
    out.toString
  }

}
