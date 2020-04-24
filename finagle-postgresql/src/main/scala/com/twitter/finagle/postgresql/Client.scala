package com.twitter.finagle.postgresql

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.postgresql.Response.Command
import com.twitter.finagle.postgresql.Response.Empty
import com.twitter.finagle.postgresql.Response.QueryResponse
import com.twitter.finagle.postgresql.Response.ResultSet
import com.twitter.finagle.postgresql.Response.Row
import com.twitter.io.Buf
import com.twitter.io.Reader
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time

trait QueryClient[Q] {

  def query(sql: Q): Future[QueryResponse]

  def read(sql: Q): Future[ResultSet] =
    query(sql).flatMap {
      case rs: ResultSet => Future.value(rs)
      case Empty => Future.value(ResultSet.empty)
      case other => Future.exception(new IllegalStateException(s"invalid response to select: $other"))
    }

  def select[T](sql: Q)(f: Row => T): Future[Seq[T]] =
    read(sql)
      .flatMap { _.toSeq }
      .map { seq => seq.map(f)}

  def modify(sql: Q): Future[Command] =
    query(sql).flatMap {
      case c: Command => Future.value(c)
      case other => Future.exception(new IllegalStateException(s"invalid response to command: $other"))
    }
}

trait Client extends QueryClient[String] with Closable {

  def multiQuery(sql: String): Future[Reader[QueryResponse]]

  /**
   * Assumes a single-line query. If the backend responds with more than one, an error is raised.
   */
  def query(sql: String): Future[QueryResponse] =
    multiQuery(sql)
      .flatMap { reader =>
        reader
          .read()
          .flatMap {
            case Some(rs: ResultSet) => rs.buffered
            case Some(r) => Future.value(r)
            case None => Future.exception(new IllegalStateException("no query response"))
          }
          .flatMap { first =>
            reader
              .read()
              .flatMap {
                case None => Future.value(first)
                case Some(_) =>
                  reader.discard()
                  Future.exception(new IllegalStateException("expected a single response, got multiple"))
              }
          }
      }

  def prepare(sql: String): PreparedStatement

  def cursor(sql: String): CursoredStatement

}

object Client {
  def apply(factory: ServiceFactory[Request, Response]): Client = new Client {
    val service = factory.toService

    override def multiQuery(sql: String): Future[Reader[QueryResponse]] =
      service(Request.Query(sql))
        .flatMap {
          case Response.SimpleQueryResponse(responses) => Future.value(responses)
          case r => Future.exception(new IllegalStateException(s"invalid response to simple query: $r"))
        }

    override def prepare(sql: String): PreparedStatement = new PreparedStatement {
      // NOTE: this assumes that caching is done down the stack so that the statement isn't re-prepared on the same connection
      //   The rationale is that it allows releasing the connection earlier at the expense
      //   of re-preparing statements on each connection and potentially more than once (but not every time)
      override def query(parameters: Seq[Parameter]): Future[QueryResponse] =
        factory()
          .flatMap { svc =>
            svc(Request.Prepare(sql))
              .map {
                case Response.ParseComplete(prep) => prep
                case _ => sys.error("") // TODO
              }
              .flatMap { prepared =>
                svc(Request.ExecutePortal(prepared, parameters.map(_.buf)))
              }
              .map {
                case r: Response.QueryResponse => r
                case _ => sys.error("") // TODO
              }
          }
    }

    override def cursor(sql: String): CursoredStatement = ???

    override def close(deadline: Time): Future[Unit] = factory.close(deadline)
  }
}

// TODO
trait Parameter {
  def buf: Buf
}

trait PreparedStatement extends QueryClient[Seq[Parameter]]

trait CursoredStatement
