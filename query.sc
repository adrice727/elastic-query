/**
  * Ammonite imports
  */
import $ivy.`io.circe::circe-core:0.8.0`
import $ivy.`io.circe::circe-generic:0.8.0`
import $ivy.`io.circe::circe-parser:0.8.0`
import $ivy.`com.sksamuel.elastic4s::elastic4s-tcp:5.4.6`
import $ivy.`com.sksamuel.elastic4s::elastic4s-core:5.4.6`
import $ivy.`com.sksamuel.elastic4s::elastic4s-http:5.4.6`
import $ivy.`com.sksamuel.elastic4s::elastic4s-circe:5.4.6`

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import com.sksamuel.elastic4s.searches.SearchDefinition
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import com.sksamuel.elastic4s.searches.queries.term.TermQueryDefinition
import com.sksamuel.elastic4s.searches.sort.FieldSortDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.circe._
import io.circe.generic.auto._

object ES {

  private val ES_SIZE = 10000
  private val SCROLL_SIZE = s"${ES_SIZE / 1000}m"
  private val client = HttpClient(ElasticsearchClientUri("YOUR_ELASTIC_SEARCH_URI", 9200))

  type ScrollId = Option[String]
  type SearchResult = (ScrollId, Array[SearchHit])


  def queryIndex(queryTerms: SearchDefinition): Future[Array[SearchHit]] = {

    def scrollIndex(scrollId: ScrollId = None): Future[SearchResult] = {

      def wrapSuccess(response: SearchResponse): SearchResult = (response.scrollId, response.hits.hits)

      val wrapFailure: PartialFunction[Throwable, SearchResult] = {
        case e =>
          println("Error querying index", e.toString)
          (None, Array())
      }

      val query: Future[SearchResponse] = {
        if (scrollId.isDefined) {
          client.execute(searchScroll(scrollId.get))
        } else {
          client.execute {
            queryTerms.size(ES_SIZE).scroll(SCROLL_SIZE)
          }
        }
      }

      query.map(wrapSuccess).recover(wrapFailure)
    }


    /**
     * Recursively build up search results
     */
    def buildResults(id: ScrollId = None): Future[SearchResult] = {
      val emptyResult: Future[SearchResult] = Future.successful((None, Array[SearchHit]()))
      for {
        r1 <- scrollIndex(id)
        r2 <- if (r1._1.isDefined) buildResults(r1._1) else emptyResult
      } yield (None, r1._2 ++ r2._2) // We can discard the ScrollId here
    }

    buildResults().map(_._2)

  }

  def searchIndex(query: SearchDefinition): Array[SearchHit] = {
    queryIndex(query).await
  }
}

object Parser {

  /**
   * Any code you need to parse your results or perform data transformations
   * can go here.
   */
  def parse(results: Array[SearchHit]): Unit = {
    results.foreach(println)
  }
}

/**
  * Build your query here and pass it to ES.parse().
  * Elastic4S docs: https://sksamuel.github.io/elastic4s/docs/
  * ElasticSearch docs: https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/index.html
  */
val query =
  search("myIndex")
    .query(existsQuery("someRequiredField"))
    .sortBy(FieldSortDefinition("timestamp"))
    .sourceInclude("whatever", "fields", "you", "want")

Parser.parse(ES.searchIndex(query))