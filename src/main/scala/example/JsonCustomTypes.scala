package example

import org.squeryl._
import org.squeryl.dsl._
import java.sql.Timestamp
import java.sql.ResultSet
import org.squeryl.adapters.H2Adapter
import java.util.Date
import scala.collection.mutable.HashMap
import org.squeryl.annotations.Column
import org.squeryl.dsl.ast.FunctionNode
import org.squeryl.dsl.ast.LogicalBoolean

object XsnTypeMode extends PrimitiveTypeMode {
  import com.codahale.jerkson.Json._
  import scala.language.implicitConversions

  implicit val jsonTEF = new NonPrimitiveJdbcMapper[String, HashMap[String, Any], TString](stringTEF, this) {
    def convertFromJdbc(v: String) = if (v == null) null else parse[HashMap[String, Any]](v)
    def convertToJdbc(v: HashMap[String, Any]) = if (v == null) null else generate(v)
    override def sample = new HashMap[String, Any]()
  }

  implicit def mapToTE(s: HashMap[String, Any]) = jsonTEF.create(s)

  // Support for Option[Map]
  implicit val optionJsonTEF =
    new TypedExpressionFactory[Option[HashMap[String, Any]], TOptionString] with DeOptionizer[String, HashMap[String, Any], TString, Option[HashMap[String, Any]], TOptionString] {

      val deOptionizer = jsonTEF
    }

  implicit def optionMapToTE(s: Option[HashMap[String, Any]]) = optionJsonTEF.create(s)
  
  def custom[A1,T1](s: TypedExpression[A1,T1]) =
    new FunctionNode("custom", Seq(s)) with LogicalBoolean

}


case class JsonTester(val extra: HashMap[String, Any] = null) {
}

case class OptionJsonTester(val extra: Option[HashMap[String, Any]] = None) {
  def this() = this(Some(new HashMap[String, Any]())) // Squeryl trap
}

import XsnTypeMode._

// Since Option fails at the schema load time, we define two schemes to test it.
object JsonTesterSchema extends Schema {
  val timestampTester = table[JsonTester]
}

object OptionJsonTesterSchema extends Schema {
  val optionTimestampTester = table[OptionJsonTester]
}
class Article( @Column("id") val id: Int, @Column("title") val title: String)
object ArticlesSchema extends Schema {
  val articles = table[Article]
}

object JsonTests {

  def main(args: Array[String]): Unit = {
    println("Starting a JSON test")
    Class.forName("org.h2.Driver")
    SessionFactory.concreteFactory = Some(() => {
      val session = Session.create(
        java.sql.DriverManager.getConnection("jdbc:h2:mem:test", "sa", ""),
        new H2Adapter)
      session.setLogger((sql: String) => println(s"SQL: $sql"))
      session
        })

//    test // works
//    testOption // fails
//    testRs
      testQ
  }

  def testMap = new HashMap[String, Any] { put("key", "value") }

  def test = transaction {
    try { JsonTesterSchema.drop } catch { case e: Exception => {} }
    JsonTesterSchema.create

    JsonTesterSchema.timestampTester.insert(new JsonTester(testMap))

    val value = inTransaction {
      from(JsonTesterSchema.timestampTester)(s => select(s)).single
    }

    println(value)
  }

  def testOption = transaction {
    try { OptionJsonTesterSchema.drop } catch { case e: Exception => {} }
    OptionJsonTesterSchema.create

    OptionJsonTesterSchema.optionTimestampTester.insert(new OptionJsonTester(Some(testMap)))

    val value = inTransaction { from(OptionJsonTesterSchema.optionTimestampTester)(s => select(s)).single }

    println(value)
  }

  def testRs = {
    println(JsonTesterSchema.timestampTester.posoMetaData)
    val holder = java.sql.DriverManager.getConnection("jdbc:h2:mem:test", "sa", "")
    transaction {
      try { JsonTesterSchema.drop } catch { case e: Exception => {} }
      JsonTesterSchema.create
      JsonTesterSchema.timestampTester.insert(new JsonTester(testMap))
    }
    inTransaction {
      val q = from(ArticlesSchema.articles)(s => where(custom(upper(s.title))) select(s))
      println(q.statement)
    }
    val c = java.sql.DriverManager.getConnection("jdbc:h2:mem:test", "sa", "")
    val rs = c.prepareStatement(s"select * from ${JsonTesterSchema.timestampTester.name}").executeQuery()
    rs.next()
    val result = new JsonTester
    JsonTesterSchema.timestampTester.posoMetaData.findFieldMetaDataForProperty("extra").map(md => md.setFromResultSet(result, rs, 1))
    println(result)
//    while (rs.next()) {
//      println(rs.getString(1))
//    }
    c.close()
    holder.close
  }
  
  def testQ = {
    val holder = java.sql.DriverManager.getConnection("jdbc:h2:mem:test", "sa", "")
    transaction {
      ArticlesSchema.create
      val products: List[Option[String]] = List(Some("content"), Some("2"), Some("3"))
      val (n1 :: n2 :: n3 :: rest) = products
      val ls = from(ArticlesSchema.articles)(a => where(a.title like n1.?) select(a.id))
      println("Testing the Query.statement method")
      println(ls.statement)
      println
      println("Testing the real query")
      ls.toList
    }
  }

}
