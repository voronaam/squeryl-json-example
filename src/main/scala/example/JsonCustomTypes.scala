package example

import org.squeryl._
import org.squeryl.dsl._
import java.sql.Timestamp
import java.sql.ResultSet
import org.squeryl.adapters.H2Adapter
import java.util.Date
import scala.collection.mutable.HashMap

object XsnTypeMode extends PrimitiveTypeMode {
  import com.codahale.jerkson.Json._
  import scala.language.implicitConversions
  
  implicit val jsonTEF = new NonPrimitiveJdbcMapper[String, HashMap[String, Any], TString](stringTEF, this) {
    def convertFromJdbc(v: String) = if(v == null) null else parse[HashMap[String, Any]](v)
    def convertToJdbc(v: HashMap[String, Any]) = if(v == null) null else generate(v)
    override def sample = new HashMap[String, Any]()
  }
  
  implicit def mapToTE(s: HashMap[String, Any]) = jsonTEF.create(s)  
  
  // Support for Option[Map]
  implicit val optionJsonTEF =
    new TypedExpressionFactory[Option[HashMap[String, Any]], TOptionString]
      with DeOptionizer[String, HashMap[String, Any], TString, Option[HashMap[String, Any]], TOptionString] {

    val deOptionizer = jsonTEF
  }
  
  implicit def optionMapToTE(s: Option[HashMap[String, Any]]) = optionJsonTEF.create(s)
}


case class JsonTester(val extra: HashMap[String, Any] = null) {
}

case class OptionJsonTester(val extra: Option[HashMap[String, Any]] = None) {
}

import XsnTypeMode._

// Since Option fails at the schema load time, we define two schemes to test it.
object JsonTesterSchema extends Schema {
  val timestampTester = table[JsonTester]
}

object OptionJsonTesterSchema extends Schema {
  val optionTimestampTester = table[OptionJsonTester]
}


object JsonTests {
  
  def main(args: Array[String]): Unit = {
    println("Starting a JSON test")
    Class.forName("org.h2.Driver")
    SessionFactory.concreteFactory = Some(() =>
      Session.create(
        java.sql.DriverManager.getConnection("jdbc:h2:mem:test", "sa", ""),
        new H2Adapter))
        
    test // works
    testOption // fails
  }

  def testMap = new HashMap[String, Any]{put("key","value")}

  def test = transaction {
    try { JsonTesterSchema.drop } catch { case e: Exception => {} }
    JsonTesterSchema.create

    JsonTesterSchema.timestampTester.insert(new JsonTester(testMap))
    
    val value = inTransaction{ from(JsonTesterSchema.timestampTester)( s => select(s)).single }

    println(value)
  }
  
  def testOption = transaction {
    try { OptionJsonTesterSchema.drop } catch { case e: Exception => {} }
    OptionJsonTesterSchema.create

    OptionJsonTesterSchema.optionTimestampTester.insert(new OptionJsonTester(Some(testMap)))
    
    val value = inTransaction{ from(OptionJsonTesterSchema.optionTimestampTester)( s => select(s)).single }

    println(value)
  }

}
