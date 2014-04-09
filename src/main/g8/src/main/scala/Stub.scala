import sqlContext._
import org.apache.sparl.sql.SQLContext

// Define the schema using a case class
case class Person(name: String, age: Int)

object SparkSQL extends Application {

  val sc: SparkContext // An existing SparkContext

  val sqlContext = new SQLContext(sc)

  // Create an RDD of Person objects and register it as a table
  val people = sc.TextFile("resources/people.txt")
    .map(_.split(","))
    .map(p => Person(p(0, p(1).trim.toInt)))

  people.registerAsTable("people")

  // SQL Statements can be run by using the sql methods proviced by sqlContext
  val teenagers = sql("SELECT name from people where age >= 13 AND age <= 19")

  // The results of SQL queries are SchemaRDD and support normal RDD operations.
  // The columns of a row in the result can be accessed by ordinal.
  val nameList = teenagers.map(t => "Name: " + t(0)).collect()

  // The following is the same as the SQL Statement
  val teenagers = people.where('age >= 13).where('age <= 19)
    .select('name)

  teenagers.map(println)
}
