package cc.projects.mrutils.scala_queries

import cc.projects.mrutils.contracts.DBManager
import org.apache.spark.sql.SparkSession
import cc.projects.mrutils.sqlUtils.ParseSQL
import java.time


object SparkInnerJoin {

  def execute(parseSQL: ParseSQL): Unit = {


    val sc = SparkSession.builder()
      .master("local[*]") // necessary for allowing spark to use as many laogical datanodes as available
      .getOrCreate()
    //    val user_df = sc.read.format("csv").option("header", "false").load("hdfs://localhost:9000/users.csv")
    //    val zipcodes_df = sc.read.format("csv").option("header", "false").load("hdfs://localhost:9000/zipcodes.csv")
    val jk = parseSQL.getOperationColumns.get(0)
    val tab1ColIndex = DBManager.getColumnIndex(parseSQL.getTable1, jk)
    val tab2ColIndex = DBManager.getColumnIndex(parseSQL.getTable2, jk)

    val table1 = sc.read.format("csv").option("header", "false")
      .load("hdfs://localhost:9000/" + DBManager.getFileName(parseSQL.getTable1))

    val table2 = sc.read.format("csv").option("header", "false")
      .load("hdfs://localhost:9000/" + DBManager.getFileName(parseSQL.getTable2))

    val ij = table1.join(table2, table1("_c" + tab1ColIndex) === table2("_c" + tab2ColIndex)).drop(table1("_c" + tab1ColIndex))
    ij.show
    val newNames = Seq("1","2","3","4","5","6","7","8")
    val dfRenamed = ij.toDF(newNames: _*)
    dfRenamed.write.format("csv").save("hdfs://localhost:9000/spark")
  }
}