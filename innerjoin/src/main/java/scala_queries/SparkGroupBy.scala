package scala_queries

import contracts.DBManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sqlUtils.{AggregateFunction, ParseSQL}

import scala.annotation.switch
import scala.collection.JavaConverters._

object SparkGroupBy {

	def execute(parseSQL: ParseSQL): Unit = {
		
		// convert columns to required _c# format, where # denotes a number
		for (i <- 0 until parseSQL.getOperationColumns.size()) {
			parseSQL.getOperationColumns.set(i,
				"_c" + DBManager.getColumnIndex(parseSQL.getTable1,
					parseSQL.getOperationColumns.get(i)))
		}
		
		// extracting name of column on which aggregate function is applied
		var aggColumn = parseSQL.getColumns.get(parseSQL.getColumns.size() - 1)
		aggColumn = "_c" + DBManager.getColumnIndex(parseSQL.getTable1,
			aggColumn.split("\\(")(1).split("\\)")(0))
		
		// creating a spark session
		val sc = SparkSession.builder()
			.master("local[*]") // necessary for allowing spark to use as many laogical datanodes as available
			.getOrCreate()
		
		// converting ArrayList of operationColumns to scalaBuffer
		val scalaBuffer = asScalaBuffer(parseSQL.getOperationColumns)
		
		// creating dataframe (time evaluation should start here)
		val table_df = sc.read.format("csv").option("header", "false")
			.load("hdfs://localhost:9000/" + DBManager.getFileName(parseSQL.getTable1))
		
		var res = table_df // this variable will eventually store the results
		
		// perform required operation based on aggregate function (switch-case)
		// TODO: Fix switch warning (syntax correction possibly needed)
		(parseSQL.getAggregateFunction: @switch) match {
			case AggregateFunction.SUM =>
        res = table_df.groupBy(scalaBuffer.head, scalaBuffer.tail.asInstanceOf[Seq[String]]: _*)
					.agg(sum(aggColumn).as("sum"))
					.filter("sum>" + parseSQL.getComparisonNumber);
			
			case AggregateFunction.COUNT =>
        res = table_df.groupBy(scalaBuffer.head, scalaBuffer.tail.asInstanceOf[Seq[String]]: _*)
					.agg(count(aggColumn).as("count"))
					.filter("count>" + parseSQL.getComparisonNumber);
			
			case AggregateFunction.MAX =>
        res = table_df.groupBy(scalaBuffer.head, scalaBuffer.tail.asInstanceOf[Seq[String]]: _*)
					.agg(max(aggColumn).as("max"))
					.filter("max>" + parseSQL.getComparisonNumber);
			
			case AggregateFunction.MIN =>
        res = table_df.groupBy(scalaBuffer.head, scalaBuffer.tail.asInstanceOf[Seq[String]]: _*)
					.agg(min(aggColumn).as("min"))
					.filter("min>" + parseSQL.getComparisonNumber);
			
			case AggregateFunction.NONE => throw new IllegalArgumentException("The aggregate function is not valid");
		}
		// time evaluation should end here
		
		// display the results
		res.show
	}
}