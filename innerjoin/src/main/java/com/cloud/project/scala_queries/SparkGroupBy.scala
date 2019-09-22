package com.cloud.project.scala_queries

import com.cloud.project.contracts.DBManager
import com.cloud.project.models.OutputModel
import com.cloud.project.sqlUtils.{AggregateFunction, ParseSQL}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object SparkGroupBy {
	
	def execute(parseSQL: ParseSQL, groupByOutput: OutputModel): Unit = {
		
		// convert columns to required _c# format, where # denotes a number
		/*for (i <- 0 until parseSQL.getOperationColumns.size()) {
			parseSQL.getOperationColumns.set(i,
				"_c" + DBManager.getColumnIndex(parseSQL.getTable1,
					parseSQL.getOperationColumns.get(i)))
		}*/
		
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
		var table_df = sc.read.format("csv").option("header", "false")
			.load("hdfs://localhost:9000/" + DBManager.getFileName(parseSQL.getTable1))
		
		for (a <- 0 until DBManager.getTableSize(parseSQL.getTable1) - 1) {
			table_df = table_df.withColumnRenamed("_c" + a,
				DBManager.getColumnFromIndex(parseSQL.getTable1, a));
		}
		
		var res = table_df // this variable will eventually store the results
		
		// perform required operation based on aggregate function (switch-case)
		parseSQL.getAggregateFunction match {
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
		
		// display the results
		groupByOutput.setSparkExecutionTime(sc.time(res.show) + "")
    //groupByOutput.setSparkOutput(res.write.format("csv").toString)
	}
}