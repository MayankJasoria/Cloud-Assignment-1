package com.cloud.project.scala_queries

import com.cloud.project.Globals
import com.cloud.project.contracts.DBManager
import com.cloud.project.models.OutputModel
import com.cloud.project.sqlUtils.ParseSQL
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Time
import org.apache.spark.sql.{SaveMode, SparkSession}


object SparkInnerJoin {
	
	def execute(parseSQL: ParseSQL, innerJoinOutput: OutputModel): Unit = {
		
		val sc = SparkSession.builder()
			.master(Globals.getSparkMaster) // necessary for allowing spark to use as many laogical datanodes as available
			.getOrCreate()
		//    val user_df = sc.read.format("csv").option("header", "false").load("hdfs://localhost:9000/users.csv")
		//    val zipcodes_df = sc.read.format("csv").option("header", "false").load("hdfs://localhost:9000/zipcodes.csv")
		
		val jk = parseSQL.getOperationColumns.get(0)
		
		val startTime = Time.now
		
		var table1 = sc.read.format("csv").option("header", "false")
			.load(Globals.getNamenodeUrl + Globals.getCsvInputPath + DBManager.getFileName(parseSQL.getTable1))
		
		for (a <- 0 until DBManager.getTableSize(parseSQL.getTable1)) {
			table1 = table1.withColumnRenamed("_c" + a,
				DBManager.getColumnFromIndex(parseSQL.getTable1, a))
		}
		
		var table2 = sc.read.format("csv").option("header", "false")
			.load(Globals.getNamenodeUrl + Globals.getCsvInputPath + DBManager.getFileName(parseSQL.getTable2))
		
		for (a <- 0 until DBManager.getTableSize(parseSQL.getTable2)) {
			table2 = table2.withColumnRenamed("_c" + a,
				DBManager.getColumnFromIndex(parseSQL.getTable2, a))
		}
		
		//		table1.show
		//		table2.show
		
		val table1Enum = parseSQL.getTable1
		val table2Enum = parseSQL.getTable2
		
		parseSQL.getWhereTable match {
			case `table1Enum` =>
				table1 = table1.select("*")
					.where(parseSQL.getWhereColumn
						+ "=" + parseSQL.getWhereValue).toDF()
				table1.show
			
			case `table2Enum` =>
				table2 = table2.select("*")
					.where(parseSQL.getWhereColumn
						+ "=" + parseSQL.getWhereValue).toDF()
				table2.show
			
			case _ => new IllegalArgumentException("Table " + parseSQL.getWhereTable.name
				+ " is not part of the join tables")
		}
		
		val ij = table1.join(table2, table1(jk) === table2(jk)).drop(table2(jk))
		
		
		ij.show
		var plan = "select(" + parseSQL.getWhereTable.name + "[" + parseSQL.getWhereColumn + "] = " + parseSQL.getWhereValue + ")\n"
		plan = plan + ".join(" + parseSQL.getTable1.name + "[" + jk + "] = " + parseSQL.getTable2.name + "[" + jk + "])\n"
		plan = plan + ".drop(" + parseSQL.getTable2.name + " [" + jk + "])\n"
		plan = plan + ".show"
		
		innerJoinOutput.setSparkPlan(plan)
		val endTime = Time.now()
		innerJoinOutput.setSparkExecutionTime(String.valueOf(endTime - startTime) + " milliseconds")
		
		//				innerJoinOutput.setSparkExecutionTime(sc.time(ij.show). + "")
		//innerJoinOutput.setSparkOutput(ij.write.format("csv").toString)
		val outputPathString = Globals.getNamenodeUrl + Globals.getSparkOutputPath
		ij.write.mode(SaveMode.Overwrite).csv(outputPathString)
		
		val outputPath = new Path(outputPathString)
		
		val it = outputPath.getFileSystem(sc.sparkContext.hadoopConfiguration).listFiles(outputPath, false)
		val downloadUrl = new StringBuilder()
		while (it.hasNext) {
			val file = it.next()
			if (file.isFile) {
				val filename = file.getPath.getName
				if (filename.matches("part-[\\d-*][[a-zA-Z0-9]-]*.*")) {
					downloadUrl.append(Globals.getWebhdfsHost)
						.append("/webhdfs/v1")
						.append(Globals.getSparkOutputPath).append("/")
						.append(filename)
						.append("?op=OPEN\n")
				}
			}
		}
		
		downloadUrl.append("NOTE: These URLs will work only if WebHDFS is enabled")
		
		innerJoinOutput.setSparkOutputUrl(downloadUrl.toString())
	}
}