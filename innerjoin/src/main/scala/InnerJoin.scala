

object InnerJoin {
  def main(args: Array[String]): Unit = {
    val user_df = sc.read.format("csv").option("header", "false").load("/users.csv")
    val zipcodes_df = sc.read.format("csv").option("header", "false").load("/zipcodes.csv")

    val ij = user_df.join(zipcodes_df, user_df("_c4") === zipcodes_df("_c0")).drop(user_df("_c4"))
    ij.show
  }
}