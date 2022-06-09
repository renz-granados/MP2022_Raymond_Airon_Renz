package squad3.data

import org.apache.spark.sql.SparkSession

object LocalTest extends App {
  val sparkSession = SparkSession.builder().master(master="local").getOrCreate()
  val localDatasetPath = "C:\\Users\\airon.v.hiwatig\\OneDrive - Accenture\\Documents\\BIG DATA MASTERCLASS\\Practical\\Datasets\\"

  val userDataset = sparkSession.read.format(source="com.databricks.spark.csv")
    .option("header",true)
    .option("delimiter","\t")
    .load(path=localDatasetPath+"userid-profile.tsv")

  userDataset.createOrReplaceTempView("usertable")
  sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  sparkSession.sql("select *, to_date(registered,'MMMM dd, yyyy') as registration_date from usertable").show()
}
