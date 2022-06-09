package squad3.data

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties

object Functions {

  //Initialize Spark Session using storage account name & access key
  def createSparkSession (name:String, key:String):SparkSession= {
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    sparkSession.conf.set(s"fs.azure.account.key.${name}.dfs.core.windows.net", key)
    sparkSession
  }

  //Define the database connection properties
  def configureDatabaseProperties(username:String, password:String):Properties = {
    val connectionProperties = new Properties()
    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    connectionProperties.put("user", username)
    connectionProperties.put("password", password)
    connectionProperties.setProperty("Driver", driverClass)
    connectionProperties
  }

}
