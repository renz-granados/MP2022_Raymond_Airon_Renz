package squad3.data

import squad3.data.Functions._
import squad3.data.Configurations._
import squad3.data.CreateMasterTables._

object ReadFromDatabase extends App {

  //jdbc Connection url
  val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
  //Initialize Spark Session
  val sparkSession = createSparkSession(storage_name,storage_key)
  //Initialize database connection
  val connectionProperties = configureDatabaseProperties(jdbcUsername,jdbcPassword)

  sparkSession.read.jdbc(jdbcUrl, "user_profile", connectionProperties).show()
}
