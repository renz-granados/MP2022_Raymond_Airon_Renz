package squad3.data

object Configurations {
  //Data Lake storage name & key
  val storage_name = "squad3datalake"
  val storage_key = "H7vFYN3+xWspjeA4tGxsyF5wKEgazjdfqLb1lp+ccbtaVpFlxkSLn9Yhw0ucvC80MoaDnP8QOydQ+AStr/cLQQ=="
  //Data Lake storage file paths
  val user_file = "abfss://squad3blob@squad3datalake.dfs.core.windows.net/userid-profile.tsv"
  val country_continent_file = "abfss://squad3blob@squad3datalake.dfs.core.windows.net/country-continent-dataset.csv"
  val zodiac_file = "abfss://squad3blob@squad3datalake.dfs.core.windows.net/zodiac_table.csv"
  val track_file = "abfss://squad3blob@squad3datalake.dfs.core.windows.net/userid-timestamp-artid-artname-traid-traname.tsv"
  //Database configurations
  val jdbcHostname = "squad3server.database.windows.net"
  val jdbcPort = 1433
  val jdbcDatabase = "squad3database"
  val jdbcUsername = "squad3admin"
  val jdbcPassword = "squad3password!"

}
