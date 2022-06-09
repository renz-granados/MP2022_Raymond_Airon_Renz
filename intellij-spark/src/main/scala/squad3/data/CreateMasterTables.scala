package squad3.data

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import squad3.data.Functions._
import squad3.data.Configurations._

//Creates master table with horoscope sign and continents
object CreateMasterTables extends App {

  //Initialize Spark Session
  val sparkSession = createSparkSession(storage_name, storage_key)

  //Create Dataframes
  var user_profile = sparkSession.read.format("com.databricks.spark.csv").option("header", true).option("delimiter", "\t").load(user_file)
  val country_continent = sparkSession.read.format("com.databricks.spark.csv").option("header", true)
    .load(country_continent_file)
  val zodiac_table = sparkSession.read.format("com.databricks.spark.csv").option("header", true)
    .load(zodiac_file)
  val track_schema = new StructType().add("id", StringType, true)
    .add("timestamp", TimestampType, true)
    .add("musicbrainz_artist_id", StringType, true)
    .add("artist_name", StringType, true)
    .add("musicbrainz_track_id", StringType, true)
    .add("track_name", StringType, true)
  val user_track = sparkSession.read.format("com.databricks.spark.csv").option("header", true).option("delimiter", "\t")
    .schema(track_schema).load(track_file)

  //Create Temp SQL Tables
  zodiac_table.createOrReplaceTempView("zodiac_table")
  user_profile.createOrReplaceTempView("user_profileX")
  country_continent.createOrReplaceTempView("country_continent")
  user_track.createOrReplaceTempView("user_track")

  //Transforming datasets using SQL Queries
  sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  //Create DF and SQL Table of Users DF with updated format of registration date
  user_profile = sparkSession.sql("select `#id` id, gender, age, country, to_date(registered,'MMMM dd, yyyy') as f_registered from user_profileX")
  user_profile.createOrReplaceTempView("user_profile")

  //Create DF and SQL Table of Users with horoscope sign
  val user_zodiac = sparkSession.sql("select u.*, z.zodiac " +
    "from user_profile u inner join zodiac_table z on " +
    "(from_date < to_date and month(f_registered) * 100 + day(f_registered) between from_date and to_date) " +
    "OR (from_date > to_date and (month(f_registered) * 100 + day(f_registered) >= from_date " +
    "OR month(f_registered) * 100 + day(f_registered) <= to_date))")
  user_zodiac.createOrReplaceTempView("user_zodiac")

  //Create master DF and SQL Table with horoscope sign and continents
  val user_continent = sparkSession.sql("select uz.*, c.continent from user_zodiac uz left join country_continent c on uz.country=c.country")
  user_continent.createOrReplaceTempView("user_profile")

  //Group users by continent, country, and zodiac
  val user_group = sparkSession.sql("select continent, country, zodiac, count(id)count_users from user_profile group by continent, country, zodiac")
  user_group.createOrReplaceTempView("user_group")

  //Get top artists for most listened by each horoscope sign
  val most_listened = sparkSession.sql("with oa_rank as (select *, rank() over(partition by zodiac order by n_listened desc)rank_no " +
    "from(select zodiac, artist_name, count(*)n_listened from user_track ut left join user_profile up on up.id=ut.id group by zodiac,artist_name)), " +
    "oa_users as (select zodiac, count(id)count_users from user_profile group by zodiac) " +
    "select r.*, u.count_users from oa_rank r inner join oa_users u on r.zodiac=u.zodiac where rank_no between 1 and 5")
  most_listened.createOrReplaceTempView("most_listened")

  //Set up jdbc Connection
  val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
  val connectionProperties = configureDatabaseProperties(jdbcUsername, jdbcPassword)

  //Upload temp tables to database
  sparkSession.sql("select * from zodiac_table").write.mode(SaveMode.Overwrite)
    .jdbc(jdbcUrl, "zodiac_table", connectionProperties)
  println("LOG ==> zodiac_table uploaded to database")

  sparkSession.sql("select * from country_continent").write.mode(SaveMode.Overwrite)
    .jdbc(jdbcUrl, "country_continent", connectionProperties)
  println("LOG ==> country_continent uploaded to database")

  sparkSession.sql("select * from user_profile").write.mode(SaveMode.Overwrite)
    .jdbc(jdbcUrl, "user_profile", connectionProperties)
  println("LOG ==> user_profile uploaded to database")

  sparkSession.sql("select * from user_group").write.mode(SaveMode.Overwrite)
    .jdbc(jdbcUrl, "user_group", connectionProperties)
  println("LOG ==> user_group uploaded to database")

  sparkSession.sql("select * from most_listened").write.mode(SaveMode.Overwrite)
    .jdbc(jdbcUrl, "most_listened", connectionProperties)
  println("LOG ==> most_listened uploaded to database")
}
