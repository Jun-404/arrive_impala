package app

import org.apache.spark.sql.{DataFrame, SparkSession}
import util.MysqlUtil

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

object Arrive_impala {

  private val prop1 = new Properties()
  prop1.setProperty("user", "root")
  prop1.setProperty("password", "i@SH021.bg")
  prop1.setProperty("driver", "com.mysql.jdbc.Driver")

  def main(args: Array[String]): Unit = {

    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd") //.format(new Date())
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var dt = dateFormat.format(cal.getTime())
    println(dt)

    val spark = SparkSession.builder()
      .appName("arrive_impala")
      .master("local[10]")
      .enableHiveSupport()
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .getOrCreate()

    spark.sql("show databases").show()

    val frame: DataFrame = spark.sql(
      s"select * from bgd.ads_dvs_arrive_count_day where dt = $dt "
    ).toDF()

    //    frame.show(100)


    spark.read.jdbc("jdbc:mysql://172.30.12.105:3306/DBMS?useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false", "taskconfig", prop1).createOrReplaceTempView("taskconfig")

    val frame1: DataFrame = spark.sql(
      "SELECT " +
        "proid,brandid bid,ptype,placedb_ip,placedb_name,placedb_account,placedb_pwd,arrive_table," +
        "pay_ip,pay_name,pay_account,pay_pwd,pay_arrive_table,`status` " +
        "FROM taskconfig " +
        "GROUP BY " +
        "proid,brandid,ptype,placedb_ip,placedb_name,placedb_account,placedb_pwd,arrive_table," +
        "pay_ip,pay_name,pay_account,pay_pwd,pay_arrive_table,`status`").toDF()


    val frame2: DataFrame = frame.join(frame1, Seq("proid", "bid", "ptype")) //.show(100)

//    frame2.show(/*300*/)

    frame2.foreach {
      row => {
        if (row.getInt(16) == 0) {
          MysqlUtil.insert(row, row.getString(11), row.getString(12), row.getString(15), row.getString(13), row.getString(14))
          MysqlUtil.insert(row, row.getString(6), row.getString(7), row.getString(10), row.getString(8), row.getString(9))
        } else if (row.getInt(16) == 1)
//          println("已插入")
          MysqlUtil.insert(row, row.getString(6), row.getString(7), row.getString(10), row.getString(8), row.getString(9))
      }
    }

  }
}
