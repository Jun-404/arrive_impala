package util

import org.apache.spark.sql.Row

import java.sql.{Connection, DriverManager}

object MysqlUtil {

  def insert(line: Row, ip: String, dataBases: String, table: String, user: String, passWord: String): Unit = {

    val driver = "com.mysql.jdbc.Driver"
    var url = s"jdbc:mysql://$ip/$dataBases"
    var username = s"$user"
    var password = s"$passWord"

    var bid: Int = line.getInt(1)
    var pid: Int = line.getInt(0)
    var ptype: String = line.getString(2)
    var pmac: String = line.getString(3)
    var cdate: String = line.getString(4)

    println(url + username + password + "")
    println(ip, dataBases, table, user, passWord)
    println(bid + pid + ptype + pmac + cdate)

    var connection: Connection = null

    try {

      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      val statement = connection.createStatement()
      //      val resultSet = statement.executeQuery(s"select name, password from $table")
      var sql = s"insert into $table values ($bid,$pid,$ptype,'$pmac','$cdate')"
      println(sql)
      statement.execute(sql)
    } catch {
      case e => e.printStackTrace
      //case _: Throwable => println("ERROR")
    }
    connection.close()
  }
}
