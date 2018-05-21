package session.movielens

import org.apache.spark.sql.SparkSession

object MovieLens {

  case class U_User(userId:Int,age:Int,gender:String,professional:String,coding:String)
  case class U_Data(userId:Int,movieId:Int,score:Int,createTime:String)
  case class U_Item(movieId:Int,movieName:String,releaseTime:String,address:String)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark movieLens")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames

    import spark.implicits._
    //读取用户信息
    val u_user = spark.sparkContext.textFile("D:\\soft\\jetbrains\\project\\idea\\myspark\\dataset\\ml-100k\\u.user")
      .map(_.split("\\|"))
      .map(attributes => U_User(attributes(0).trim.toInt,attributes(1).trim.toInt,attributes(2).trim,attributes(3).trim,attributes(4).trim))
      .toDS()
    //读取用户的观影记录
    val u_data = spark.sparkContext.textFile("D:\\soft\\jetbrains\\project\\idea\\myspark\\dataset\\ml-100k\\u.data")
      .map(_.split("	"))
      .map(attributes => U_Data(attributes(0).trim.toInt,attributes(1).trim.toInt,attributes(2).trim.toInt,attributes(3).trim))
      .toDS()
    //读取电影的信息
    val u_item = spark.sparkContext.textFile("D:\\soft\\jetbrains\\project\\idea\\myspark\\dataset\\ml-100k\\u.item")
      .map(x => x.split("\\|"))
      .map(attributes => U_Item(attributes(0).trim.toInt,attributes(1).trim,attributes(2).trim,attributes(3).trim))
      .toDS()
    //查询个人信息，转换时间
    val u_moive = u_data.selectExpr("userId","movieId","score","from_unixtime(createTime,'yyyy-MM-dd HH:mm:ss') as createTime")
      .join(u_item,"movieId")
    //个人信息连接观影记录
    val user_data = u_user.join(u_moive,"userId")
    user_data.orderBy($"userId",$"movieId").show(600)



  }
}
