package context.wordCount.output

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkContextWordCount {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    //创建conf，设置应用的名字和运行方式，local[2]运行2个线程，产生两个文件结果
    val config = new SparkConf().setAppName("wordCount").setMaster("local[1]")
    //创建上下文对象
    val sparkContext = new SparkContext(config)
    //开始计算代码
    val file:RDD[String] = sparkContext.textFile("hdfs://hd1:9000/wordCount/words") //textFile从hdfs读文件
    //1.缓存到内存中
    file.cache()
    //2.缓存到硬盘中
    //file.persist(StorageLevel.MEMORY_AND_DISK)

    val words:RDD[String] = file.flatMap(_.split(" "))//压平，分割每一行数据为每个单词
    val tuple:RDD[(String,Int)] = words.map((_,1))//将单词转换为（_,1）
    val result:RDD[(String,Int)] = tuple.reduceByKey(_+_)//将相同的key汇总聚合，前一个_表示累加数据，后一个表示新数据
    val resultSort:RDD[(String,Int)] = result.sortBy(_._1,true)//第一个参数：按照出现次数进行排序，第二个参数f：正序,倒序

    //输出目录存在就删除
    val output = new Path("E:\\jetbrains\\project\\idea\\myspark\\src\\main\\scala\\context\\wordCount\\output");
    val fileSystem = FileSystem.get(new Configuration());
    if (fileSystem.exists(output)) fileSystem.delete(output, true)

    resultSort.foreach(println)//打印结果
    resultSort.saveAsTextFile(output.toString)
    //resultSort.saveAsTextFile("hdfs://hd1:9000/wordsResult")

  }

}
