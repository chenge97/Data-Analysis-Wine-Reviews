package handler

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import java.io._
import java.nio.file.{ Files, FileSystems }


class Stats (val _path :  String, val _filename: String) {


  def createContext():  SQLContext = {

    val conf = new SparkConf().setMaster("yarn-client")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);

    sqlContext
  }
  

  def prepareDataframe(l_sqlContext : SQLContext): org.apache.spark.sql.DataFrame = {

    var df = l_sqlContext.read.format("json").load(_path)

    df = df.withColumn("price" , df("price").cast(IntegerType))

    df = df.withColumn("points" , df("points").cast(IntegerType))

    df
  }

  def deleteFileIfExist(){

    val defaultFS = FileSystems.getDefault()
    val separator = defaultFS.getSeparator()

    val path = defaultFS.getPath(_filename)

    if(Files.exists(path)){

      val fileToDelete = new File(_filename)

      if(fileToDelete.delete()){

        println("File deleted succesfully")

      }else{
        println("Error! Something wrong happened")

      }

    }else{
      println("File doesn't exists")
    }


  }

  def writeFile(stringToWrite: String){

    val writer = new FileWriter(_filename,true)
    writer.write(stringToWrite+"\n")
    writer.close()

  }


  def getMainStats(df : org.apache.spark.sql.DataFrame){

    var l_df = df

    val avg_price_df = l_df.select(avg("price"))
    var avg_price_value = avg_price_df.first.getDouble(0)
    avg_price_value  =  BigDecimal(avg_price_value).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble

    val max_price_df = l_df.select(max("price"))
    val max_price_value = max_price_df.first.getInt(0)

    val min_price_df = l_df.select(min("price"))
    val min_price_value = min_price_df.first.getInt(0)
    writeFile("Getting the main stats in the dataframe")

    writeFile("")

    writeFile("The max price is " + max_price_value.toString)
    writeFile("The min price is " + min_price_value.toString)
    writeFile("The avg price is " + avg_price_value.toString)

    writeFile("")

    val avg_points_df = l_df.select(avg("points"))
    var avg_points_value = avg_points_df.first.getDouble(0)
    avg_points_value  =  BigDecimal(avg_points_value).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble

    val max_points_df = l_df.select(max("points"))
    val max_points_value = max_points_df.first.getInt(0)

    val min_points_df = l_df.select(min("points"))
    val min_points_value = min_points_df.first.getInt(0)

    writeFile("The max points is " + max_points_value.toString)
    writeFile("The min points is " + min_points_value.toString)
    writeFile("The avg points is " + avg_points_value.toString)

    writeFile("")


  }

  def relationshipPricePoints(df : org.apache.spark.sql.DataFrame){

    writeFile("Searching for relationship between price and points")

    writeFile("Looking if wines with prices higher than 100 dlls have any relationship with the chepear ones")

    writeFile("")

    var l_df = df

    l_df = l_df.filter("title is not null")

    val totalOfWines = l_df.count()

    val df_top95 = l_df.filter("points >= 95")

    val totalOfTop95Wines = df_top95.count()

    val df_more100 = df_top95.filter("price >= 100")

    val totalOfMoreThan100Wines = df_more100.count

    val df_less100 = df_top95.filter("price < 100")

    val totalOfLessThan100Wines = df_less100.count

    val df_Null = df_top95.filter("price is null")

    val totalOfNull = df_Null.count

    writeFile("Total of wines in the dataset: "+totalOfWines)
    writeFile("Total of wines with Top 95 score: "+totalOfTop95Wines)
    writeFile("Total of top 95 wines with prices higher or equal than 100 :" +totalOfMoreThan100Wines)
    writeFile("Total of top 95 wines with prices lesser than 100 :" +totalOfLessThan100Wines)
    writeFile("Total of top 95 wines with no price :" +totalOfNull)
    writeFile("")


    var percentageMoreThan100Price = (totalOfMoreThan100Wines.toDouble/totalOfTop95Wines)*100
    percentageMoreThan100Price  =  BigDecimal(percentageMoreThan100Price).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble

    var percentageLessThan100Price = (totalOfLessThan100Wines.toDouble/totalOfTop95Wines)*100
    percentageLessThan100Price  =  BigDecimal(percentageLessThan100Price).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble

    var percentageNull = (totalOfNull.toDouble/totalOfTop95Wines)*100
    percentageNull  =  BigDecimal(percentageNull).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble

    writeFile("Percent of top 95 wines with prices higher or equal than 100: " +percentageMoreThan100Price+"%")
    writeFile("Percent of top 95 wines with prices lesser than 100: " +percentageLessThan100Price+"%")
    writeFile("Percent of top 95 wines with no prices : " +percentageNull+"%")
    writeFile("")


  }


  def countriesInTop95(df : org.apache.spark.sql.DataFrame){
    writeFile("Searching which are the top 5 countries that appear the most in the dataset")

    writeFile("")
    var df_top95 = df.filter("points is not null")

    df_top95 = df_top95.filter("points >= 95")

    val top5Countries = df_top95.select("country").groupBy("country").agg(count("country")).orderBy(desc("count(country)")).limit(5)

    for(row <- top5Countries.rdd.collect){
      var country = row.mkString(",").split(",")(0)
      var howManyTimesItIsRepeated= row.mkString(",").split(",")(1)

      writeFile("The top country "+country+" it is repeated "+howManyTimesItIsRepeated)
    }
    writeFile("")


  }

  def mostRelevantJudgesinDataset(df : org.apache.spark.sql.DataFrame,l_sqlContext : SQLContext){
    writeFile("Searching which judges are the most relevant to take in mind to see which wines have they score higher than 94")
    writeFile("The way that we are going to make this approach is by searching judges that have give low,medium and high scores")

    writeFile("")

    val totalOfRecordsInDF = df.count
    var df_top95 = df.filter("points is not null")

    df_top95 = df_top95.filter("points >= 95")

    writeFile("Total of Top 95 rows: "+df_top95.count)
    var df_88To94= df.filter("points is not null")

    df_88To94 = df_88To94.filter("points <= 94 and points > 87")

    var df_lowerThan87= df.filter("points is not null")

    df_lowerThan87 = df_lowerThan87.filter("points < 87")

    df_top95 = df_top95.select("taster_name").distinct()
    df_88To94 = df_88To94.select("taster_name").distinct()
    df_lowerThan87 = df_lowerThan87.select("taster_name").distinct()

    df_top95.registerTempTable("df_top95")
    df_88To94.registerTempTable("df_88To94")
    df_lowerThan87.registerTempTable("df_lowerThan87")

    val df_top95RealValuesToCompare = df.select("title","country","points","price","taster_name").filter("points is not null and points >= 95")

    val df_NameOfBestTasters= l_sqlContext.sql("""SELECT T1.taster_name FROM df_top95 T1 INNER JOIN (SELECT df_lowerThan87.taster_name
                  FROM  df_lowerThan87 INNER JOIN df_88To94
                  ON df_lowerThan87.taster_name = df_88To94.taster_name) T2 ON T1.taster_name = T2.taster_name""")

    df_top95RealValuesToCompare.registerTempTable("df_top95RealValuesToCompare")
    df_NameOfBestTasters.registerTempTable("df_NameOfBestTasters")

    val df_top95RealValuesToShow = l_sqlContext.sql("""SELECT df_top95RealValuesToCompare.title, df_top95RealValuesToCompare.country, 
      df_top95RealValuesToCompare.points, df_top95RealValuesToCompare.price,df_top95RealValuesToCompare.taster_name FROM df_top95RealValuesToCompare INNER JOIN 
      df_NameOfBestTasters ON df_top95RealValuesToCompare.taster_name = df_NameOfBestTasters.taster_name""")

    writeFile("Total of records in table before filters: "+totalOfRecordsInDF)
    writeFile("Total of records after filters of best tasters "+ df_top95RealValuesToShow.count)


  }


}



object Stats {

  def getInstance(_path: String,_filename: String) : Stats = {
    var stats = new Stats(_path,_filename)
    stats

  }


}

object Handler{
  def main(args: Array[String]){
    val stats = Stats.getInstance("<-- Directory inside HDFS (Hadoop) -->","<-- Directory where to save the file with the results of the Data analysis -->")
    val g_SqlContext = stats.createContext()
    var g_df = stats.prepareDataframe(g_SqlContext)
    stats.deleteFileIfExist()
    stats.getMainStats(g_df)
    stats.relationshipPricePoints(g_df)
    stats.countriesInTop95(g_df)
    stats.mostRelevantJudgesinDataset(g_df,g_SqlContext)
    
  }
  
}