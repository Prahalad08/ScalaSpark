package com.exampleTimers.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

/**Case 1:Check upsets basic
Upsets are calculated on the fact that importance is higher for the given team and the result is a loss or draw away from home or loss at home
Worrying months are in the format month,number of upsets that month
*/

/*Month count
 * 1:January
 * 2:February
 * 3:March
 * 4:April
 * 5:May
 * 6:June
 * 7:July
 * 8:August
 * 9:September
 * 10:October
 * 11:November
 * 12:December
 */

/*
 * field 0:date,
 * field 1:league_id,
 * field 2:league,
 * field 3:team1,
 * field 4:team2,
 * field 5:spi1,
 * field 6:spi2,
 * field 7:prob1,
 * field 8:prob2,
 * field 9:probtie,
 * field 10:proj_score1,
 * field 11:proj_score2,
 * field 12:importance1,
 * field 13:importance2,
 * field 14:score1,
 * field 15:score2,
 * field 16:xg1,
 * field 17:xg2,
 * field 18:nsxg1,
 * field 19:nsxg2,
 * field 20:adj_score1,
 * field 21:adj_score2
 */



object FootballMatches {
  
  case class Upsets(dateYear:Int,dateMonth:Int,dateDate:Int,league:String,team1:String,team2:String,importance1:Float,importance2:Float,scoreTeam1:Int,scoreTeam2:Int)
  
  def split(lines:String):Upsets={
    val fields = lines.split(",")
        if(fields.length==22){
        val dateYear = fields(0).split("-")(0)
        val dateMonth = fields(0).split("-")(1)
        val dateDate = fields(0).split("-")(2)
        val league = fields(2)
        val team1 = fields(3)
        val team2 = fields(4)
        val importance1 = fields(12)
        val importance2 = fields(13)
        val scoreTeam1 = fields(14)
        val scoreTeam2 = fields(15)
        
    if(dateYear.length()>0 && dateMonth.length()>0 && dateDate.length()>0 && league.length()>0 && team1.length()>0 && team2.length()>0 && importance1.length()>0 && importance2.length()>0 && scoreTeam1.length()>0 && scoreTeam2.length()>0)    
        return Upsets(dateYear.toInt,dateMonth.toInt,dateDate.toInt,league,team1,team2,importance1.toFloat,importance2.toFloat,scoreTeam1.toInt,scoreTeam2.toInt)
    else
      return Upsets(-1,-1,-1,"","","",0,0,0,0)
    }
    else
      return Upsets(-1,-1,-1,"","","",0,0,0,0)
    
  }
  
  def main(args:Array[String])={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
              .builder
              .appName("FootballMatches")
              .master("local[*]")
              .getOrCreate()
              
          val text = spark.sparkContext.textFile("../soccer-spi/spi_matches.csv")
          
          val map = text.map(split)
          
          import spark.implicits._
          
          val dataset = map.toDS.cache()
          
          dataset.createOrReplaceTempView("FootballDataSet")
             
    if(args.length==1){
          
          val teamName = args(0)
          
          println("///////////////////////////////"+ teamName +" TOP 10 Home upsets/////////////////////////////////")
          
          val upsetHome = spark.sql("SELECT dateDate,dateMonth,dateYear,team2,league FROM FootballDataSet WHERE importance1>=importance2 AND scoreTeam1<=scoreTeam2 AND team1='" + teamName + "' ORDER BY importance1 DESC")
          
          val upsetDataHome = upsetHome.collect().foreach(println)
          
          println("///////////////////////////////"+ teamName +" TOP 10 Away upsets/////////////////////////////////")
          
          val upsetAway = spark.sql("SELECT dateDate,dateMonth,dateYear,team1,league FROM FootballDataSet WHERE importance2>=importance1 AND scoreTeam2<scoreTeam1 AND team2='"+ teamName +"' ORDER BY importance2 DESC")
          
          val upsetDataAway = upsetAway.collect().foreach(println)
          
          println("//////////////////////////////Worrying Months HOME////////////////////////////////////////")
          
          val worryHome = spark.sql("SELECT dateMonth,COUNT(dateMonth) FROM FootballDataSet WHERE importance1>=importance2 AND scoreTeam1<=scoreTeam2 AND team1='"+ teamName +"' GROUP BY dateMonth ORDER BY COUNT(dateMonth) DESC")
          
          val worryDataHome = worryHome.collect().foreach(println)
          
          println("//////////////////////////////Worrying Months AWAY////////////////////////////////////////")
          
          val worryAway = spark.sql("SELECT dateMonth,COUNT(dateMonth) FROM FootballDataSet WHERE importance2>=importance1 AND scoreTeam2<scoreTeam1 AND team2='"+ teamName +"' GROUP BY dateMonth ORDER BY COUNT(dateMonth) DESC")
          
          val worryDataAway = worryAway.collect().foreach(println)
          
          
          spark.stop()
    }
    
    if(args.length==2){
          
          val teamName1 = args(0)
          
          val teamName2 = args(1)
          
          println("///////////////////////////////"+ teamName1+ " " + teamName2 +" TOP 10 Home upsets/////////////////////////////////")
          
          val upsetHome = spark.sql("SELECT dateDate,dateMonth,dateYear,team2,league FROM FootballDataSet WHERE importance1>=importance2 AND scoreTeam1<=scoreTeam2 AND team1='" + teamName1+ " " + teamName2 + "' ORDER BY importance1 DESC")
          
          val upsetDataHome = upsetHome.collect().foreach(println)
          
          println("///////////////////////////////"+ teamName1+ " " + teamName2 +" TOP 10  Away upsets/////////////////////////////////")
          
          val upsetAway = spark.sql("SELECT dateDate,dateMonth,dateYear,team1,league FROM FootballDataSet WHERE importance2>=importance1 AND scoreTeam2<scoreTeam1 AND team2='"+ teamName1 + " " + teamName2 +"' ORDER BY importance2 DESC")
          
          val upsetDataAway = upsetAway.collect().foreach(println)
          
          println("//////////////////////////////Worrying Months HOME////////////////////////////////////////")
          
          val worryHome = spark.sql("SELECT dateMonth,COUNT(dateMonth) FROM FootballDataSet WHERE importance1>=importance2 AND scoreTeam1<=scoreTeam2 AND team1='"+ teamName1 + " " + teamName2 +"' GROUP BY dateMonth ORDER BY COUNT(dateMonth) DESC")
          
          val worryDataHome = worryHome.collect().foreach(println)
          
          println("//////////////////////////////Worrying Months AWAY////////////////////////////////////////")
          
          val worryAway = spark.sql("SELECT dateMonth,COUNT(dateMonth) FROM FootballDataSet WHERE importance2>=importance1 AND scoreTeam2<scoreTeam1 AND team2='"+ teamName1 + " " + teamName2 +"' GROUP BY dateMonth ORDER BY COUNT(dateMonth) DESC")
          
          val worryDataAway = worryAway.collect().foreach(println)
          
          spark.stop()
    }
    
  } 
  
}