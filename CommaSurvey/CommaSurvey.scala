package com.exampleTimers.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


/*
DATASET comma-survey.csv is taken from fivethirtyeight

Link for git for the file:https://github.com/fivethirtyeight/data/tree/master/comma-survey
*/

/*
 * Based on the comma dataset
 * field 0:RespondentID
 * field 1:"In your opinion, which sentence is more gramatically correct?"(It's important for a person to be honest, kind and loyal.)
 * field 2:Prior to reading about it above, had you heard of the serial (or Oxford) comma
 * field 3:How much, if at all, do you care about the use (or lack thereof) of the serial (or Oxford) comma in grammar?
 * field 4:How would you write the following sentence?(Some experts say it's important to drink milk, but the data is inconclusive.)
 * field 5:When faced with using the word ""data"", have you ever spent time considering if the word was a singular or plural noun
 * field 6:How much, if at all, do you care about the debate over the use of the word ""data"" as a singluar or plural noun?
 * field 7:In your opinion, how important or unimportant is proper use of grammar?
 * field 8:Gender
 * field 9:Age
 * field 10:Household Income
 * field 11:Education
 * field 12:Location
 */

/*
 * Objective 1:based on age(field 9), determine whether they have seen comma(field 2),do they care abt comma(field 3),do they care abt the debate on data(field 6)
 */

object CommaSurvey {
  
  case class Comma(age:String,gender:String,householdIncome:String,education:String,location:String,seenComma:String,careForComma:String,careForData:String)
  
  def split(lines:String):Comma={
    val fields = lines.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    if(fields.length==13){
    val ageRange = fields(9)
    val seenComma = fields(2)
    val careForComma = fields(3)
    val careForData = fields(6)
    return Comma(fields(9),fields(8),fields(10),fields(11),fields(12),fields(2),fields(3),fields(6))
    }
    else
      return Comma("","X","","","","","","")
  }
  
  def main(args:Array[String])={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
        .builder
        .appName("CommaSurvey")
        .master("local[*]")
        .getOrCreate()
        
    val text = spark.sparkContext.textFile("../comma-survey.csv")
    
    val mapped = text.map(split)
        
     import spark.implicits._
     
     val dataset = mapped.toDS.cache()
     
     dataset.printSchema
     
     
     dataset.createOrReplaceTempView("Comma")
     
     println("\nFirst field indicates the analysis parameter.\nFollowing fields are(in percentage)\n->Q:Prior to reading about it above, had you heard of the serial (or Oxford) comma\nA:YES\n" +
             "\n->Q:Prior to reading about it above, had you heard of the serial (or Oxford) comma\nA:NO\n"+
             "\n->Q:How much, if at all, do you care about the use (or lack thereof) of the serial (or Oxford) comma in grammar?\nA:Some\n"+
             "\n->Q:How much, if at all, do you care about the use (or lack thereof) of the serial (or Oxford) comma in grammar?\nA:Not much\n"+
             "\n->Q:How much, if at all, do you care about the use (or lack thereof) of the serial (or Oxford) comma in grammar?\nA:A lot\n"+
             "\n->Q:How much, if at all, do you care about the use (or lack thereof) of the serial (or Oxford) comma in grammar?\nA:Not at all\n"+
             "\n->Q:How much, if at all, do you care about the debate over the use of the word \"data\" as a singluar or plural noun?\nA:Some\n"+
             "\n->Q:How much, if at all, do you care about the debate over the use of the word \"data\" as a singluar or plural noun?\nA:Not much\n"+
             "\n->Q:How much, if at all, do you care about the debate over the use of the word \"data\" as a singluar or plural noun?\nA:A lot\n"+
             "\n->Q:How much, if at all, do you care about the debate over the use of the word \"data\" as a singluar or plural noun?\nA:Not at all\n"
     )
     
     println("\nONLY fields with all answers given to survey are analysed\n")
     
     println("\n//////////////////////////////////////////ANALYSIS BASED ON AGE//////////////////////////////\n")
     
     val survey1 = spark.sql("SELECT age,ROUND(SUM(case seenComma when 'Yes' then 1 else 0 end)*100/COUNT(*),2) AS SEENCOMMA," +
                                        "ROUND(SUM(case seenComma when 'No' then 1 else 0 end)*100/COUNT(*),2) AS NOTSEENCOMMA," +
                                        "ROUND(SUM(case careForComma when 'Some' then 1 else 0 end)*100/COUNT(*),2) AS SOMECARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'Not much' then 1 else 0 end)*100/COUNT(*),2) AS NOTMUCHCARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'A lot' then 1 else 0 end)*100/COUNT(*),2) AS FULLCARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'Not at all' then 1 else 0 end)*100/COUNT(*),2) AS NOCARECOMMA," +
                                        "ROUND(SUM(case careForData when 'Some' then 1 else 0 end)*100/COUNT(*),2) AS SOMECAREDATA," +
                                        "ROUND(SUM(case careForData when 'Not much' then 1 else 0 end)*100/COUNT(*),2) AS NOTMUCHCAREDATA," +
                                        "ROUND(SUM(case careForData when 'A lot' then 1 else 0 end)*100/COUNT(*),2) AS FULLCAREDATA," +
                                        "ROUND(SUM(case careForData when 'Not at all' then 1 else 0 end)*100/COUNT(*),2) AS NOCAREDATA " +
                                        "FROM Comma WHERE age!='' GROUP BY age ORDER BY age"
                            )
                            
    val surveyRes1 = survey1.collect()
    
    surveyRes1.foreach(println)
    
    
    println("\n//////////////////////////////////////////ANALYSIS BASED ON GENDER//////////////////////////////\n")
    
    val survey2 = spark.sql("SELECT gender,ROUND(SUM(case seenComma when 'Yes' then 1 else 0 end)*100/COUNT(*),2) AS SEENCOMMA," +
                                        "ROUND(SUM(case seenComma when 'No' then 1 else 0 end)*100/COUNT(*),2) AS NOTSEENCOMMA," +
                                        "ROUND(SUM(case careForComma when 'Some' then 1 else 0 end)*100/COUNT(*),2) AS SOMECARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'Not much' then 1 else 0 end)*100/COUNT(*),2) AS NOTMUCHCARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'A lot' then 1 else 0 end)*100/COUNT(*),2) AS FULLCARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'Not at all' then 1 else 0 end)*100/COUNT(*),2) AS NOCARECOMMA," +
                                        "ROUND(SUM(case careForData when 'Some' then 1 else 0 end)*100/COUNT(*),2) AS SOMECAREDATA," +
                                        "ROUND(SUM(case careForData when 'Not much' then 1 else 0 end)*100/COUNT(*),2) AS NOTMUCHCAREDATA," +
                                        "ROUND(SUM(case careForData when 'A lot' then 1 else 0 end)*100/COUNT(*),2) AS FULLCAREDATA," +
                                        "ROUND(SUM(case careForData when 'Not at all' then 1 else 0 end)*100/COUNT(*),2) AS NOCAREDATA " +
                                        "FROM Comma GROUP BY gender ORDER BY gender"
                            )
                            
    val surveyRes2 = survey2.collect()
    
    surveyRes2.foreach(println)
    
    println("\n//////////////////////////////////////////ANALYSIS BASED ON INCOME//////////////////////////////\n")
    
    val survey3 = spark.sql("SELECT householdIncome,ROUND(SUM(case seenComma when 'Yes' then 1 else 0 end)*100/COUNT(*),2) AS SEENCOMMA," +
                                        "ROUND(SUM(case seenComma when 'No' then 1 else 0 end)*100/COUNT(*),2) AS NOTSEENCOMMA," +
                                        "ROUND(SUM(case careForComma when 'Some' then 1 else 0 end)*100/COUNT(*),2) AS SOMECARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'Not much' then 1 else 0 end)*100/COUNT(*),2) AS NOTMUCHCARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'A lot' then 1 else 0 end)*100/COUNT(*),2) AS FULLCARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'Not at all' then 1 else 0 end)*100/COUNT(*),2) AS NOCARECOMMA," +
                                        "ROUND(SUM(case careForData when 'Some' then 1 else 0 end)*100/COUNT(*),2) AS SOMECAREDATA," +
                                        "ROUND(SUM(case careForData when 'Not much' then 1 else 0 end)*100/COUNT(*),2) AS NOTMUCHCAREDATA," +
                                        "ROUND(SUM(case careForData when 'A lot' then 1 else 0 end)*100/COUNT(*),2) AS FULLCAREDATA," +
                                        "ROUND(SUM(case careForData when 'Not at all' then 1 else 0 end)*100/COUNT(*),2) AS NOCAREDATA " +
                                        "FROM Comma WHERE householdIncome!='' GROUP BY householdIncome ORDER BY householdIncome"
                            )
                            
    val surveyRes3 = survey3.collect()
    
    surveyRes3.foreach(println)
    
    println("\n//////////////////////////////////////////ANALYSIS BASED ON LOCATION//////////////////////////////\n")
    
    val survey4 = spark.sql("SELECT location,ROUND(SUM(case seenComma when 'Yes' then 1 else 0 end)*100/COUNT(*),2) AS SEENCOMMA," +
                                        "ROUND(SUM(case seenComma when 'No' then 1 else 0 end)*100/COUNT(*),2) AS NOTSEENCOMMA," +
                                        "ROUND(SUM(case careForComma when 'Some' then 1 else 0 end)*100/COUNT(*),2) AS SOMECARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'Not much' then 1 else 0 end)*100/COUNT(*),2) AS NOTMUCHCARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'A lot' then 1 else 0 end)*100/COUNT(*),2) AS FULLCARECOMMA," +
                                        "ROUND(SUM(case careForComma when 'Not at all' then 1 else 0 end)*100/COUNT(*),2) AS NOCARECOMMA," +
                                        "ROUND(SUM(case careForData when 'Some' then 1 else 0 end)*100/COUNT(*),2) AS SOMECAREDATA," +
                                        "ROUND(SUM(case careForData when 'Not much' then 1 else 0 end)*100/COUNT(*),2) AS NOTMUCHCAREDATA," +
                                        "ROUND(SUM(case careForData when 'A lot' then 1 else 0 end)*100/COUNT(*),2) AS FULLCAREDATA," +
                                        "ROUND(SUM(case careForData when 'Not at all' then 1 else 0 end)*100/COUNT(*),2) AS NOCAREDATA " +
                                        "FROM Comma WHERE location!='' GROUP BY location ORDER BY location"
                            )
                            
    val surveyRes4 = survey4.collect()
    
    surveyRes4.foreach(println)
    
    spark.stop()
    
  }
  
  
}
