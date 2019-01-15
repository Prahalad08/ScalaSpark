package com.exampleTimers.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt

/**Find movie mates */

/* 
 * First Map every row as (movieID,(userId,ratingByUser))
 * Then join as (movieId,((userId1,ratingByUserID1),(userId2,ratingByUserID2)))
 * then convert as (userId1,userId2),(ratingByUserID1,ratingByUserID2)
 * Find strength based on cosineRule
 * Additionally users can be grouped on all ratings,good ratings,bad ratings
 */


object MovieRec {
  
  type userAndRating = (Int,Double)
  type movieUserAndRating = (Int,(userAndRating,userAndRating))
  
  def filterDup(movie:movieUserAndRating):Boolean={
    return movie._2._1._1 < movie._2._2._1
  }
  
  def makeUserPairs(movie:movieUserAndRating)={
    val user1Id = movie._2._1._1
    val user2Id = movie._2._2._1
    val user1Rating = movie._2._1._2
    val user2Rating = movie._2._2._2
    
    ((user1Id,user2Id),(user1Rating,user2Rating))
  }
  
  type ratingPair = (Double,Double)
  type ratingPairs = Iterable[ratingPair]
  
  def cosineSimilarityAlgo(ratings:ratingPairs)={
    
    var numPairs = 0
    var Ratingxx = 0.0
    var Ratingyy = 0.0
    var Ratingxy = 0.0 
    
    for(rating<-ratings){
      val rating1 = rating._1
      val rating2 = rating._2
      Ratingxx += rating1 * rating1
      Ratingyy += rating2 * rating2
      Ratingxy += rating1 * rating2
      numPairs +=1
    }
    
    var score = 0.0
    val num = Ratingxy
    val den = sqrt(Ratingxx) * sqrt(Ratingyy)
    if(den!=0){
      score = num/den
    }
    (score,numPairs)
    
  }
  
  def filterBad(input:(Int,(Int,Double))):Boolean={
    return input._2._2>=4
  }

  
  def main(args:Array[String]){
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","MovieRec")
    
    val text = sc.textFile("../ml-100k/u.data")
    
    val rdd = text.map(x=>x.split("\t")).map(x=>(x(1).toInt,(x(0).toInt,x(2).toDouble))).filter(filterBad)
    
    //At this point the rdd is of form (movieId,(userId1,ratingByUserID1)
    
    //Now perform join on values to get form (movieId,(userId1,ratingByUserID1),(userId2,ratingByUserID2))
    
    val joinedRatings = rdd.join(rdd)
    
    //Now joinedRatings is of the form (movieId,(userId1,ratingByUserID1),(userId2,ratingByUserID2))
    
    //Now to filter duplicates as userID1 can appear twice in same row
    
    val filterDuplicates = joinedRatings.filter(filterDup)
    
    //Now filteredDuplicates contains (movieId,((userId1,ratingByUserID1),(userId2,ratingByUserID2))) with no duplicates
    
    //Now to convert (movieId,((userId1,ratingByUserID1),(userId2,ratingByUserID2))) to (userId1,userId2),(ratingByUserID1,ratingByUserID2)
    
    val userPairs = filterDuplicates.map(makeUserPairs)
    
    //Now userPairs contains (userId1,userId2),(ratingByUserID1,ratingByUserID2)
    
    //Now to group all user1Id,user2Id together
    
    val groupedUser = userPairs.groupByKey()
    
    //Now groupedUser contains (userId1,userId2)=>(ratingByUserID1,ratingByUserID2),(ratingByUserID1,ratingByUserID2),(ratingByUserID1,ratingByUserID2)...for different movies
    
    val userSimilarities = groupedUser.mapValues(cosineSimilarityAlgo).cache()
    
    //Now userSimilarities consists of (userId1,userID2)=>(score,numRating)
    
    //Now to get specific results for given user
    
    if(args.length>0){
      
      //Set threshold values for  the minimum allowed score and 
      val scoreThreshold = .99
      val numPairsThreshold = 10
      
      val userId = args(0).toInt
      
      val filteredRes = userSimilarities.filter(x=>
        {
        val userPair = x._1
        val simData = x._2
        ((userPair._1==userId || userPair._2==userId) && simData._1>scoreThreshold && simData._2>numPairsThreshold)
        }       
      )
      
      val finalResults = filteredRes.sortBy(x=>(x._2._1 + x._2._2)).top(10).reverse
      
      println("Matches for userId:" + userId)
 
      for(finalResult<-finalResults){
        var name = finalResult._1._1
        if(name==userId){
          name = finalResult._1._2
        }
        println(name+" Strength:" + finalResult._2._1 + " Movie Count:" + finalResult._2._2)
        
      }
    }
    
  }
  
}