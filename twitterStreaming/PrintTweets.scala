

package twitterStreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._


object PrintTweets {
 

  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context 
    
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    
    // Get rid of log spam 
    setupLogging()

    // Create a DStream from Twitter using streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // filter only english tweets 
     val englishTweets = tweets.filter(_.getLang() == "en") 
    
    // Now extract the text of each status 
     //transform all the letters to lower case
    val statuses = englishTweets.map(status => (status.getId(), status.getText().toString().trim().toLowerCase()))
    
   

    val stopWordsRDD = ssc.sparkContext.textFile("/C:/Users/KEVSER/Desktop/spark/stop-words.txt")
    val posWordsRDD = ssc.sparkContext.textFile("/C:/Users/KEVSER/Desktop/spark/pos-words.txt")
    val negWordsRDD = ssc.sparkContext.textFile("/C:/Users/KEVSER/Desktop/spark/neg-words.txt")

    val positiveWords = posWordsRDD.collect().toSet
    val negativeWords = negWordsRDD.collect().toSet
    val stopWords     = stopWordsRDD.collect().toSet
    
   
    
     val filterStopWords = statuses.map(x => (x._1,x._2.split("\\s+").filter(!stopWords.contains(_)).toList))
     val positiveTweetScore = filterStopWords.map(x => (x._1,(x._2,x._2.filter(positiveWords.contains(_)).toList.length.toFloat/x._2.length)))
     val negativeTweetScore = filterStopWords.map(x => (x._1,(x._2,x._2.filter(negativeWords.contains(_)).toList.length.toFloat/x._2.length)))
     val joinPositiveNegative = positiveTweetScore.join(negativeTweetScore)
     val getJoinColumns = joinPositiveNegative.map(x=>(if( x._2._1._2 >= x._2._2._2) "Positive" else "Negative",x._1))
     val countEveryTenSecs = getJoinColumns.map(x=>(x._1,1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(10), Seconds(10))
     val countEveryThirtySecs = getJoinColumns.map(x=>(x._1,1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(30))
   
    
   filterStopWords.print()
   positiveTweetScore.print()
   negativeTweetScore.print()
   joinPositiveNegative.print()
   getJoinColumns.print()
 
    countEveryTenSecs.print()
    countEveryThirtySecs.print()
    

   
    ssc.start()
    ssc.awaitTermination()
  }  
}