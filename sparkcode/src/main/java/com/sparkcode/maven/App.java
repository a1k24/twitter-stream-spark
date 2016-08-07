package com.sparkcode.maven;

/**
 * Hello world!
 *
 */
import java.io.*;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.GeoLocation;
import twitter4j.Status;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.*;
// import scala.Function1;
import scala.Tuple2;
import scala.runtime.AbstractFunction2;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import com.twitter.*;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;


public class App {
    public static void main(String[] args) {
        final String consumerKey = "72Xk5AHG6sPic0chIVeNlZqSz";
        final String consumerSecret = "a5SfPIZXIZkDxnJh7I4A0HSna3w52UD7Kzv35id8jFEHLLAzwe";
        final String accessToken = "27398343-CJeKK9Q7cPvvmZY8WSdRZAe9EyDLzaDRg9enuvLZm";
        final String accessTokenSecret = "f3vcqk48YDZ7dK8ZzoSzzAWFj5FxnsdVtD5mBS1Bzdsiv";
        final String proxyHost = "10.3.100.207";
        final String proxyPort = "8080";

        String url = "jdbc:mysql://localhost:3306/spark_data";
        final String user = "root";
        final String password = "3235";

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamExample");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
 
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
        System.setProperty("twitter4j.http.proxyHost", proxyHost);
        System.setProperty("twitter4j.http.proxyPort", proxyPort);
 
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

        MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("spark_data");

		db.getCollection("hashtags").createIndex(new Document("hashtag", "hashed"));
		db.getCollection("hashtags").createIndex(new Document("count", -1));
		db.getCollection("mentions").createIndex(new Document("mention", "hashed"));
		db.getCollection("mentions").createIndex(new Document("count", -1));
        db.getCollection("co_occuring_ht").createIndex(new Document("key", "hashed"));
        db.getCollection("co_occuring_ht").createIndex(new Document("count", -1));
        // db.getCollection("data").insertOne(
        //     new Document("address",
        //         new Document()
        //             .append("street", "2 Avenue")
        //             .append("zipcode", "10075")
        //             .append("building", "1480"))
        //         .append("borough", "Manhattan")
        //         .append("cuisine", "Italian")
        //         .append("name", "Vella")
        //         .append("restaurant_id", "41704620"));
 
        // JavaDStream<Status> tweetsWithLocation = twitterStream.filter(
        //     new Function<Status, Boolean>() {
        //         public Boolean call(Status status){
        //             if (status.getGeoLocation() != null) {
        //                 return true;
        //             } else {
        //                 return false;
        //             }
        //         }
        //     }
        // );

        // JavaDStream<String> statuses = tweetsWithLocation.map(
        //     new Function<Status, String>() {
        //         public String call(Status status) {
        //             return status.getGeoLocation().toString() + ": " + status.getText();
        //         }
        //     }
        // );
 		

        JavaDStream<String> statuses = twitterStream
        .filter(new Function<Status,Boolean>(){
        	public Boolean call(Status s) {
                    System.out.println("==============================3 "+s.getLang());
                    return (s.getLang().equals("en"));
                }
        })
        .map(
            new Function<Status, String>() {
                public String call(Status status) {
                    String text = "";
                    List<String> names, hashtags;
                    Extractor extractor = new Extractor();
			        MongoClient mongoClient = new MongoClient();
			        MongoDatabase db = mongoClient.getDatabase("spark_data");
			        UpdateOptions updateOption = new UpdateOptions();
			        updateOption.upsert(true);

                    hashtags = extractor.extractHashtags(status.getText());
                    names = extractor.extractMentionedScreennames(status.getText());
                    for (String name : names) {
                        text += "@" + name + " ";
                        db.getCollection("mentions").updateOne(new Document("mention", name), new Document("$inc", new Document("count", 1)), updateOption);
                    }

                    // Lexically order hashtags
                    Collections.sort(hashtags);
                    int hlen = hashtags.size();
                    for (int hit = 0; hit < hlen; hit++) {
                        for (int shit = 0; shit < hit; shit++) {
                            Document ht_couple = new Document();
                            String ht1 = hashtags.get(shit), ht2 = hashtags.get(hit);
                            ht_couple.append("key", ht1 + " " + ht2);
                            ht_couple.append("ht1", ht1);
                            ht_couple.append("ht2", ht2);
                            db.getCollection("co_occurring_ht").updateOne(ht_couple, new Document("$inc", new Document("count", 1)), updateOption);
                        }
                    }

                    for (String hashtag : hashtags) {
                        text += "#" + hashtag + " ";
                    	db.getCollection("hashtags").updateOne(new Document("hashtag", hashtag), new Document("$inc", new Document("count", 1)), updateOption);
					}

                    Document tweet = new Document();
                    tweet.append("username", status.getUser().getScreenName());
                    tweet.append("favourites", status.getFavoriteCount());
                    tweet.append("mentions", names);
                    tweet.append("text", status.getText());
                    if(status.getPlace() == null || status.getPlace().getCountry() == null)
                    	tweet.append("country", "");
                    else
                    	tweet.append("country", status.getPlace().getCountry());
                    tweet.append("hashtags", hashtags);
                    db.getCollection("data").insertOne(tweet);

                    mongoClient.close();

                    return text.trim();
                }
            }
        );


    //     JavaDStream<String> words = statuses.flatMap(
    //         new FlatMapFunction<String, String>() {
    //             public Iterable<String> call(String line) {
    //                 return Arrays.asList(line.split(" "));
    //             }
    //         }
    //     );
    //     JavaPairDStream<String, Integer> pairs = words.mapToPair(
    //         new PairFunction<String, String, Integer>() {
    //             public Tuple2<String, Integer> call(String word) {
    //                 return new Tuple2<String, Integer>(word, 1);
    //             }
    //         }
    //     );
    //     JavaPairDStream<String, Integer> counts = pairs.reduceByKey(
    //         new Function2<Integer, Integer, Integer>() {
    //             public Integer call(Integer a, Integer b) { return a + b; }
    //         }
    //     );

    //     counts.foreachRDD(
    //     	rdd -> {
	   //    		List<Tuple2<String, Integer>> topEndpoints = rdd.collect();
	   //        	for(Tuple2<?,?> tuple : topEndpoints){
    //                 String add = "";
	   //        		System.out.println(tuple._1() + ": " + tuple._2());
    //                 if(tuple._1().equals(""))
    //                     continue;
    //                 if(tuple._1().toString().charAt(0) == '#'){
    //                     add = "INSERT into data_hashtags VALUES('"
    //                         + tuple._1() +"' ,"
    //                         + tuple._2() +")";
    //                 }
    //                 else {
    //                     add = "INSERT into data_names VALUES('"
    //                         + tuple._1() +"' ,"
    //                         + tuple._2() +")";
    //                 }
    //     			try {
    //     			    Statement stmt = null;
    //     			    Connection con = DriverManager.getConnection(url, user, password);
	   //            		PreparedStatement pstmt = con.prepareStatement(add);
	   //  				pstmt.executeUpdate();
    //     			    System.out.println("Success");
    //     			} catch (SQLException ex) {

    //     			}
	   //        	}
				// return null;
    // 	    });
        // counts.print();
        statuses.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
