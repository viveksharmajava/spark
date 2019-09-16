package vivek.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
public class SimpleApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimpleApp myApp  = new SimpleApp();
		System.out.println("Hello");
		 SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local").getOrCreate();
		
		//countDirectory(spark); some problem with it.
		 lineCount(spark);
		 try {
			Thread.sleep(500000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	    spark.stop();

	}
     public static void lineCount(SparkSession spark) {
    	 String logFile = "/Users/viveksharma/Documents/oracle/gettingstarted.txt"; // Should be some file on your system
    	 System.out.println(spark.sparkContext().defaultMinPartitions());
 	    Dataset<String> logData = spark.read().textFile(logFile).cache();
 	   //long numAs = logData.filter(s -> s.contains("a")).count();
 	   // long numBs = logData.filter(s -> s.contains("b")).count();
 	    long numAs = logData.filter((String s) -> s.contains("a")).count();
 		 long numBs = logData.filter((String s) -> s.contains("b")).count();

 	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
 
     }
	public static void countDirectory(SparkSession spark) {
		String filePath = "/Users/viveksharma/Documents/vivek/learning/spark/sample-data/dirlist.txt";
		Dataset<String> dirDs = spark.read().textFile(filePath).cache();
		Dataset<String> arrDirs =  dirDs.filter((String s)-> s.split("/"));
		long etcCount  = arrDirs.filter((String s) -> s.contains("etc")).count();
		System.out.println("etcCount ="+etcCount);
	}
}
