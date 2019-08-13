package vivek.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
public class SimpleApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Hello");
		String logFile = "/Users/viveksharma/Documents/oracle/gettingstarted.txt"; // Should be some file on your system
	    SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local").getOrCreate();
	    Dataset<String> logData = spark.read().textFile(logFile).cache();
	    
	   // long numAs = logData.filter(s -> s.contains("a")).count();
	   // long numBs = logData.filter(s -> s.contains("b")).count();
	    long numAs = logData.count();
		 long numBs = logData.count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

	    spark.stop();

	}

}
