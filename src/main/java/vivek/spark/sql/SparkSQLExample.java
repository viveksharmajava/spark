package vivek.spark.sql;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.Collections;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.examples.sql.JavaSparkSQLExample.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class SparkSQLExample {
	public static class Person implements Serializable {
	    private String name;
	    private int age;

	    public String getName() {
	      return name;
	    }

	    public void setName(String name) {
	      this.name = name;
	    }

	    public int getAge() {
	      return age;
	    }

	    public void setAge(int age) {
	      this.age = age;
	    }
	  }

	public static void main(String[] args) {

		 SparkSession spark = SparkSession.builder().appName("Java Spark Sql sample App").config("spark.master", "local").getOrCreate();
		 //runBasicDataFrameExample(spark);
		 runDatasetCreationExample(spark);
		 spark.stop();
	}
	private static void runBasicDataFrameExample(SparkSession spark) {
		
		Dataset<Row> df = spark.read().json("src/main/resources/people.json");
		//Show the Content of DataFrame to stdout.
		df.show();
		
		//Print the schema in tree format.
		df.printSchema();
		
		//DF select based on name.
		df.select("name").show();
		
		// Select everybody, but increment the age by 1
	    df.select(col("name"), col("age").plus(1)).show();
	    
	    //select people whoes age > 21 
	    df.filter(col("age").gt(21)).show();
	   //group by age count people 
	    df.groupBy("age").count().show();
	    
	    //Register DataFrame as SQL template viewer.
	    
	    df.createOrReplaceTempView("people");
	    
	    Dataset<Row> sqlDf = spark.sql("Select * from people");
	    sqlDf.show();
	    
	}
	 private static void runDatasetCreationExample(SparkSession spark) {
		 Person p = new Person();
		 p.age = 40;
		 p.name ="Parvati";
		 Encoder<Person> personEncoder = Encoders.bean(Person.class);
		 Dataset<Person> personDs = spark.createDataset(Collections.singletonList(p), personEncoder);
		 personDs.show();
		// DataFrames can be converted to a Dataset by providing a class. Mapping based on name
		    String path = "src/main/resources/people.json";
		    Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
		    peopleDS.show();
	 } 
	 private static void runInferSchemaExample(SparkSession spark) {
		 JavaRDD jRdd = spark.read().textFile("src/main/resources/people.txt").javaRDD();
	 }
}
 