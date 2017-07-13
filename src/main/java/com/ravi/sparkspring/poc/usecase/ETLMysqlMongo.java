package com.ravi.sparkspring.poc.usecase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.spark.MongoSpark;
import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

@Component
public class ETLMysqlMongo {

    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    
    
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    
    public void etl(){
    	
    	testMysqlEtl(javaSparkContext, applicationConfiguration);
    }
    
    private static void testMysqlEtl(JavaSparkContext javaSparkContext2, 
    		ApplicationConfiguration applicationConfiguration2) {

    	String connectionString = applicationConfiguration2.getMysqlConnectionString();
    	
		SQLContext sqlContext = new SQLContext(javaSparkContext2);
		
		  Map<String, String> options = new HashMap<String, String>();
	        options.put("driver", MYSQL_DRIVER);
	        options.put("url", connectionString);
	        
	        options.put("dbtable",
	                   "(select columnA, columnB, columnC from tableA) as tableA");
	        options.put("partitionColumn", "columnA");
	
	        options.put("lowerBound", "0");
	        options.put("upperBound", "5000");
	        options.put("numPartitions", "10");

	        //extract MySQL query result as DataFrame (Extract)
	        sqlContext.load("jdbc", options);
	        
	        Dataset<Row> jdbcDF = sqlContext.load("jdbc", options);
	        
	        
	        //Transform
	        jdbcDF.foreachPartition(new ForeachPartitionFunction<Row>() {
	        	
	            public void call(Iterator<Row> t) throws Exception {
	            	 int i =0;
	                while (t.hasNext()){

	                    Row row = t.next();
	                    System.out.println(row.getLong(0));
	                    i++;
	                }
	                System.out.println("Rows Processed ::: " + i);
	            }
	        });
	        
	        //Load
	        MongoSpark.write(jdbcDF).option("collection", "test1").mode("overwrite").save();		
	        
    }
}
