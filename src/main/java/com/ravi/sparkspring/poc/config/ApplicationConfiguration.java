package com.ravi.sparkspring.poc.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("classpath:application.properties")
public class ApplicationConfiguration {

	@Autowired
    private Environment env;

    @Value("${app.name}")
    private String appName;

    @Value("${spark.home}")
    private String sparkHome;

    @Value("${master.uri:local}")
    private String masterUri;
    
    @Value("${spark.streamduration}")
    private Long sparkStreamDuration;
    
    @Value("${spark.streamhostservice}")
    private String sparkStreamHostedService;
    
    @Value("${spark.streamhostport}")
    private Integer sparkStreamHostedPort;
    
    @Value("${spark.streamstoragelevel}")
    private String sparkStreamStorageLevel;
    
    @Value("${spark.multipleContext}")
    private String sparkMultipleContext;
    
    @Value("${spark.mongoinput}")
    private String sparkMongoInput;
    
    @Value("${spark.mongooutput}")
    private String sparkMongoOutput;
    
    @Value("${mysql.host}")
    private String mysqlHost;
    
    @Value("${mysql.port}")
    private String mysqlPort;
    
    @Value("${mysql.database}")
    private String mysqlDatabase;
    
    @Value("${mysql.username}")
    private String mysqlUserName;
    
    @Value("${mysql.password}")
    private String mysqlPassword;

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setSparkHome(sparkHome)
                .setMaster(masterUri);
        sparkConf.set("spark.driver.allowMultipleContexts", sparkMultipleContext);
        sparkConf.set("spark.mongodb.input.uri", sparkMongoInput);
        sparkConf.set("spark.mongodb.output.uri",sparkMongoOutput);

        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

/*    @Bean
    public JavaStreamingContext javaStreamingContext() {
        return new JavaStreamingContext(sparkConf(), 
        		new Duration(sparkStreamDuration));
    }*/
    
    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .sparkContext(javaSparkContext().sc())
                .appName("Java Spark ETL Ravi")
                .getOrCreate();
    }
    
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

	public String getSparkStreamHostedService() {
		return sparkStreamHostedService;
	}

	public Integer getSparkStreamHostedPort() {
		return sparkStreamHostedPort;
	}

	public String getSparkStreamStorageLevel() {
		return sparkStreamStorageLevel;
	}

	public String getMysqlConnectionString() {
		
		return "jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/" + 
				mysqlDatabase + "?user=" + mysqlUserName + "&password=" + mysqlPassword;
	}
}
