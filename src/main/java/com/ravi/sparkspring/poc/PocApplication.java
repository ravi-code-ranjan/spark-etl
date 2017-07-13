package com.ravi.sparkspring.poc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ravi.sparkspring.poc.service.SparkJobMediatorService;

@SpringBootApplication
public class PocApplication implements CommandLineRunner {

    @Autowired
    SparkJobMediatorService sparkJobMediatorService;
    
	public static void main(String[] args) {
		SpringApplication.run(PocApplication.class, args);
	}

	
    @Override
    public void run(String... args) throws Exception {
      	
    	sparkJobMediatorService.testETL();
    } 
}
