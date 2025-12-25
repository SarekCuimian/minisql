package com.minisql.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = "com.minisql")
@ConfigurationPropertiesScan(basePackages = "com.minisql")
public class MiniSqlApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiniSqlApplication.class, args);
    }
}
