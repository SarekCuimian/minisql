package com.minisql.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.minisql")
public class MiniSqlApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiniSqlApplication.class, args);
    }
}

