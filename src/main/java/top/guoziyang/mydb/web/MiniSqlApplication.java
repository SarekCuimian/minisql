package top.guoziyang.mydb.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "top.guoziyang.mydb")
public class MiniSqlApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiniSqlApplication.class, args);
    }
}
