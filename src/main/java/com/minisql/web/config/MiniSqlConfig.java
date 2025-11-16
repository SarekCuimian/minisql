package com.minisql.web.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MiniSqlProperties.class)
public class MiniSqlConfig {
}
