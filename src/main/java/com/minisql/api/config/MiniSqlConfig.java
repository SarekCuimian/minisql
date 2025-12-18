package com.minisql.api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "minisql.engine")
public class MiniSqlConfig {

    /**
     * 后端 MyDB TCP 服务器地址
     */
    private String host;

    /**
     * 后端 MyDB TCP 服务器端口
     */
    private int port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
