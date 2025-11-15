package top.guoziyang.mydb.web.dto;

import javax.validation.constraints.NotBlank;

public class SqlExecRequest {

    @NotBlank(message = "sql 不能为空")
    private String sql;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
