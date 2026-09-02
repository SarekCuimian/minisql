package com.minisql.engine.sql.parser;

import java.util.Arrays;

import com.google.gson.Gson;

import org.junit.jupiter.api.Test;

import com.minisql.engine.sql.ast.statement.Begin;
import com.minisql.engine.sql.ast.statement.Create;
import com.minisql.engine.sql.ast.statement.Delete;
import com.minisql.engine.sql.ast.statement.Insert;
import com.minisql.engine.sql.ast.statement.Select;
import com.minisql.engine.sql.ast.statement.Show;
import com.minisql.engine.sql.ast.Statement;
import com.minisql.engine.sql.ast.statement.Update;
import com.minisql.engine.transaction.mvcc.IsolationLevel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ByteUtilTest {
    @Test
    public void testCreate() throws Exception {
        String stat = "create table student (id int32 primary key, name string, uid int64, fsm (name, uid));";
        Statement res = Parser.parse(stat.getBytes());
        Create create = (Create)res;
        assertEquals("student", create.tableName);
        System.out.println("Create");
        for (int i = 0; i < create.fieldName.length; i++) {
            System.out.println(create.fieldName[i] + ":" + create.fieldType[i]);
        }
        System.out.println(Arrays.toString(create.index));
        System.out.println("======================");
    }

    @Test
    public void testBegin() throws Exception {
        String stat = "begin isolation level read committed";
        Statement res = Parser.parse(stat.getBytes());
        Begin begin = (Begin)res;
        assertNotEquals(IsolationLevel.REPEATABLE_READ, begin.isolationLevel);

        stat = "begin";
        res = Parser.parse(stat.getBytes());
        begin = (Begin)res;
        assertNotEquals(IsolationLevel.REPEATABLE_READ, begin.isolationLevel);

        stat = "begin isolation level repeatable read";
        res = Parser.parse(stat.getBytes());
        begin = (Begin)res;
        assertEquals(IsolationLevel.REPEATABLE_READ, begin.isolationLevel);
    }

    @Test
    public void testRead() throws Exception {
        String stat = "select name, id, strudeng from student where id > 1 and id < 4";
        Statement res = Parser.parse(stat.getBytes());
        Select select = (Select)res;
        assertEquals("student", select.tableName);
        Gson gson = new Gson();
        System.out.println("Select");
        System.out.println(gson.toJson(select.fields));
        System.out.println(gson.toJson(select.where));
        System.out.println("======================");
    }

    @Test
    public void testInsert() throws Exception {
        String stat = "insert into student values (5, 'Guo Ziyang', 22);";
        Statement res = Parser.parse(stat.getBytes());
        Insert insert = (Insert)res;
        Gson gson = new Gson();
        System.out.println("Insert");
        System.out.println(gson.toJson(insert));
        System.out.println("======================");
    }

    @Test
    public void testDelete() throws Exception {
        String stat = "delete from student where name = \"Guo Ziyang\"";
        Statement res = Parser.parse(stat.getBytes());
        Delete delete = (Delete)res;
        Gson gson = new Gson();
        System.out.println("Delete");
        System.out.println(gson.toJson(delete));
        System.out.println("======================");
    }

    @Test
    public void testShow() throws Exception {
        String stat = "show";
        Statement res = Parser.parse(stat.getBytes());
        Show show = (Show)res;
        Gson gson = new Gson();
        System.out.println("Show");
        System.out.println(gson.toJson(show));
        System.out.println("======================");
    }

    @Test
    public void testUpdate() throws Exception {
        String stat = "update student set name = \"GZY\" where id = 5";
        Statement res = Parser.parse(stat.getBytes());
        Update update = (Update)res;
        Gson gson = new Gson();
        System.out.println("Update");
        System.out.println(gson.toJson(update));
        System.out.println("======================");
    }
}
