package top.guoziyang.mydb.backend.parser.statement;

public class Show {
    public enum Target {
        TABLES,
        DATABASES
    }

    public Target target = Target.TABLES;
}
