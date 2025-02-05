package com.hl.bigdata.flink.sql.java;

import com.hl.bigdata.flink.sql.java.udf.*;
import org.apache.flink.table.api.*;
import org.junit.Test;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author huanglin
 * @date 2024/11/12 21:34
 */
public class DoMain {

    /**
     * -- flink操作记录变更标号
     * +I：表示插入操作。
     * -D：表示删除操作。
     * +U：表示更新操作，更新后的记录。-U：表示更新操作，更新前的记录
     */

    final String tempTableSql = "create table myTable ( id INT, name STRING, age INT, sex STRING, num INT, price BIGINT)" +
            " WITH ('connector'='jdbc', " +
            " 'url' = 'jdbc:mysql://127.0.0.1:3306/flink_test', " +
            " 'table-name' = 'mytable_copy1'," +
            " 'username' = 'root'," +
            " 'password' = 'root')";

    final String sinkTableSql = "create table sinkTable ( sex STRING, udagg BIGINT, PRIMARY KEY (sex) not ENFORCED)" +
            " WITH ('connector'='jdbc', " +
            " 'url' = 'jdbc:mysql://127.0.0.1:3306/flink_test?useSSL=false', " +
            " 'table-name' = 'mytable_result'," +
            " 'username' = 'root'," +
            " 'password' = 'root')";

    /**
     * udf 标量函数测试
     */
    @Test
    public void scalarFunction() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setIdleStateRetention(Duration.ofDays(1L));
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        tableEnvironment.executeSql(tempTableSql);

        tableEnvironment.executeSql("select * from myTable;").print();
        // udf使用函数--方式一
        tableEnvironment.from("myTable")
                .select(call(ScalarQuantityFunction.class, $("id")))
                .execute().print();
        // 注册udf
        tableEnvironment.createTemporaryFunction("ScalarQuantityFunction", ScalarQuantityFunction.class);
        tableEnvironment.from("myTable")
                .select(call("ScalarQuantityFunction", $("name")))
                .execute().print();
        tableEnvironment.sqlQuery("select ScalarQuantityFunction(age) from myTable")
                .execute().print();
    }

    /**
     * udf 表值函数测试
     *
     * @throws Exception
     */
    @Test
    public void tabularValueFunction() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setIdleStateRetention(Duration.ofDays(1L));
        TableEnvironment env = TableEnvironment.create(settings);

        env.executeSql(tempTableSql);

        /*
         * leftOuterJoinLateral 是 Apache Flink 中用于表操作的一个方法，它结合了 LEFT OUTER JOIN 和 LATERAL 的功能。
         * 这个方法允许你在左表的每一行上应用一个表值函数（Table-valued Function），并将结果与左表进行左外连接。
         */
        env.from("myTable")
                .leftOuterJoinLateral(call(TabularValueFunction.class, $("name")))
                .select($("id"), $("name"), $("word"), $("length"))
                .execute().print();
        /*
         * joinLateral 是另一个表操作方法，它与 leftOuterJoinLateral 的主要区别在于，它不返回左表中没有匹配项的行。
         */
        env.from("myTable")
                .joinLateral(call(TabularValueFunction.class, $("name")))
                .select($("id"), $("name"), $("word"), $("length"))
                .execute().print();

        env.createTemporaryFunction("TabularValueFunction", TabularValueFunction.class);

        env.from("myTable")
                .leftOuterJoinLateral(call("TabularValueFunction", $("name")))
                .select($("id"), $("name"), $("word"), $("length"))
                .execute().print();
        env.from("myTable")
                .joinLateral(call("TabularValueFunction", $("name")))
                .select($("id"), $("name"), $("word"), $("length"))
                .execute().print();

        env.sqlQuery("select name, word, length from myTable, lateral table(TabularValueFunction(name))")
                .execute().print();
        env.sqlQuery("select name, word, length from myTable left join lateral table(TabularValueFunction(name)) on true")
                .execute().print();
        env.sqlQuery("select name, newWord, newLength from myTable left join lateral table(TabularValueFunction(name)" +
                        ") as T(newWord, newLength) on true")
                .execute().print();
    }

    /**
     * 测试自定义聚合函数
     *
     * @throws Exception
     */
    @Test
    public void udaf() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setIdleStateRetention(Duration.ofDays(1L));
        TableEnvironment env = TableEnvironment.create(settings);

        env.executeSql(tempTableSql);
        env.executeSql(sinkTableSql);
        env.createTemporaryFunction("UDAGGFunction", UDAGGFunction.class);
        env.executeSql("create view groupTable as select sex, UDAGGFunction(coalesce(price, 0), coalesce(num, 0)) as udagg from myTable group by sex");
        env.executeSql("insert into sinkTable select * from groupTable").print();
    }

    /**
     * 表值聚合函数udf测试
     *
     * @throws Exception
     */
    @Test
    public void udtagg() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setIdleStateRetention(Duration.ofDays(1L));
        TableEnvironment env = TableEnvironment.create(settings);

        env.createTemporaryFunction("UDTAGG", UDTAGGFunction.class);
        env.executeSql(tempTableSql);
        Table table = env.sqlQuery("select * from myTable");
        table.groupBy($("sex"))
                .flatAggregate(call("UDTAGG", $("num")).as("v", "rank"))
                .select($("sex"), $("v"), $("rank")).execute().print();
    }

    /**
     * 表值聚合函数udf测试2
     *
     * @throws Exception
     */
    @Test
    public void udtagg2() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setIdleStateRetention(Duration.ofDays(1L));
        TableEnvironment env = TableEnvironment.create(settings);

        env.createTemporaryFunction("UDTAGG2", UDTAGGFunction2.class);
        env.executeSql(tempTableSql);
        Table table = env.sqlQuery("select * from myTable");
        table.groupBy($("sex"))
                .flatAggregate(call("UDTAGG2", $("num")).as("v", "rank"))
                .select($("sex"), $("v"), $("rank"))
                .execute().print();

    }
}
