package org.fiend.kudu.starter.template;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.fiend.kudu.starter.KuduTestApplication;
import org.fiend.kudu.starter.template.entity.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = KuduTestApplication.class)
public class TemplateTest {
    Logger log = LoggerFactory.getLogger(this.getClass());

    public static final String TABLE_NAME = "user";

    /**
     * 用于操作 kudu 原生的表
     * 当然可以直接注入 KuduClient 和 KuduSession 来使用原生 api
     */
    @Autowired
    KuduTemplate kuduTemplate;

    @Autowired
    KuduClient kuduClient;


    /* ============================ kuduTemplate =============================*/
    public void insertDefaultDB(User user) throws KuduException {
        kuduTemplate.insert(TABLE_NAME, JSON.parseObject(JSONObject.toJSONString(user)));
    }

    public List<String> getTableList() {
        return kuduTemplate.getTablesList();
    }

    /* ============================= kuduClient ==============================*/
    @Test
    public void createTable() throws KuduException {
        List<ColumnSchema> columns = new LinkedList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("Clo1", Type.INT64).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("Clo2", Type.INT64).build());
        Schema schema = new Schema(columns);

        CreateTableOptions createTableOptions = new CreateTableOptions();
        createTableOptions.setRangePartitionColumns(Collections.singletonList("Clo1"));

        KuduTable kuduTable = kuduClient.createTable("kuduTable", schema, createTableOptions);
    }

    @Test
    public void getTables() throws KuduException {
        kuduClient.getTablesList().getTablesList().forEach(s -> System.out.println(s));
    }

    @Test
    public void getTable() throws KuduException {
        KuduTable kuduTable = kuduClient.openTable("kuduTable");
        kuduTable.getSchema().getColumns().forEach(s -> log.info("clo={}", s));

    }

    @Test
    public void getCol() throws KuduException {
        KuduTable kuduTable = kuduClient.openTable("kuduTable");
        ColumnSchema clo1 = kuduTable.getSchema().getColumn("Clo1");
        log.info("col={}", clo1);
    }

    @Test
    public void delTable() throws KuduException {
        log.info("{}", kuduClient.deleteTable("wgj.apiTest"));
    }

    @Test
    public void scan() throws KuduException {
        KuduTable table = kuduClient.openTable("impala::test.user");
        KuduScanner.KuduScannerBuilder scanner = kuduClient.newScannerBuilder(table);

        //指定返回字段
        List<String> returnColumns = new ArrayList<>();

        for (ColumnSchema columnSchema : table.getSchema().getColumns()) {
            returnColumns.add(columnSchema.getName());
        }

        //组装条件
        KuduPredicate predicate = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("name"),
                KuduPredicate.ComparisonOp.EQUAL, "jason");
        scanner.addPredicate(predicate);
        System.out.println(KuduPredicate.ComparisonOp.EQUAL);

        KuduScanner build = scanner.build();
        while (build.hasMoreRows()) {
            RowResultIterator results = build.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                log.info("{}", result);
                System.out.println(result.getLong(0));
            }
        }
    }
}