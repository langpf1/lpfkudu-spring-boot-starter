package org.fiend.kudu.starter.controller;

import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.fiend.kudu.starter.config.CondType;
import org.fiend.kudu.starter.entity.ColumnCond;
import org.fiend.kudu.starter.entity.KuduColumn;
import org.fiend.kudu.starter.template.KuduTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author langpf 2020-05-14 19:54:30
 */
@RestController
@RequestMapping("test")
public class TestController {
    Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    KuduTemplate kuduTemplate;

    @RequestMapping("hi")
    public String hi() {
        return "ok";
    }

    /* ====================================  kudu操作  ==================================== */
    @RequestMapping("create-table")
    public JSONObject createTable(@RequestParam String tableName) throws KuduException {
        JSONObject json = new JSONObject();

        int buckets = 3;
        int copies = 3;

        List<KuduColumn> kuduColumnList = Lists.newArrayList();
        kuduColumnList.add(KuduColumn.of("name", Type.STRING));
        kuduColumnList.add(KuduColumn.of("age", Type.INT32));

        boolean result = kuduTemplate.createTable(tableName, kuduColumnList, buckets, copies);

        log.info("创建表{} {}", tableName, result ? "成功" : "失败");
        json.put("result", result);

        return json;
    }

    @RequestMapping("delete-table")
    public JSONObject deleteTable(@RequestParam String tableName) throws KuduException {
        JSONObject json = new JSONObject();
        kuduTemplate.deleteTable(tableName);
        json.put("result", true);

        return json;
    }

    @RequestMapping("add-nullable-column")
    public JSONObject addNullableColumn(@RequestParam String tableName,
                                        @RequestParam String colName) throws KuduException, InterruptedException {
        JSONObject json = new JSONObject();
        Type type = Type.INT32;
        boolean result = kuduTemplate.addNullableColumn(tableName, colName, type);

        json.put("result", result);

        return json;
    }

    @RequestMapping("add-column")
    private JSONObject addColumn(@RequestParam String tableName,
                                 @RequestParam String colName) throws KuduException, InterruptedException {
        JSONObject json = new JSONObject();
        Type type = Type.INT32;
        Integer defaultVal = 12;

        boolean result = kuduTemplate.addDefaultValColumn(tableName, colName, type, defaultVal);

        json.put("result", result);

        return json;
    }

    @RequestMapping("exist-column")
    private JSONObject existColumn(@RequestParam String tableName, @RequestParam String colName) throws KuduException {
        JSONObject json = new JSONObject();

        boolean result = kuduTemplate.existColumn(tableName, colName);
        log.info("表{}中的列{} {}", tableName, colName, result ? "存在" : "不存在");
        json.put("result", result);

        return json;
    }

    @RequestMapping("insert")
    private JSONObject insert() throws KuduException {
        JSONObject json = new JSONObject();

        String tableName = "test";
        JSONObject data = new JSONObject();
        data.put("name", "lpf");
        data.put("age", 12);

        kuduTemplate.insert(tableName, data);
        json.put("result", true);

        return json;
    }

    @RequestMapping("upsert")
    private JSONObject upsert() throws KuduException {
        JSONObject json = new JSONObject();

        String tableName = "test";
        JSONObject data = new JSONObject();
        data.put("name", "lpf88");
        data.put("age", 24);
        data.put("money", 36);

        kuduTemplate.upsert(tableName, data);
        json.put("result", true);

        return json;
    }

    @RequestMapping("update")
    private JSONObject update() throws KuduException {
        JSONObject json = new JSONObject();

        String tableName = "test";
        JSONObject data = new JSONObject();
        data.put("name", "lpf");
        data.put("age", 13);
        data.put("money", 1333);

        kuduTemplate.update(tableName, data);
        json.put("result", true);

        return json;
    }

    @RequestMapping("query")
    private JSONObject query() throws KuduException {
        JSONObject json = new JSONObject();

        String tableName = "test";
        List<String> selectColumnList = Lists.newArrayList();

        List<ColumnCond> columnCondList = Lists.newArrayList();
        columnCondList.add(ColumnCond.of("money", KuduPredicate.ComparisonOp.EQUAL, 33));

        // is null 查询
        columnCondList.add(ColumnCond.of("age", KuduPredicate.ComparisonOp.EQUAL, CondType.IS_NULL));

        // is not null 查询
        columnCondList.add(ColumnCond.of("count", KuduPredicate.ComparisonOp.EQUAL, CondType.IS_NOT_NULL));

        List<JSONObject> dataList = kuduTemplate.query(tableName, selectColumnList, columnCondList, 10);

        json.put("data", dataList);
        json.put("result", true);

        return json;
    }

    @RequestMapping("delete")
    private JSONObject delete() throws KuduException {
        JSONObject json = new JSONObject();

        String tableName = "test";
        JSONObject data = new JSONObject();
        data.put("name", "lpf2");
        data.put("age", 23);
        data.put("money", 1333);

        kuduTemplate.delete(tableName, data);
        json.put("result", true);

        return json;
    }
    /* ================================  impala kudu 操作  ================================ */
}
