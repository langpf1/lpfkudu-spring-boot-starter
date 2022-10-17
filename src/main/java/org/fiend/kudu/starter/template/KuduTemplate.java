package org.fiend.kudu.starter.template;

import com.alibaba.fastjson.JSONObject;
import org.fiend.kudu.starter.config.kudu.KuduProperties;
import org.fiend.kudu.starter.entity.ColumnCond;
import org.fiend.kudu.starter.entity.KuduColumn;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.List;

public interface KuduTemplate {
    long getId();

    KuduProperties getProperties();

    void applyAndFlush(Operation operation) throws KuduException;

    void applyAndFlush(List<Operation> operations) throws KuduException;

    /* ============================ 表操作 ============================ */
    KuduTable getTable(String tableName) throws KuduException;

    List<String> getTablesList();

    boolean createTable(String tableName, List<KuduColumn> kuduColumnList, int buckets, int copies) throws KuduException;

    void deleteTable(String tableName) throws KuduException;

    boolean addNullableColumn(String tableName, String colName, Type type) throws KuduException, InterruptedException;

    boolean addDefaultValColumn(String tableName, String colName, Type type, Object defaultVal) throws KuduException, InterruptedException;

    boolean existColumn(String tableName, String colName) throws KuduException;

    /* ============================ 单条数据操作 ============================ */
    void insert(String tableName, JSONObject data) throws KuduException;

    void upsert(String tableName, JSONObject data) throws KuduException;

    void update(String tableName, JSONObject data) throws KuduException;

    List<JSONObject> query(String tableName,
                           List<String> selectColumnList,
                           List<ColumnCond> columnCondList,
                           Integer size) throws KuduException;

    void delete(String tableName, JSONObject data) throws KuduException;

    /* ============================ 多条数据操作-同步 ============================ */
    void batchInsert(String tableName, List<JSONObject> dataList) throws KuduException;

    void batchUpsert(String tableName, List<JSONObject> dataList) throws KuduException;

    void batchUpdate(String tableName, List<JSONObject> dataList) throws KuduException;

    void batchDelete(String tableName, List<JSONObject> dataList) throws KuduException;

    /* ============================ 同步步多条数据操作-异步 ============================ */
    void batchInsertAsyn(String tableName, List<JSONObject> dataList) throws KuduException, InterruptedException;

    void batchUpsertAsyn(String tableName, List<JSONObject> dataList) throws KuduException, InterruptedException;

    void batchUpdateAsyn(String tableName, List<JSONObject> dataList) throws KuduException, InterruptedException;

    void batchDeleteAsyn(String tableName, List<JSONObject> dataList) throws KuduException, InterruptedException;

    /* =================== return Operation 用于批量操作(apply) ==================== */
    Insert createInsert(String tableName, JSONObject data) throws KuduException;

    Insert createEmptyInsert(String tableName) throws KuduException;

    Upsert createUpsert(String tableName, JSONObject data) throws KuduException;

    Upsert createEmptyUpsert(String tableName) throws KuduException;

    Update createUpdate(String tableName, JSONObject data) throws KuduException;

    Update createEmptyUpdate(String tableName) throws KuduException;

    Delete createDelete(String tableName, JSONObject data) throws KuduException;

    Delete createEmptyDelete(String tableName) throws KuduException;

    KuduScanner.KuduScannerBuilder scannerBuilder(String tableName) throws KuduException;
}
