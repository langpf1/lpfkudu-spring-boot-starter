package org.fiend.kudu.starter.template.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.fiend.kudu.starter.component.KuduApplyAndFlushComp;
import org.fiend.kudu.starter.config.CondType;
import org.fiend.kudu.starter.config.kudu.KuduProperties;
import org.fiend.kudu.starter.config.kudu.IKuduOperation;
import org.fiend.kudu.starter.config.kudu.KuduConnPool;
import org.fiend.kudu.starter.config.kudu.OperationEnum;
import org.fiend.kudu.starter.config.kudu.impl.DeleteOperation;
import org.fiend.kudu.starter.config.kudu.impl.InsertOperation;
import org.fiend.kudu.starter.config.kudu.impl.UpdateOperation;
import org.fiend.kudu.starter.config.kudu.impl.UpsertOperation;
import org.fiend.kudu.starter.entity.ColumnCond;
import org.fiend.kudu.starter.entity.KuduColumn;
import org.fiend.kudu.starter.template.KuduTemplate;
import org.fiend.kudu.starter.utils.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.fiend.kudu.starter.config.Constants.MAX_KUDU_OPERATION_SIZE;

public class PlainKuduTemplate implements KuduTemplate {
    Logger log = LoggerFactory.getLogger(getClass());

    private final KuduClient kuduClient;
    private final KuduSession kuduSession;
    private final KuduApplyAndFlushComp kuduApplyAndFlushComp;
    KuduProperties kuduProperties;

    private final IdGenerator idGenerator;
    private static Map<String, KuduTable> tables;

    private final KuduConnPool kuduConnPool;

    public PlainKuduTemplate(KuduClient kuduClient,
                             KuduSession kuduSession,
                             KuduConnPool kuduConnPool,
                             KuduApplyAndFlushComp kuduApplyAndFlushComp,
                             KuduProperties kuduProperties) {
        this.kuduClient = kuduClient;
        this.kuduProperties = kuduProperties;
        this.kuduSession  = kuduSession;
        this.kuduConnPool = kuduConnPool;
        this.kuduApplyAndFlushComp = kuduApplyAndFlushComp;

        // 初始化 id 生成器
        tables = new HashMap<>();
        Long wordId = kuduProperties.getWorkerId();
        log.info("默认workId ={}, 本节点的 workId = {}", KuduProperties.DEFAULT_WORK_ID, wordId);
        idGenerator = new IdGenerator(wordId);
    }

    /**
     * @return 不重复的 Long 类型 id
     */
    @Override
    public long getId() {
        return idGenerator.nextId();
    }

    @Override
    public KuduProperties getProperties() {
        return kuduProperties;
    }

    /* ==================================== 表操作  ==================================== */
    @Override
    public KuduTable getTable(String tableName) throws KuduException {
        KuduTable ktable = tables.get(tableName);
        if (null == ktable) {
            ktable = kuduClient.openTable(tableName);
            tables.put(tableName, ktable);
        }

        return ktable;
    }

    /**
     * 获取table列表, 获取kudu中所有的表
     * 数据库是Impala的定义，kudu无数据库定义, 只有表的定义
     * @return list
     */
    @Override
    public List<String> getTablesList() {
        try {
            return kuduClient.getTablesList().getTablesList();
        } catch (KuduException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * @param tableName      tableName
     * @param kuduColumnList column
     * @param buckets        分区数目
     * @param copies         副本数
     * @throws KuduException e
     */
    @Override
    public boolean createTable(String tableName,
                               List<KuduColumn> kuduColumnList,
                               int buckets,
                               int copies) throws KuduException {
        if (kuduClient.tableExists(tableName)) {
            log.warn("表" + tableName + "已存在, 放弃创建表。");
            return false;
        }

        // 2、创建schema信息
        List<ColumnSchema> columns = Lists.newArrayList();

        // 3、指定分区字段
        List<String> partitions = Lists.newArrayList();

        kuduColumnList.forEach(x -> {
            // columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).nullable(false).build());
            columns.add(
                    new ColumnSchema
                            .ColumnSchemaBuilder(x.getName(), x.getType())
                            .key(x.isKey())
                            .nullable(x.isNullable())
                            .build()
            );

            if (x.isPartitionKey()) {
                partitions.add(x.getName());
            }
        });
        Schema schema = new Schema(columns);

        //4、指定分区方式
        CreateTableOptions options = new CreateTableOptions()
                .addHashPartitions(partitions, buckets)
                .setNumReplicas(copies);

        //5、创建表，
        kuduClient.createTable(tableName, schema, options);

        return true;
    }

    @Override
    public void deleteTable(String tableName) throws KuduException {
        if (!kuduClient.tableExists(tableName)) {
            return;
        }
        kuduClient.deleteTable(tableName);
    }

    /**
     * 添加空列
     * @param tableName table name
     * @param colName   column name
     * @param type      column type
     * @throws KuduException        e
     * @throws InterruptedException i
     */
    @Override
    public boolean addNullableColumn(String tableName, String colName, Type type) throws KuduException, InterruptedException {
        if (existColumn(tableName, colName)) {
            log.error("表{}中的columnName: {}已存在!", tableName, colName);
            return false;
        }

        AlterTableOptions ato = new AlterTableOptions();
        ato.addNullableColumn(colName, type);
        kuduClient.alterTable(tableName, ato);
        flushTables(tableName);

        int totalTime = 0;
        while (!kuduClient.isAlterTableDone(tableName)) {
            Thread.sleep(200);
            if (totalTime > 20000) {
                log.warn("Alter table is Not Done!");
                return false;
            }
            totalTime += 200;
        }

        return true;
    }

    @Override
    public boolean addDefaultValColumn(String tableName,
                                       String colName,
                                       Type type,
                                       Object defaultVal) throws KuduException, InterruptedException {
        if (existColumn(tableName, colName)) {
            log.error("表{}中的columnName: {}已存在!", tableName, colName);
            return false;
        }

        AlterTableOptions ato = new AlterTableOptions();
        ato.addColumn(colName, type, defaultVal);
        kuduClient.alterTable(tableName, ato);
        flushTables(tableName);

        int totalTime = 0;
        while (!kuduClient.isAlterTableDone(tableName)) {
            Thread.sleep(200);
            if (totalTime > 20000) {
                log.warn("Alter table is Not Done!");
                return false;
            }
            totalTime += 200;
        }

        return true;
    }

    @Override
    public boolean existColumn(String tableName, String colName) throws KuduException {
        flushTables(tableName);
        KuduTable ktable = getTable(tableName);

        Schema schema = ktable.getSchema();
        List<ColumnSchema> columns = schema.getColumns();
        for (ColumnSchema c : columns) {
            if (c.getName().equals(colName)) {
                return true;
            }
        }

        return false;
    }

    /* ============================ 单条数据操作 ============================ */
    @Override
    public void insert(String tableName, JSONObject data) throws KuduException {
        Insert insert = createInsert(tableName, data);
        applyAndFlush(insert);
    }

    @Override
    public void upsert(String tableName, JSONObject data) throws KuduException {
        Upsert upsert = createUpsert(tableName, data);
        applyAndFlush(upsert);
    }

    @Override
    public void update(String tableName, JSONObject data) throws KuduException {
        Update update = createUpdate(tableName, data);
        applyAndFlush(update);
    }

    /**
     * @param tableName        表名
     * @param selectColumnList 查询字段名 为空时返回全部字段
     * @param columnCondList   条件列 可为空
     * @param size             size为空时该条件失效
     * @return data
     * @throws KuduException k
     */
    @Override
    public List<JSONObject> query(String tableName,
                                  List<String> selectColumnList,
                                  List<ColumnCond> columnCondList,
                                  Integer size) throws KuduException {
        List<JSONObject> dataList = Lists.newArrayList();

        KuduTable ktable = getTable(tableName);
        if (null == ktable) {
            return null;
        }

        if (null == selectColumnList) {
            selectColumnList = Lists.newArrayList();
        }
        if (selectColumnList.size() < 1) {
            selectColumnList = getColumnList(ktable);
        }

        KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient.newScannerBuilder(ktable);
        kuduScannerBuilder.setProjectedColumnNames(selectColumnList);

        if ((null != columnCondList) && (columnCondList.size() > 0)) {
            KuduPredicate predicate;
            for (ColumnCond cond : columnCondList) {
                predicate = getKuduPredicate(ktable, cond);
                if (null != predicate) {
                    kuduScannerBuilder.addPredicate(predicate);
                }
            }
        }

        KuduScanner scanner = kuduScannerBuilder.build();

        outer:
        while (scanner.hasMoreRows()) {
            RowResultIterator rows = scanner.nextRows();

            int numRows = rows.getNumRows();
            if ((numRows > 10000) && isNull(size)) {
                log.error("查询数据条数: {}, 大于10000条, 数据量过载!", numRows);
                break;
            }

            while (rows.hasNext()) {
                dataList.add(getRowData(rows.next(), selectColumnList));

                if (dataList.size() >= 10000) {
                    log.error("查询数据条数: {}, 大于10000条, 数据量过载!", dataList.size());
                    break outer;
                }

                if (nonNull(size) && (dataList.size() >= size)) {
                    break outer;
                }
            }
        }

        return dataList;
    }

    @Override
    public void delete(String tableName, JSONObject data) throws KuduException {
        Delete delete = createDelete(tableName, data);
        applyAndFlush(delete);
    }

    /* ============================ 多条数据操作--同步 ============================ */
    @Override
    public void batchInsert(String tableName, List<JSONObject> dataList) throws KuduException {
        handleOpera(tableName, dataList, OperationEnum.INSERT);
    }

    @Override
    public void batchUpsert(String tableName, List<JSONObject> dataList) throws KuduException {
        handleOpera(tableName, dataList, OperationEnum.UPSERT);
    }

    @Override
    public void batchUpdate(String tableName, List<JSONObject> dataList) throws KuduException {
        handleOpera(tableName, dataList, OperationEnum.UPDATE);
    }

    @Override
    public void batchDelete(String tableName, List<JSONObject> dataList) throws KuduException {
        handleOpera(tableName, dataList, OperationEnum.DELETE);
    }

    private void handleOpera(String tableName, List<JSONObject> dataList, OperationEnum opEnum) throws KuduException {
        if (kuduConnPool.getTokenFail()) {
            throw new RuntimeException("====================> 限流, 令牌已用完! <==================");
        }

        if (dataList.isEmpty()) {
            return;
        }

        KuduTable ktable = getTable(tableName);
        IKuduOperation kuduOperation = getKuduOperation(ktable, opEnum);

        int index = 0;
        long start = System.currentTimeMillis();

        Operation operation;
        PartialRow row;
        Schema schema;
        for (JSONObject data : dataList) {
            operation = kuduOperation.getOperation();
            row = operation.getRow();

            schema = ktable.getSchema();
            for (String colName : data.keySet()) {
                fillRow(row, schema, colName, data);
            }

            kuduSession.apply(operation);

            if (++index % KuduConnPool.CURRENT_OPERATION_PARALLEL_SIZE == 0) {
                printResponse(kuduSession.flush());
            }
        }

        printResponse(kuduSession.flush());
        log.info("kudu 请求条数={}条，处理时间={} ms", dataList.size(), System.currentTimeMillis() - start);

        kuduConnPool.returnToken();
    }

    /* ============================ 多条数据操作--异步 ============================ */
    @Override
    public void batchInsertAsyn(String tableName, List<JSONObject> dataList) throws KuduException, InterruptedException {
        handleOperaAsyn(tableName, dataList, OperationEnum.INSERT);
    }

    @Override
    public void batchUpsertAsyn(String tableName, List<JSONObject> dataList) throws KuduException, InterruptedException {
        handleOperaAsyn(tableName, dataList, OperationEnum.UPSERT);
    }

    @Override
    public void batchUpdateAsyn(String tableName, List<JSONObject> dataList) throws KuduException, InterruptedException {
        handleOperaAsyn(tableName, dataList, OperationEnum.UPDATE);
    }

    @Override
    public void batchDeleteAsyn(String tableName, List<JSONObject> dataList) throws KuduException, InterruptedException {
        KuduTable ktable = getTable(tableName);

        List<Operation> operations = Lists.newArrayList();

        Delete delete;
        PartialRow row;
        Schema schema;
        for (JSONObject data : dataList) {
            delete = ktable.newDelete();
            row = delete.getRow();

            schema = ktable.getSchema();
            for (String colName : data.keySet()) {
                fillRow(row, schema, colName, data);
            }

            operations.add(delete);
        }

        kuduApplyAndFlushComp.addKuduOperations(operations);
    }

    private void handleOperaAsyn(String tableName, List<JSONObject> dataList, OperationEnum oEnum) throws KuduException, InterruptedException {
        KuduTable ktable = getTable(tableName);
        List<Operation> operations = Lists.newArrayList();
        IKuduOperation kuduOperation = getKuduOperation(ktable, oEnum);

        Operation operation;
        PartialRow row;
        Schema schema;
        for (JSONObject data : dataList) {
            operation = kuduOperation.getOperation();
            row = operation.getRow();

            schema = ktable.getSchema();
            for (String colName : data.keySet()) {
                fillRow(row, schema, colName, data);
            }

            operations.add(operation);
        }

        kuduApplyAndFlushComp.addKuduOperations(operations);
    }

    private IKuduOperation getKuduOperation(KuduTable ktable, OperationEnum oEnum) {
        IKuduOperation kuduOperation = null;
        switch (oEnum) {
            case INSERT:
                kuduOperation = new InsertOperation(ktable);
                break;
            case UPSERT:
                kuduOperation = new UpsertOperation(ktable);
                break;
            case UPDATE:
                kuduOperation = new UpdateOperation(ktable);
                break;
            case DELETE:
                kuduOperation = new DeleteOperation(ktable);
                break;
            default:
                break;
        }

        return kuduOperation;
    }

    @Override
    public Insert createInsert(String tableName, JSONObject data) throws KuduException {
        KuduTable ktable = getTable(tableName);

        Insert insert = ktable.newInsert();
        PartialRow row = insert.getRow();

        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            fillRow(row, schema, colName, data);
        }

        return insert;
    }

    @Override
    public Insert createEmptyInsert(String tableName) throws KuduException {
        KuduTable ktable = getTable(tableName);
        return ktable.newInsert();
    }

    /**
     * @param tableName t
     * @param data      必须包含 主键 和 not null 字段
     * @return u
     * @throws KuduException e
     */
    @Override
    public Upsert createUpsert(String tableName, JSONObject data) throws KuduException {
        KuduTable ktable = getTable(tableName);

        Upsert upsert = ktable.newUpsert();
        PartialRow row = upsert.getRow();
        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            fillRow(row, schema, colName, data);
        }

        return upsert;
    }

    @Override
    public Upsert createEmptyUpsert(String tableName) throws KuduException {
        KuduTable ktable = getTable(tableName);
        return ktable.newUpsert();
    }

    /**
     * 构建并返回一个删除操作，一般用于批量 apply
     * @param tableName t
     * @param data      一定要有主键字段
     * @return r
     * @throws KuduException e
     */
    @Override
    public Update createUpdate(String tableName, JSONObject data) throws KuduException {
        KuduTable ktable = getTable(tableName);

        Update update = ktable.newUpdate();
        PartialRow row = update.getRow();
        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            fillRow(row, schema, colName, data);
        }

        return update;
    }

    @Override
    public Update createEmptyUpdate(String tableName) throws KuduException {
        KuduTable ktable = getTable(tableName);
        return ktable.newUpdate();
    }

    /**
     * 构建删除
     * @param tableName t
     * @param data      <column,value> 删除条件 必须要有主键，存在其他条件的时候 and 关系，不符合就不删除
     * @return 1
     * @throws KuduException k
     */
    @Override
    public Delete createDelete(String tableName, JSONObject data) throws KuduException {
        KuduTable ktable = getTable(tableName);
        Delete delete = ktable.newDelete();
        PartialRow row = delete.getRow();

        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            fillRow(row, schema, colName, data);
        }

        return delete;
    }

    @Override
    public Delete createEmptyDelete(String tableName) throws KuduException {
        KuduTable ktable = getTable(tableName);
        return ktable.newDelete();
    }

    @Override
    public KuduScanner.KuduScannerBuilder scannerBuilder(String tableName) throws KuduException {
        return kuduClient.newScannerBuilder(getTable(tableName));
    }

    /**
     * 单条提交
     * @param operation op
     * @throws KuduException e
     */
    @Override
    public void applyAndFlush(Operation operation) throws KuduException {
        this.applyAndFlush(Collections.singletonList(operation));
    }

    /**
     * 批量提交
     * @param operations op
     * @throws KuduException e
     */
    @Override
    public void applyAndFlush(List<Operation> operations) throws KuduException {
        long start = System.currentTimeMillis();
        int index = 0;
        for (Operation operation : operations) {
            kuduSession.apply(operation);
            if (++index % MAX_KUDU_OPERATION_SIZE == 0) {
                printResponse(kuduSession.flush());
            }
        }

        printResponse(kuduSession.flush());
        log.info("kudu 请求条数={}条，处理时间={} ms", operations.size(), System.currentTimeMillis() - start);
    }

    /**
     * 打印失败的结果
     */
    private void printResponse(List<OperationResponse> responses) {
        if (responses == null || responses.isEmpty()) {
            return;
        }

        List<RowError> rowErrors = OperationResponse.collectErrors(responses);
        if (!rowErrors.isEmpty()) {
            log.error("kudu {}条操作请求失败！tips={}", rowErrors.size(), rowErrors);
        }
    }

    /* ============================ private ==============================*/
    public void flushTables(String tableName) throws KuduException {
        tables.put(tableName, kuduClient.openTable(tableName));
    }

    private static void fillRow(PartialRow row, Schema schema, String colName, JSONObject data) {
        ColumnSchema colSchema = schema.getColumn(colName);
        if (data.get(colName) == null) {
            return;
        }
        Type type = colSchema.getType();
        switch (type) {
            case STRING:
                row.addString(colName, data.getString(colName));
                break;
            case INT64:
                row.addLong(colName, data.getLongValue(colName));
                break;
            case UNIXTIME_MICROS:
                row.addTimestamp(colName, data.getTimestamp(colName));
                break;
            case DOUBLE:
                row.addDouble(colName, data.getDoubleValue(colName));
                break;
            case INT32:
                row.addInt(colName, data.getIntValue(colName));
                break;
            case INT16:
                row.addShort(colName, data.getShortValue(colName));
                break;
            case INT8:
                row.addByte(colName, data.getByteValue(colName));
                break;
            case BOOL:
                row.addBoolean(colName, data.getBooleanValue(colName));
                break;
            case BINARY:
                row.addBinary(colName, data.getBytes(colName));
                break;
            case FLOAT:
                row.addFloat(colName, data.getFloatValue(colName));
                break;
            default:
                break;
        }
    }

    private JSONObject getRowData(RowResult row, List<String> selectColumnList) {
        JSONObject dataJson = new JSONObject();

        selectColumnList.forEach(colName -> {
            if (row.isNull(colName)) {
                dataJson.put(colName, null);
                return;
            }

            Type type = row.getColumnType(colName);
            switch (type) {
                case STRING:
                    dataJson.put(colName, row.getString(colName));
                    break;
                case INT64:
                    dataJson.put(colName, row.getLong(colName));
                case UNIXTIME_MICROS:
                    dataJson.put(colName, row.getTimestamp(colName));
                    break;
                case DOUBLE:
                    dataJson.put(colName, row.getDouble(colName));
                    break;
                case FLOAT:
                    dataJson.put(colName, row.getFloat(colName));
                    break;
                case INT32:
                case INT16:
                case INT8:
                    dataJson.put(colName, row.getInt(colName));
                    break;
                case BOOL:
                    dataJson.put(colName, row.getBoolean(colName));
                    break;
                case BINARY:
                    dataJson.put(colName, row.getBinary(colName));
                    break;

                default:
                    break;
            }
            ;
        });

        return dataJson;
    }

    private KuduPredicate getKuduPredicate(KuduTable ktable, ColumnCond cond) {
        String colName = cond.getColName();
        KuduPredicate.ComparisonOp op = cond.getOp();
        Object objVal = cond.getValue();

        if (objVal == CondType.IS_NULL) {
            return KuduPredicate.newIsNullPredicate(ktable.getSchema().getColumn(colName));
        }
        if (objVal == CondType.IS_NOT_NULL) {
            return KuduPredicate.newIsNotNullPredicate(ktable.getSchema().getColumn(colName));
        }
        if (objVal instanceof Boolean) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op,
                    (boolean) cond.getValue());
        }
        if (objVal instanceof Integer) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op, ((Integer)cond.getValue()).longValue());
        }
        if (objVal instanceof Long) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op, cond.getValue());
        }
        if (objVal instanceof Short) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op, ((Short) cond.getValue()).longValue());
        }
        if (objVal instanceof Byte) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op, ((Byte) cond.getValue()).longValue());
        }
        if (objVal instanceof Float) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op,
                    (float) cond.getValue());
        }
        if (objVal instanceof Double) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op,
                    (double) cond.getValue());
        }
        if (objVal instanceof String) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op,
                    (String) cond.getValue());
        }
        if (objVal instanceof byte[]) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op,
                    (byte[]) cond.getValue());
        }
        if (objVal instanceof Timestamp) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op,
                    (Timestamp) cond.getValue());
        }
        if (objVal instanceof BigDecimal) {
            return KuduPredicate.newComparisonPredicate(
                    ktable.getSchema().getColumn(colName),
                    op,
                    (BigDecimal) cond.getValue());
        }

        return null;
    }

    private List<String> getColumnList(KuduTable ktable) {
        List<String> columns = Lists.newArrayList();

        List<ColumnSchema> columnSchemaList = ktable.getSchema().getColumns();
        columnSchemaList.forEach(x -> {
            columns.add(x.getName());
        });

        return columns;
    }
}
