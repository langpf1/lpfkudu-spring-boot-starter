package org.fiend.kudu.starter.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.fiend.kudu.starter.entity.ColumnCond;
import org.fiend.kudu.starter.template.KuduTemplate;
import org.fiend.kudu.starter.thread.InsertTask;
import org.fiend.kudu.starter.thread.InsertTaskAsyn;
import org.fiend.kudu.starter.thread.QueryTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 性能测试
 *
 * 通过hue连接impala进行建表，建表语句:
 *  --创建table
 * 		CREATE TABLE base_tag2
 * 		(
 * 		  id BIGINT,
 * 		  base_tag_id STRING,
 * 		  user_id BIGINT,
 * 		  name STRING,
 * 		  phone STRING,
 * 		  sex STRING,
 * 		  address STRING,
 * 		  job STRING,
 * 		  birthday BIGINT,
 * 		  PRIMARY KEY(id)
 * 		)
 * 		partition by hash(id) partitions 3
 * 		stored as kudu
 *
 * @author langpf 2020-06-22 21:29:44
 */
@Service
public class PerformTestService {
    Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    KuduTemplate kuduTemplate;

    /* ============================ 多条数据操作-同步 ============================ */
    public JSONObject batchInsert(int nThreads, int dataNum) throws InterruptedException, ExecutionException {
        JSONObject json = new JSONObject();
        json.put("result", false);

        // long start = System.currentTimeMillis();
        // List<List<JSONObject>> threadDataList = genInsetData(nThreads, dataNum);
        // long end = System.currentTimeMillis();
        // log.info("---------------> 构造数据, 共用时 {} ms <---------------", end - start);
        // start = end;
        // Thread.sleep(2000);

        long start = System.currentTimeMillis();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(nThreads);
        List<InsertTask> tasks = Lists.newArrayList();
        for (int i = 0; i < nThreads; i++) {
            tasks.add(new InsertTask(kuduTemplate, dataNum));
        }
        List<Future<Integer>> futures = fixedThreadPool.invokeAll(tasks);

        long end = System.currentTimeMillis();

        log.info("---------------> 数据写入共用时 {} ms <---------------", end - start);

        int num = 0;
        for (Future<Integer> future : futures) {
            num += future.get();
        }
        log.info("num: {}", num);

        json.put("result", true);

        return json;
    }

    private List<List<JSONObject>> genInsetData(int nThreads, int dataNum) {
        List<List<JSONObject>> threadDataList = Lists.newArrayList();

        String dataJsonStr = "{\"high_31\":\"123123\",\"high_30\":\"123123\",\"high_33\":\"123123\",\"high_32\":\"123123\",\"high_35\":\"123123\",\"high_34\":\"123123\",\"high_26\":\"123123\",\"high_25\":\"123123\",\"high_28\":\"123123\",\"high_27\":\"123123\",\"high_29\":\"123123\",\"high_1\":\"123123\",\"high_2\":\"123123\",\"high_3\":\"123123\",\"high_4\":\"123123\",\"high_5\":\"123123\",\"high_40\":\"123123\",\"high_6\":\"123123\",\"high_7\":\"123123\",\"high_8\":\"123123\",\"high_41\":\"123123\",\"high_9\":\"123123\",\"high_37\":\"123123\",\"high_36\":\"123123\",\"high_39\":\"123123\",\"high_38\":\"123123\",\"phone\":\"13337825667\",\"name\":\"郎鹏飞\",\"job\":\"研发\",\"birthday\":527529600000,\"high_11\":\"123123\",\"high_10\":\"123123\",\"high_13\":\"123123\",\"high_12\":\"123123\",\"base_tag_id\":\"user_info\",\"address\":\"沈阳市浑南区\",\"sex\":\"男\",\"high_20\":\"123123\",\"high_22\":\"123123\",\"high_21\":\"123123\",\"high_24\":\"123123\",\"high_23\":\"123123\",\"high_15\":\"123123\",\"high_14\":\"123123\",\"high_17\":\"123123\",\"high_16\":\"123123\",\"high_19\":\"123123\",\"high_18\":\"123123\"}";

        JSONObject data;
        List<JSONObject> dataList;
        for (int t = 0; t < nThreads; t++) {
            dataList = Lists.newArrayList();
            for (int i = 0; i < dataNum; i++) {
                data = JSONObject.parseObject(dataJsonStr);
                // data.put("base_tag_id", "user_info");
                // data.put("name", "郎鹏飞");
                // data.put("phone", "13337825667");
                // data.put("sex", "男");
                // data.put("address", "沈阳市浑南区");
                // data.put("job", "研发");
                // data.put("birthday", DateUtil.str2Long2("1986.09.20"));

                // for (int k = 1; k <= 41; k++) {
                //     data.put("high_" + k, "123123");
                // }

                data.put("id", kuduTemplate.getId());
                data.put("user_id", kuduTemplate.getId());

                dataList.add(data);
            }

            threadDataList.add(dataList);
        }

        return threadDataList;
    }

    public JSONObject batchQuery(int nThreads, int dataNum) throws InterruptedException, ExecutionException {
        JSONObject json = new JSONObject();
        json.put("result", false);

        long start = System.currentTimeMillis();

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(nThreads);
        List<QueryTask> tasks = Lists.newArrayList();
        for (int i = 0; i < nThreads; i++) {
            tasks.add(new QueryTask(kuduTemplate, dataNum));
        }
        List<Future<Integer>> futures = fixedThreadPool.invokeAll(tasks);

        long end = System.currentTimeMillis();

        log.info("共用时 {} ms", end - start);

        int num = 0;
        for (Future<Integer> future : futures) {
            num += future.get();
        }
        log.info("num: {}", num);

        json.put("result", true);

        return json;
    }

    public JSONObject batchUpsert() throws KuduException {
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

    public JSONObject batchUpdate() throws KuduException {
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

    /* ============================ 多条数据操作-异步 ============================ */
    public JSONObject batchInsertAsyn(int nThreads, int dataNum) throws InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(nThreads);
        List<InsertTaskAsyn> tasks = Lists.newArrayList();
        for (int i = 0; i < nThreads; i++) {
            tasks.add(new InsertTaskAsyn(kuduTemplate, dataNum));
        }
        List<Future<Integer>> futures = fixedThreadPool.invokeAll(tasks);

        long end = System.currentTimeMillis();

        log.info("共用时 {} ms", end - start);

        int num = 0;
        for (Future<Integer> future : futures) {
            num += future.get();
        }
        log.info("num: {}", num);

        JSONObject json = new JSONObject();
        json.put("result", true);

        return json;
    }

    public JSONObject batchUpsertAsyn() throws KuduException {
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

    public JSONObject batchUpdateAsyn() throws KuduException {
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

    /* ========================================================================= */
    public JSONObject query() throws KuduException {
        JSONObject json = new JSONObject();

        String tableName = "test";
        List<String> selectColumnList = Lists.newArrayList();

        List<ColumnCond> columnCondList = Lists.newArrayList();
        columnCondList.add(ColumnCond.of("money", KuduPredicate.ComparisonOp.EQUAL, 33));

        List<JSONObject> dataList = kuduTemplate.query(tableName, selectColumnList, columnCondList, 100);

        json.put("data", dataList);
        json.put("result", true);

        return json;
    }

    public JSONObject delete() throws KuduException {
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
}
