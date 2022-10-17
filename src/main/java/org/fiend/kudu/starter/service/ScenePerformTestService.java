package org.fiend.kudu.starter.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.fiend.kudu.starter.template.KuduTemplate;
import org.fiend.kudu.starter.thread.SceneDeleteTask;
import org.fiend.kudu.starter.thread.SceneInsertTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.fiend.kudu.starter.config.Constants.SCENE_TABLE_NAME;

/**
 * 性能测试
 *
 * 通过hue连接impala进行建表，建表语句:
 *  --创建table
 * 		CREATE TABLE base_tag
 * 		primary key(id)
 * 		partition by hash(id) partitions 3
 * 		stored as kudu
 * 		as select * from performance_db.base_tag
 *
 * @author langpf 2020-06-22 21:29:44
 */
@Service
public class ScenePerformTestService {
    Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    KuduTemplate kuduTemplate;

    /* ============================ 多条数据操作-同步 ============================ */
    public JSONObject batchInsert(int nThreads, int dataNum) throws InterruptedException, ExecutionException {
        JSONObject json = new JSONObject();
        json.put("result", false);

        long start = System.currentTimeMillis();

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(nThreads);
        List<SceneInsertTask> tasks = Lists.newArrayList();
        for (int i = 0; i < nThreads; i++) {
            tasks.add(new SceneInsertTask(kuduTemplate, dataNum));
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

    public JSONObject batchDelete(int nThreads, int dataNum) throws InterruptedException, KuduException, ExecutionException {
        JSONObject json = new JSONObject();

        List<String> selectColumnList = Lists.newArrayList();
        selectColumnList.add("id");

        List<JSONObject> dataList;
        dataList = kuduTemplate.query(SCENE_TABLE_NAME, selectColumnList, null, nThreads * dataNum);
        if ((null == dataList) || dataList.isEmpty()) {
            json.put("result", true);
            return json;
        }

        long start = System.currentTimeMillis();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(nThreads);
        List<SceneDeleteTask> tasks = Lists.newArrayList();
        for (int i = 0; i < nThreads; i++) {
            if ((i * dataNum) > dataList.size()) {
                break;
            }
            if (((i + 1) * dataNum) > dataList.size()) {
                tasks.add(new SceneDeleteTask(kuduTemplate, dataList.subList(i * dataNum, dataList.size())));
            } else {
                tasks.add(new SceneDeleteTask(kuduTemplate, dataList.subList(i * dataNum, (i + 1) * dataNum)));
            }

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
}
