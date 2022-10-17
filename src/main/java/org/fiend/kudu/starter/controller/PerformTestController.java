package org.fiend.kudu.starter.controller;

import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.client.KuduException;
import org.fiend.kudu.starter.service.PerformTestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author langpf 2020-05-14 19:54:30
 */
@RestController
@RequestMapping("perform-test")
public class PerformTestController {
    Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    PerformTestService performTestService;

    /* ============================ 多条数据操作-同步 ============================ */
    @RequestMapping("batch-insert")
    private JSONObject batchInsert(int nThreads, int dataNum) throws Exception {
        return performTestService.batchInsert(nThreads, dataNum);
    }

    @RequestMapping("batch-query")
    private JSONObject batchQuery(int nThreads, int dataNum) throws Exception {
        return performTestService.batchQuery(nThreads, dataNum);
    }

    @RequestMapping("batch-upsert")
    private JSONObject batchUpsert() throws KuduException {
        return performTestService.batchUpsert();
    }

    @RequestMapping("batch-update")
    private JSONObject batchUpdate() throws KuduException {
        return performTestService.batchUpdate();
    }

    /* ============================ 多条数据操作-异步 ============================ */
    @RequestMapping("batch-insert-asyn")
    private JSONObject batchInsertAsyn(int nThreads, int dataNum) throws Exception {
        return performTestService.batchInsertAsyn(nThreads, dataNum);
    }

    @RequestMapping("batch-upsert-asyn")
    private JSONObject batchUpsertAsyn() throws KuduException {
        return performTestService.batchUpsertAsyn();
    }

    @RequestMapping("batch-update-asyn")
    private JSONObject batchUpdateAsyn() throws KuduException {
        return performTestService.batchUpdateAsyn();
    }

    /* ========================================================================= */
    @RequestMapping("query")
    private JSONObject query() throws KuduException {
        return performTestService.query();
    }

    @RequestMapping("delete")
    private JSONObject delete() throws KuduException {
        return performTestService.delete();
    }
}
