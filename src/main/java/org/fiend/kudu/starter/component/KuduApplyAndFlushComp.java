package org.fiend.kudu.starter.component;

import com.google.common.collect.Lists;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.fiend.kudu.starter.config.Constants.*;

/**
 * kudu消息队列
 * @author langpf 2020-03-16 11:04:47
 */
@Component
public class KuduApplyAndFlushComp {
    Logger log = LoggerFactory.getLogger(getClass());
    private long lastConsumeTime = 0;

    @Autowired
    KuduSession kuduSession;

    public void addKuduOperation(Operation operation) {
        //向队列中插入消息
        try {
            KUDU_OPERATION_QUEUE.put(operation);
        } catch (InterruptedException e) {
            log.warn(KUDU_OPERATION_QUEUE + " 队列保存数据异常! paramJson: ", e);
        }
    }

    public void addKuduOperations(List<Operation> operations) throws InterruptedException {
        DATA_SIZE += operations.size();

        //向队列中插入消息
        for (Operation operation : operations) {
            KUDU_OPERATION_QUEUE.put(operation);
        }
    }

    /**
     * 消费队列数据
     *  每 MAX_INTERVAL_FLUSH_MILLISECOND 时间 或者 size > MAX_KUDU_OPERATION_SIZE 消费一次
     */
    public void consumeKuduOperation() throws KuduException {
        long duration = System.currentTimeMillis() - lastConsumeTime;
        int size = KUDU_OPERATION_QUEUE.size();
        if ((size == 0) || ((size < MAX_KUDU_OPERATION_SIZE) && (duration < MAX_INTERVAL_FLUSH_MILLISECOND))) {
            log.debug("放弃消费kuduOperation, KUDU_OPERATION_QUEUE size: {}, 距离上次消费时间: {}ms",
                    size, duration);
            return;
        }

        if (START_TIME == 0) {
            START_TIME = System.currentTimeMillis();
        }

        List<Operation> operationList = Lists.newArrayList();
        KUDU_OPERATION_QUEUE.drainTo(operationList, MAX_KUDU_OPERATION_SIZE);

        long start = System.currentTimeMillis();
        for (Operation operation : operationList) {
            try {
                kuduSession.apply(operation);
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }

        printResponse(kuduSession.flush());
        long totalTime = System.currentTimeMillis() - start;
        log.info("kudu 请求条数={}条，处理时间={} ms", operationList.size(), totalTime);

        if (KUDU_OPERATION_QUEUE.size() == 0) {
            long allTime = System.currentTimeMillis() - START_TIME;
            log.info("======================// 本次异步处理数据, 共处理数据={}条, 共用时={} ms", DATA_SIZE, allTime);
            START_TIME = 0;
            DATA_SIZE = 0;
        }
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
}
