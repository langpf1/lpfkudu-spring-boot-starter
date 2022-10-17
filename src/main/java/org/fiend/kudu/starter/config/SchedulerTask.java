package org.fiend.kudu.starter.config;

import org.apache.kudu.client.KuduException;
import org.fiend.kudu.starter.component.KuduApplyAndFlushComp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static org.fiend.kudu.starter.config.Constants.SCHEDULE_INTERVAL_PERIOD;

/**
 * @author langpf 2020/2/28
 */
@Component
@EnableScheduling
public class SchedulerTask {
    Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    KuduApplyAndFlushComp kuduApplyAndFlushComp;

    @Scheduled(fixedDelay = SCHEDULE_INTERVAL_PERIOD)
    public void fixedDelayJob() throws KuduException {
        log.debug("开始执行 clinic data persistent：");
        kuduApplyAndFlushComp.consumeKuduOperation();
    }
}
