package org.fiend.kudu.starter.config.kudu;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.SessionConfiguration;
import org.fiend.kudu.starter.component.KuduApplyAndFlushComp;
import org.fiend.kudu.starter.template.KuduTemplate;
import org.fiend.kudu.starter.template.impl.PlainKuduTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class KuduClientManualConfig {
    Logger log = LoggerFactory.getLogger(this.getClass());

    @Bean
    @ConditionalOnClass(KuduSession.class)
    public KuduTemplate KuduTemplate(@Qualifier("kuduClient") KuduClient kuduClient,
                                     @Qualifier("kuduSession") KuduSession kuduSession,
                                     @Qualifier("kuduConnPool") KuduConnPool kuduConnPool,
                                     @Qualifier("kuduApplyAndFlushComp") KuduApplyAndFlushComp kuduApplyAndFlushComp,
                                     @Qualifier("kuduProperties") KuduProperties kuduProperties) {
        return new PlainKuduTemplate(kuduClient, kuduSession, kuduConnPool, kuduApplyAndFlushComp, kuduProperties);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnClass(KuduClient.class)
    public KuduSession kuduSession(@Qualifier("kuduClient") KuduClient kuduClient) {
        KuduSession kuduSession = kuduClient.newSession();

        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setFlushInterval(500);

        //缓存条数
        kuduSession.setMutationBufferSpace(10000000);

        return kuduSession;
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnClass(KuduProperties.class)
    public KuduClient kuduClient(@Qualifier("kuduProperties") KuduProperties kuduProperties) {
        List<String> masterAddr = Arrays.asList(kuduProperties.getKuduAddress().split(","));
        log.info("kudu实例化, servers:{}", masterAddr);

        //创建kudu的数据库链接
        return new KuduClient
                .KuduClientBuilder(kuduProperties.getKuduAddress())
                .defaultSocketReadTimeoutMs(60000)
                .defaultOperationTimeoutMs(30000)
                .defaultAdminOperationTimeoutMs(60000)
                // .workerCount(kuduProperties.getWorkerCount())
                .build();
    }

    @Bean
    @ConditionalOnProperty(value = "kudu.kudu-address")
    public KuduProperties kuduProperties() {
        KuduProperties kuduProperties = new KuduProperties();
        return kuduProperties;
    }
}
