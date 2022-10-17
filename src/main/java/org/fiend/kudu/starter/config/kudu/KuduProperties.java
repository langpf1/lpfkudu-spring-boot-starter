package org.fiend.kudu.starter.config.kudu;

import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.NotNull;

@ConfigurationProperties(prefix = "kudu")
public class KuduProperties {
    public static final Long DEFAULT_WORK_ID = 35L;

    @NotNull
    private String kuduAddress;

    private Long workerId = DEFAULT_WORK_ID;

    // 对于操作 impala 创建的 kudu 表，存在 DB 概念
    private String defaultDataBase;

    public String getKuduAddress() {
        return kuduAddress;
    }

    public void setKuduAddress(String kuduAddress) {
        this.kuduAddress = kuduAddress;
    }

    public Long getWorkerId() {
        return workerId;
    }

    public void setWorkerId(Long workerId) {
        this.workerId = workerId;
    }

    public String getDefaultDataBase() {
        return defaultDataBase;
    }

    public void setDefaultDataBase(String defaultDataBase) {
        this.defaultDataBase = defaultDataBase;
    }
}
