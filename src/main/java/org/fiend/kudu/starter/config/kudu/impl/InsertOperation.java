package org.fiend.kudu.starter.config.kudu.impl;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.fiend.kudu.starter.config.kudu.IKuduOperation;

/**
 * @author langpf 2020-06-29 11:03:53
 */
public class InsertOperation implements IKuduOperation {
    private final KuduTable ktable;

    public InsertOperation(KuduTable ktable) {
        this.ktable = ktable;
    }

    @Override
    public Operation getOperation() {
        return ktable.newInsert();
    }
}
