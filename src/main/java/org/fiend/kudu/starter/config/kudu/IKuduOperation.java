package org.fiend.kudu.starter.config.kudu;

import org.apache.kudu.client.Operation;

public interface IKuduOperation {
    Operation getOperation();
}
