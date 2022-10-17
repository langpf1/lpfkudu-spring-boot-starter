package org.fiend.kudu.starter.entity;

import org.apache.kudu.client.KuduPredicate;

/**
 * @author langpf 2020-05-08 9:10:20
 */
public class ColumnCond {
    // ColumnSchema column, KuduPredicate.ComparisonOp op, long value
    private String colName;
    private KuduPredicate.ComparisonOp op;
    private Object value;

    public static ColumnCond of(String colName,
                                 KuduPredicate.ComparisonOp op,
                                 Object value) {
        return new ColumnCond(colName, op, value);
    }

    private ColumnCond(String colName,
                       KuduPredicate.ComparisonOp op,
                       Object value) {
        this.colName = colName;
        this.op = op;
        this.value = value;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public KuduPredicate.ComparisonOp getOp() {
        return op;
    }

    public void setOp(KuduPredicate.ComparisonOp op) {
        this.op = op;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
