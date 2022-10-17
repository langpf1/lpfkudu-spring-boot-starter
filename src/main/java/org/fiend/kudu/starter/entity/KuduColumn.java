package org.fiend.kudu.starter.entity;

import org.apache.kudu.Type;

/**
 * @author langpf 2020-05-08 9:10:20
 */
public class KuduColumn {
    private String name;
    private Type type;
    private boolean isKey;
    private boolean nullable;
    private boolean isPartitionKey;

    public static KuduColumn of(String name, Type type) {
        return new KuduColumn(name, type);
    }

    public static KuduColumn of(String name, Type type, boolean nullable) {
        return new KuduColumn(name, type, false, nullable, false);
    }

    public static KuduColumn of(String name, Type type, boolean isKey, boolean nullable, boolean isPartitionKey) {
        return new KuduColumn(name, type, isKey, nullable, isPartitionKey);
    }

    private KuduColumn(String name, Type type) {
        this.name = name;
        this.type = type;
        this.isKey = true;
        this.nullable = false;
        this.isPartitionKey = true;
    }

    private KuduColumn(String name, Type type, boolean isKey, boolean nullable, boolean isPartitionKey) {
        this.name = name;
        this.type = type;
        this.isKey = isKey;
        this.nullable = nullable;
        this.isPartitionKey = isPartitionKey;

        if (this.isKey) {
            this.nullable = false;
        }
    }

    public boolean isPartitionKey() {
        return isPartitionKey;
    }

    public void setPartitionKey(boolean partitionKey) {
        isPartitionKey = partitionKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }
}
