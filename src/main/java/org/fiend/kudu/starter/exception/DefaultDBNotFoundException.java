package org.fiend.kudu.starter.exception;

public class DefaultDBNotFoundException extends RuntimeException {
    public DefaultDBNotFoundException() {
        super();
    }

    public DefaultDBNotFoundException(String message) {
        super(message);
    }
}
