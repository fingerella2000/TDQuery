package com.td.toolkit.utils;

/**
 * When TDQuery job failed, this exception will be thrown.
 */
public class JobFailedException extends Exception {

    public JobFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobFailedException(String message) {
        super(message);
    }
}
