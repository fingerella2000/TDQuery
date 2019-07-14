/**
 * Copyright (c) 2019 Bryan Luo. All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License, as
 * published by the Free Software Foundation.
 *
 */
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
