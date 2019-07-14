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
 * Treasure Data returned columns in JSON format.
 *
 * @author  Bryan Luo
 * @version 1.0
 * @since   1.0
 *
 */
public class TDSchema {

    public String[][] getColumns() {
        return columns;
    }

    public void setColumns(String[][] columns) {
        this.columns = columns;
    }

    private String[][] columns;

}
