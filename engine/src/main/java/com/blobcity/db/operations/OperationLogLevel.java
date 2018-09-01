/**
 * Copyright (C) 2018 BlobCity Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.blobcity.db.operations;

/**
 *
 * @author sanketsarang
 */
public enum OperationLogLevel {

    FINE("fine",1),
    INFO("info",2),
    ERROR("error",3);
    private String text;
    private int logOrder;

    OperationLogLevel(String text, int logOrder) {
        this.logOrder = logOrder;
    }

    public int getLogOrder() {
        return logOrder;
    }

    public String getText() {
        return text;
    }
    
    public static OperationLogLevel fromText(final String text) {
        switch(text.toLowerCase()) {
            case "fine":
                return OperationLogLevel.FINE;
            case "info":
                return OperationLogLevel.INFO;
            case "error":
                return OperationLogLevel.ERROR;
            default:
                return null;
        }
    }
}
