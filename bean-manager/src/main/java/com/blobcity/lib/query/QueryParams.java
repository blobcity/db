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

package com.blobcity.lib.query;

/**
 * @author sanketsarang
 */
public enum QueryParams {

    REQUEST_ID("rid"),
    PARENT_REQUEST_ID("prid"),
    MASTER_NODE_ID("mnid"),
    USERNAME("u"),
    QUERY("q"),
    PAYLOAD("p"),
    ACK("ack"),
    FROM_NODE_ID("fnid"),
    DATASTORE("ds"),
    COLLECTION("c"),
    TYPE("type"),
    DATA("data"),
    STORAGE_TYPE("s-type"),
    NODE_ID("nid"),
    IP("ip"),
    ERROR_CODE("ec"),
    MESSAGE("msg"),
    ARCHIVE_CODE("arch-code"),
    REPLICATION_TYPE("replication-type"),
    REPLICATION_FACTOR("replication-factor"),
    COLS("cols"),
    STATUS("status"),
    IDS("ids"),
    INSERTED("inserted"),
    FAILED("failed"),
    TIME("time"),
    INTERPRETER("interpreter"),
    INTERCEPTOR("interceptor"),
    SQL("sql");

    final String param;
    QueryParams(final String param) {
        this.param = param;
    }

    public String getParam() {
        return param;
    }
}
