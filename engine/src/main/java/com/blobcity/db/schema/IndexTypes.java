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

package com.blobcity.db.schema;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;

/**
 *
 * @author sanketsarang
 */
public enum IndexTypes {

    NONE("none"),
    UNIQUE("unique"),
    BTREE("btree"),
    HASHED("hashed"),
    BITMAP("bitmap"),
    ARRAY("array"),
    TIMESERIES("timeseries"),
    GEO("geo");
    private String text;

    IndexTypes(final String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public static IndexTypes fromString(String type) throws OperationException {
        if (type == null || type.isEmpty()) {
            throw new OperationException(ErrorCode.INVALID_SCHEMA, "A blank index type is not permitted");
        }

        for (IndexTypes value : values()) {
            if (type.equalsIgnoreCase(value.getText())) {
                return value;
            }
        }

        throw new OperationException(ErrorCode.INVALID_SCHEMA, "Index type " + type + " could not be mapped to any known index types");
    }
}
