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
 * The available multi-model storage types
 *
 * @author sanketsarang
 */
public enum RecordType {
    JSON("json"),
    XML("xml"),
    CSV("csv"),
    SQL("sql"),
    TEXT("text"),
    AUTO("auto");

    final String typeCode;

    RecordType(final String typeCode) {
        this.typeCode = typeCode;
    }

    public String getTypeCode() {
        return this.typeCode;
    }

    public static RecordType fromTypeCode(final String typeCode) {
        for(RecordType recordType : RecordType.values()) {
            if(recordType.getTypeCode().equalsIgnoreCase(typeCode)) {
                return recordType;
            }
        }

        return null;
    }
}
