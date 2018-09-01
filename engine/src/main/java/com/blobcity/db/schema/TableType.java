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

/**
 * Represents the various types possible for a table
 *
 * @author sanketsarang
 */
public enum TableType {

    /* All data is stored on-disk and committed instantly to the storage system */
    ON_DISK("on-disk"),
    /* All data is stored in-memory and commits happen in memory. The data is written in bulk to the disk periodically,
     with full roll-back capabilities supported. All writes are ensured to be durable */
    IN_MEMORY("in-memory"),
    /* All data is stored temporarily in memory with no writes ever happening to the disk. All data is lost on server
     reboot.*/
    IN_MEMORY_NON_DURABLE("in-memory-nd");
    private final String type;

    TableType(final String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
    
    public static TableType fromString(final String type) {
        switch(type.toLowerCase()) {
            case "on-disk":
                return ON_DISK;
            case "in-memory":
                return IN_MEMORY;
            case "in-memory-nd":
            case "in-memory-non-durable":
                return IN_MEMORY_NON_DURABLE;
        }
        
        return null;
    }
}
