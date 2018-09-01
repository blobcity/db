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
 * The available collection storage types
 *
 * @author sanketsarang
 */
public enum CollectionStorageType {
    ON_DISK("on-disk"),
    IN_MEMORY("in-memory"),
    IN_MEMORY_NON_DURABLE("in-memory-nd");

    final String typeCode;

    CollectionStorageType(final String typeCode) {
        this.typeCode = typeCode;
    }

    public String getTypeCode() {
        return this.typeCode;
    }

    public static CollectionStorageType fromTypeCode(final String typeCode) {
        for(CollectionStorageType collectionStorageType : CollectionStorageType.values()) {
            if(collectionStorageType.getTypeCode().equalsIgnoreCase(typeCode)) {
                return collectionStorageType;
            }
        }

        return null;
    }
}

