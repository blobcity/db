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

package com.blobcity.db.permissions;

/**
 * Enum for data access permission and their value as stored in database
 * 
 * @author sanketsarang
 */
public enum DataPermission {
    
    NONE(0),
    READ(1),
    WRITE(2),
    ALL(3),
    // these are done in such a way that their BITWISE AND 
    // with current value will set the new value accordingly.
    REMOVE_ALL(0),
    REMOVE_WRITE(1),
    REMOVE_READ(2);
    
    private final Integer value;

    private DataPermission(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
    
}
