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
 * Stores the various types of replications possible for data within tables
 *
 * @author sanketsarang
 */
public enum ReplicationType {

    /* Data distributed to match the replication factor of the cluster */
    DISTRIBUTED("distributed"),
    /* Data is fully replicated across all nodes in the cluster for the specified table. The table on all nodes at all 
     times will have the same data */
    MIRRORED("mirrored");
    private final String type;

    ReplicationType(final String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
    
    public static ReplicationType fromString(final String type) {
        switch(type.toLowerCase()) {
            case "distributed":
                return DISTRIBUTED;
            case "mirrored":
                return MIRRORED;
        }
        
        return null;
    }
}
