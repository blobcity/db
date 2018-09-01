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

package com.blobcity.db.code.triggers;

/**
 * Used to store the functions allowed in Trigger class
 * 
 * @author sanketsarang
 */
public enum TriggerFunction {
    BEFORE_INSERT("beforeInsert"),
    AFTER_INSERT("afterInsert"),
    BEFORE_UPDATE("beforeUpdate"),
    AFTER_UPDATE("afterUpdate"),
    BEFORE_DELETE("beforeDelete"),
    AFTER_DELETE("afterDelete");
    
    private final String name;
    
    private TriggerFunction(String functionName) {
        this.name = functionName; 
    }

    public String getFunctionName() {
        return name;
    }
    
}
