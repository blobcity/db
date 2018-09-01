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

package com.blobcity.db.memory.old;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sanketsarang
 */
public class MemoryColumn {
    private static final Logger logger =  LoggerFactory.getLogger(MemoryColumn.class);
    
    private String colName;
    private String nestedColName;

    public String getNestedColName() {
        return nestedColName;
    }

    public void setNestedColName(String nestedColName) {
        this.nestedColName = nestedColName;
    }
    
    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }
    
    public MemoryColumn(String colName){
        this.colName = colName;
    }
    
    public MemoryColumn(String colName, String nestedColName) {
        this.colName = colName;
        this.nestedColName = nestedColName;
    }
}
