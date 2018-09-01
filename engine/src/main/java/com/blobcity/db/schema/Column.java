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

import com.blobcity.db.lang.columntypes.FieldType;
import java.io.Serializable;

/**
 * Stores schema of a single column
 * 
 * @author sanketsarang
 */
public class Column implements Serializable {
    private String name;
    private String mappedName;
    private FieldType fieldType;
    /**
     * Stores the type of the index.
     */
    private IndexTypes indexType;
    private AutoDefineTypes autoDefineType;
    
    public Column() {
        //do nothing
    }
    
    public Column(final String name, final FieldType fieldType, final IndexTypes indexType, final AutoDefineTypes autoDefineType) {
        this.name = name;
        this.fieldType = fieldType;
        this.indexType = indexType;
        this.autoDefineType = autoDefineType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMappedName() {
        return mappedName;
    }

    public void setMappedName(String mappedName) {
        this.mappedName = mappedName;
    }
    
    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public IndexTypes getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexTypes indexType) {
        this.indexType = indexType;
    }

    public AutoDefineTypes getAutoDefineType() {
        return autoDefineType;
    }

    public void setAutoDefineType(AutoDefineTypes autoDefineType) {
        this.autoDefineType = autoDefineType;
    }
}
