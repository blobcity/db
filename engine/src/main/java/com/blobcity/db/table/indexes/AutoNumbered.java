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

package com.blobcity.db.table.indexes;

import com.blobcity.db.sql.lang.AutoNumberedTypes;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * IMPORTANT: This file is only used for data storage migration for version 1 to 2.
 * 
 * Map keyed on column names and having values corresponding to the auto number type.
 * @author sanketsarang
 */
@Deprecated
public class AutoNumbered implements Map, Serializable{
    
    //keyed on column name
    private Map<String, AutoNumberedTypes> map = new HashMap<String, AutoNumberedTypes>();

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        return map.containsKey(o.toString());
    }

    @Override
    public boolean containsValue(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object get(Object o) {
        return map.get(o.toString());
    }

    @Override
    public Object put(Object k, Object v) {
        return map.put(k.toString(), (AutoNumberedTypes)v);
    }

    @Override
    public Object remove(Object o) {
        return map.remove(o.toString());
    }

    @Override
    public void putAll(Map map) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set keySet() {
        return map.keySet();
    }

    @Override
    public Collection values() {
        return map.values();
    }

    @Override
    public Set entrySet() {
        return map.entrySet();
    }
    
}
