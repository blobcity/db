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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;
import java.util.Collection;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores the memory data in a serializable manner that is eventually stored to a file.
 *
 * @author sanketsarang
 */
public class MemoryData implements Serializable {
    // primarykey mapped to data(json data)
    private Map<Object, Object> map;
    
    Logger logger = LoggerFactory.getLogger(MemoryData.class);
    
    /**
     * Constructor
     */
    public MemoryData() {
        this.map = new HashMap<>();
    }

    /**
     * 
     * @return map of <Object, Object>
     */
    public Map<Object, Object> getMap() {
        return map;
    }

    /**
     *
     * @param map of <Object, Object>
     */
    public void setMap(Map<Object, Object> map) {
        this.map = map;
    }

    /**
     *
     * @param key
     * @return Object corresponding to key
     */
    public Object getRecord(final Object key) {
        return map.get(key);
    }

    /**
     *
     * @return Collection of Objects
     */
    public Collection<Object> getAllRecords() {
        return map.values();
    }
    
    /**
     *
     * @return Set of Objects
     */
    public Set<Object> getAllKeys() {
        return map.keySet();
    }
    
    /**
     *
     * @return set of all keys as String
     */
    public Set<String> getAllKeysAsString() {
        HashSet<String> res = new HashSet<>();
        Set<Object> set = map.keySet();
        set.parallelStream().forEach((obj) -> {
            res.add(obj.toString());
        });
        return res;
    }

    /**
     *
     * @param key
     * @return record as JSONObject
     */
    public JSONObject getRecordAsJson(final Object key) {
        return new JSONObject(getRecord(key));
    }

    /**
     *
     * @param key
     * @param json
     */
    public void insert(final Object key, final JSONObject json) {
        if(!map.containsKey(key)) {
            map.put(key, json);
        }
    }

    /**
     *
     * @param key
     * @param json
     */
    public void save(final Object key, final JSONObject json) {
        if (!map.containsKey(key)) {
            map.put(key, json);
            return;
        }
        map.replace(key, json);
    }

    /**
     *
     * @param key
     * @param json
     */
    public void insert(final Object key, final Object json) {
        if(!map.containsKey(key)) {
            map.put(key, json);
        }
    }

    /**
     *
     * @param key
     * @param json
     */
    public void save(final Object key, final Object json) {
        if (!map.containsKey(key)) {
            map.put(key, json);
            return;
        }
        map.replace(key, json);
    }

    /**
     *
     * @param key
     */
    public void remove(final Object key){
        map.remove(key);
    }

    /**
     *
     */
    public void clearAll() {
        map.clear();
    }
}
