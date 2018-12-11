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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This is the default class comment, please change it!
 * If you're committing this, your merge WILL NOT BE ACCEPTED. You have been warned!
 *
 * @author sanketsarang
 */
public class MemoryRow {
    private static final Logger logger = LoggerFactory.getLogger(MemoryRow.class);
    
    private Map<Object, Object> row; // a single json object with all columns

    public MemoryRow() {
        row = new HashMap<>();
    }
    
    public Object getRow() {
        return row;
    }

    public void setRow(Map<Object, Object> row) {
        this.row = row;
    }
    
    public void insertJsonArray(Object json, String keyPrefix) {
        JSONArray jsonArray = (JSONArray) json;
        for (int i = 0; i < jsonArray.length(); i++) {
            Object jsonObj = jsonArray.get(i);
            if (jsonObj instanceof JSONObject) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                insert(jsonObj, keyPrefix);
            } else if (jsonObj instanceof JSONArray) {
                insertJsonArray(jsonObj, keyPrefix);
            }
            else {
                MemoryColumn memCol = new MemoryColumn(keyPrefix, keyPrefix);
                row.put(memCol, jsonObj);
                logger.debug("222: " + memCol.getColName() + " " + memCol.getNestedColName() + " " + jsonObj.toString());
            }
        }
    }
    
    public void insert(Object json, String keyPrefix) {
//        Set<Object> keys = ((JSONObject) json).keySet();
//        for (Object col : keys) {
//            Object val = ((JSONObject) json).get(col.toString());
//            String nestedKey = col.toString();
//            if (!keyPrefix.equals("")) {
//                nestedKey = (keyPrefix + "." + col.toString());
//            }
//            if (val instanceof JSONObject) {
//                MemoryColumn memCol = new MemoryColumn(col.toString(), nestedKey);
//                row.put(memCol, val);
////                logger.debug("1: " + memCol.getColName() + " " + memCol.getNestedColName() + " " + val.toString());
//                insert(val, nestedKey);
//            } else if (val instanceof JSONArray) {
//                MemoryColumn memCol = new MemoryColumn(col.toString(), nestedKey);
//                row.put(memCol, val);
////                logger.debug("11: " + memCol.getColName() + " " + memCol.getNestedColName() + " " + val.toString());
//                insertJsonArray(val, nestedKey);
//            } else {
//                MemoryColumn memCol = new MemoryColumn(col.toString(), nestedKey);
////                logger.debug("111: " + memCol.getColName() + " " + memCol.getNestedColName() + " " + val.toString());
//                row.put(memCol, val);
//            }
//        }
    }
}
