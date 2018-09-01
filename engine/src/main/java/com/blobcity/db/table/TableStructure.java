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

package com.blobcity.db.table;

import com.blobcity.db.table.indexes.AutoNumbered;
import com.blobcity.db.table.indexes.Column;
import com.blobcity.db.table.indexes.Index;
import com.blobcity.db.table.indexes.PrimaryKey;
import com.blobcity.util.json.JsonData;
import java.util.Iterator;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * IMPORTANT: This file is only used for data storage migration for version 1 to 2.
 * 
 * Contains the table structure that is serialized and written to the structure file.
 * @author sanketsarang
 */
@Deprecated
public class TableStructure implements java.io.Serializable {
    private static final long serialVersionUID = 8514124620981073896L;

    private Column columns = new Column();              //This is a Map
    private PrimaryKey primaryKey = new PrimaryKey();   //This is a List
    private Index index = new Index();                  //This is a List
    private AutoNumbered autoNumbered = new AutoNumbered(); //This is a Map

    public Column getColumns() {
        return columns;
    }

    public void setColumns(Column columns) {
        this.columns = columns;
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public AutoNumbered getAutoNumbered() {
        return autoNumbered;
    }

    public void setAutoNumbered(AutoNumbered autoNumbered) {
        this.autoNumbered = autoNumbered;
    }
    
    public boolean containsColumn(String column) {
        if (columns.containsKey(column)) {
            return true;
        }
        return false;
    }

    /**
     * This function should convert the table structure to it's corresponding
     * JSON representation. The JSON format returned by this function is a 
     * proprietary format followed by the BlobCity database.
     * @return The schema in sql form
     */
    public String toJsonString() throws JSONException {
        String json = "";
        Set set;
        JsonData jsonData = new JsonData();
        JSONObject jsonObject;
        JSONArray jsonArray;
        Iterator<String> iterator;
        String item;

        /* Load the structure for columns */
        set = columns.keySet();
        iterator = set.iterator();
        jsonArray = new JSONArray();
        while (iterator.hasNext()) {
            item = iterator.next();
            jsonObject = new JSONObject();
            jsonObject.put(item, columns.get(item).toString());
            jsonArray.put(jsonObject);
        }
        jsonData.put("columns", jsonArray);

        /* Load the structure for Primary Key */
        iterator = primaryKey.iterator();
        jsonObject = new JSONObject();
        jsonArray = new JSONArray();
        jsonObject.put("name", primaryKey.getName());
        while (iterator.hasNext()) {
            item = iterator.next();
            jsonArray.put(item);
        }
        jsonObject.put("items", jsonArray);
        jsonData.put("primary", jsonObject);
        
        /* Populate list of all autonumbered items */
        iterator = autoNumbered.keySet().iterator();
        jsonArray = new JSONArray();
        while(iterator.hasNext()){
            item = iterator.next();
            jsonObject = new JSONObject();
            jsonObject.put("name", item);
            jsonObject.put("type", autoNumbered.get(item));
            jsonArray.put(jsonObject);
        }
        
        if(jsonArray.length() > 0){
            jsonData.put("auto-defined", jsonArray);
        }
        
        //TODO: In future load the details for other items pertaining to the table structure
        
        return jsonData.toString();
    }
}