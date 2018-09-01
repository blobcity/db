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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Maps viewable / developer assigned names of columns to their internal names and vice versa.
 *
 * @author sanketsarang
 */
public class ColumnMapping {

    /**
     * Stores the next index number to be assigned to a newly added column. Serially increasing atomic integer.
     */
    private AtomicInteger index = new AtomicInteger(0);
    private Map<String, String> viewableNameMap = new HashMap<>();// maps viewable name of column to it's internal name
    private Map<String, String> internalNameMap = new HashMap<>(); //maps internal name of a column to it's viewable name

    public ColumnMapping() {
        //do nothing
    }

    public ColumnMapping(JSONObject jsonObject) throws JSONException {
        this.index = new AtomicInteger(jsonObject.getInt("index"));
        JSONObject mapJson = jsonObject.getJSONObject("map");
        Iterator<String> keysIterator = mapJson.keys();
        while (keysIterator.hasNext()) {
            String viewableName = keysIterator.next();
            String internalName = mapJson.getString(viewableName);
            viewableNameMap.put(viewableName, internalName);
            internalNameMap.put(internalName, viewableName);
        }
    }

    public Map<String, String> getViewableNameMap() {
        return viewableNameMap;
    }

    public Map<String, String> getInternalNameMap() {
        return internalNameMap;
    }

    /**
     * Function is internally fault tolerant and just performs the desired operations overwrite any critical values. It
     * is important to do schema validation checks before calling this function to ensure that no data in the column
     * mapping gets corrupted
     *
     * @param viewableName the viewable name of the column as provided by the developer
     * @param internalName an internal name assigned to the column which is available only to the database
     */
    public synchronized void addMapping(final String viewableName, final String internalName) {
        viewableNameMap.put(viewableName, internalName);
        internalNameMap.put(internalName, viewableName);
    }

    public synchronized String addMapping(final String viewableName) {
        if (viewableNameMap.containsKey(viewableName)) {
            return viewableNameMap.get(viewableName);
        }

        final String nextInternalName = "" + index.incrementAndGet();
        addMapping(viewableName, nextInternalName);
        return nextInternalName;
    }

    public void removeMapping(final String viewableName) {
        final String internalValue = viewableNameMap.get(viewableName);
        viewableNameMap.remove(viewableName);
        internalNameMap.remove(internalValue);
    }

    public void renameMappedColumn(final String oldViewableName, final String newViewableName) {
        final String internalValue = viewableNameMap.get(oldViewableName);
        viewableNameMap.remove(oldViewableName);
        internalNameMap.remove(internalValue);
        viewableNameMap.put(newViewableName, internalValue);
        internalNameMap.put(internalValue, newViewableName);
    }

    public String getInternalName(final String viewableName) {
        return viewableNameMap.get(viewableName);
    }

    public String getViewableName(final String internalName) {
        return internalNameMap.get(internalName);
    }

    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject = new JSONObject();
        JSONObject mapJson = new JSONObject();

        for (String viewableName : viewableNameMap.keySet()) {
            mapJson.put(viewableName, viewableNameMap.get(viewableName));
        }

        jsonObject.put("index", index.get());
        jsonObject.put("map",mapJson);
        return jsonObject;
    }

    public String toJSONString() throws JSONException {
        return toJSONObject().toString();
    }

    /**
     * Returns an ordered list of viewable column names. CSV data records should have columns in this order.
     * @return an ordered list of viewable column names
     */
    public List<String> getOrderedViewableColumns() {
        List<String> orderedInternalColumns = getOrderedInternalColumns();
        List<String> viewableColumnNames = new ArrayList<>();

        orderedInternalColumns.forEach(internalColumn -> viewableColumnNames.add(internalNameMap.get(internalColumn)));
        return viewableColumnNames;
    }

    /**
     * Returns an ordered list of internal column names. CSV data records should have columns in this order.
     * @return an ordered list of internal column names
     */
    public List<String> getOrderedInternalColumns() {
        List<String> internalColumnNames = (List<String>) viewableNameMap.values();
        Collections.sort(internalColumnNames);
        return internalColumnNames;
    }
}
