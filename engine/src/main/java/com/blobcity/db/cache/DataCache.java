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

package com.blobcity.db.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class DataCache {
    
    /* appId-table-pk -> record in non-viewable json string form */
    private final Map<String, String> map = createLruMap();
    
    public void cache(final String app, final String table, final String pk, final String internalJsonString) {
        map.put(makeKey(app, table, pk), internalJsonString);
    }
    
    public void invalidate(final String app, final String table, final String pk) {
        map.remove(makeKey(app, table, pk));
    }
    
    public String load(final String app, final String table, final String pk) {
        return map.get(makeKey(app, table, pk));
    }
    
    private String makeKey(final String app, final String table, final String pk) {
        StringBuilder sb = new StringBuilder(app);
        sb.append("-");
        sb.append(table);
        sb.append("-");
        sb.append(pk);
        return sb.toString();
    }
    
    private LinkedHashMap<String, String> createLruMap() {
        return new LinkedHashMap<String, String>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return Runtime.getRuntime().totalMemory() > Runtime.getRuntime().maxMemory() * 0.98;
            }
        };
    }
}
