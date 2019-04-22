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

package com.blobcity.db.bquery;

import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryData;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryStore;
import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class QueryStoreBean implements QueryStore {
    private final Map<String, Map<String, QueryData>> map = new HashMap<>();
    
    @Override
    public void register(final String appId, final String requestId, final QueryData queryData) {
        if(!map.containsKey(appId)){
            map.put(appId, new HashMap<>());
        }
        
        map.get(appId).put(requestId, queryData);
    }
    
    @Override
    public void unregister(final String appId, final String requestId) {
        if(!map.containsKey(appId)){
            return;
        }
        map.get(appId).remove(requestId);
    }
    
    @Override
    public Map<String, QueryData> getAppQueries(final String appId) {
        return map.get(appId);
    }

    @Override
    public int size(final String appId) {
        return map.get(appId).keySet().size();
    }
}
