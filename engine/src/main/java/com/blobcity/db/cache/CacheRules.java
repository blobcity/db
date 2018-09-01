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

import java.util.Set;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class CacheRules {
    private Set<String> allow;
    private Set<String> deny;
    
    public void setAllow(final String app) {
        allow.add(getKey(app));
        deny.remove(getKey(app));
    }
    
    public void setAllow(final String app, final String table) {
        allow.add(getKey(app, table));
        deny.remove(getKey(app, table));
    }
    
    public void setAllowAll() {
        allow.add(getAllKey());
        deny.remove(getAllKey());
    }
    
    public void setDeny(final String app, final String table) {
        allow.remove(getKey(app, table));
        deny.add(getKey(app, table));
    }
    
    public void setDeny(final String app) {
        allow.remove(getKey(app));
        deny.remove(getKey(app));
    }
    
    public void setDenyAll() {
        allow.remove(getAllKey());
        deny.remove(getAllKey());
    }
    
    private String getKey(final String app) {
        return app;
    }
    
    private String getKey(final String app, final String table) {
        StringBuilder sb = new StringBuilder(app);
        sb.append("-");
        sb.append(table);
        return sb.toString();
    }
    
    private String getAllKey() {
        return "*";
    }
    
    public boolean shouldCache(final String app, final String table) {
        //TODO: Implement this
        return true;
    }
}
