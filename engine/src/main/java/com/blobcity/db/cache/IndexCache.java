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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class IndexCache {

    /* appId-table-pk -> set of indexed values */
    private final Map<String, Set<String>> map = createLruMap();

    public void cache(final String app, final String table, final String columnValue, final Set<String> keys) {
        map.put(makeKey(app, table, columnValue), keys);
    }

    public void invalidate(final String app, final String table, final String columnValue) {
        map.remove(makeKey(app, table, columnValue));
    }

    public Set<String> load(final String app, final String table, final String columnValue) {
        return map.get(makeKey(app, table, columnValue));
    }

    private String makeKey(final String app, final String table, final String columnValue) {
        StringBuilder sb = new StringBuilder(app);
        sb.append("-");
        sb.append(table);
        sb.append("-");
        sb.append(columnValue);
        return sb.toString();
    }

    private LinkedHashMap<String, Set<String>> createLruMap() {
        return new LinkedHashMap<String, Set<String>>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Set<String>> eldest) {
                return Runtime.getRuntime().totalMemory() > Runtime.getRuntime().maxMemory() * 0.98;
            }
        };
    }

    private byte[] zip(Set<String> set) throws OperationException {
        GZIPOutputStream gzipos = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            gzipos = new GZIPOutputStream(baos);
            ObjectOutputStream oos = new ObjectOutputStream(gzipos);
            oos.writeObject(set);
            return baos.toByteArray();
        } catch (IOException ex) {
            Logger.getLogger(IndexCache.class.getName()).log(Level.SEVERE, null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Index cache failed during in-memory data compression");
        } finally {
            try {
                if (gzipos != null) {
                    gzipos.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(IndexCache.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private Set<String> unzip(byte[] zippedData) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
