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

package com.blobcity.db.code;

import com.blobcity.db.constants.BSql;
import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class LoaderStore {

    private final Map<String, RestrictedClassLoader> loaderMap = new HashMap<>();

    /**
     * Gets an instance of the RestrictedClassLoader if already present in the map
     *
     * @param appId
     * @return The RestrictedClassLoader if present in map for the specified appId, null otherwise
     */
    public RestrictedClassLoader getLoader(String appId) {
        return loaderMap.get(appId);
    }

    /**
     * Gets an instance of the RestrictedClassLoader from the map. Function will create a new loader and add into the map if not already present for the
     * mentioned appId, otherwise will return an existing instance of the loader
     *
     * @param appId
     * @return
     */
    public RestrictedClassLoader getLoaderWithCreate(String appId) {
        if (!loaderMap.containsKey(appId)) {
            final RestrictedClassLoader restrictedClassLoader = new RestrictedClassLoader(appId + BSql.DB_HOT_DEPLOY_FOLDER);
            loaderMap.put(appId, restrictedClassLoader);

            return restrictedClassLoader;
        }

        return loaderMap.get(appId);
    }

    /**
     * Creates a new loader and returns it with every request
     *
     * @param appId
     * @return
     */
    public RestrictedClassLoader getNewLoader(String appId) {
        final RestrictedClassLoader restrictedClassLoader = new RestrictedClassLoader(appId + BSql.DB_HOT_DEPLOY_FOLDER);
        loaderMap.put(appId, restrictedClassLoader);

        return restrictedClassLoader;
    }

    public void remove(String appId) {
        loaderMap.remove(appId);
    }
}

