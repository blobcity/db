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

package com.blobcity.db.memory.collection;

import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sanketsarang
 */

@Component
public class MemCollectionStoreBean {

    private Map<String, MemCollection> map = new HashMap<>();

    public void add(final String collectionName, final MemCollection memCollection) {
        map.put(collectionName, memCollection);
    }

    public MemCollection get(final String collectionName) {
        return map.get(collectionName);
    }

    public void remove(final String collectionName) {
        map.remove(collectionName);
    }
}
