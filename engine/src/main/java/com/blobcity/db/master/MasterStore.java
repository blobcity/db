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

package com.blobcity.db.master;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class MasterStore {

    private Map<String, MasterExecutable> map = new ConcurrentHashMap<>();

    public void register(final String requestId, final MasterExecutable masterExecutable) {
        map.put(requestId, masterExecutable);
    }

    public void unregister(final String requestId) {
        map.remove(requestId);
    }

    public MasterExecutable get(final String requestId) {
        return map.get(requestId);
    }
}
