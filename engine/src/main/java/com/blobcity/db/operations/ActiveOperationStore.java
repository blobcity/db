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

package com.blobcity.db.operations;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class ActiveOperationStore {

    private final Map<String, Future<OperationStatus>> statusMap = new HashMap<>();

    public void add(final String app, final String opid, final Future<OperationStatus> futureStatus) {
        statusMap.put(opid, futureStatus);
    }

    public void remove(final String opid) {
        statusMap.remove(opid);
    }

    public boolean contains(final String opid) {
        return statusMap.containsKey(opid);
    }

    public Future<OperationStatus> get(final String opid) {
        return statusMap.get(opid);
    }
}
