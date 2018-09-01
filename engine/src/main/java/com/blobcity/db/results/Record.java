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

package com.blobcity.db.results;

import java.util.LinkedHashMap;

/**
 *
 * @author sanketsarang
 */
public class Record {

    private LinkedHashMap<String, RecordItem> map = new LinkedHashMap<>();

    private RecordItem get(final int index) {
        return (RecordItem) map.values().toArray()[index];
    }

    private RecordItem get(final String columnName) {
        if (map.containsKey(columnName)) {
            return map.get(columnName);
        }

        return null;
    }
}
