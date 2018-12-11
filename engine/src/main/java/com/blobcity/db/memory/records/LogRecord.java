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

package com.blobcity.db.memory.records;

import com.blobcity.lib.query.RecordType;
import org.json.JSONObject;

/**
 * @author sanketsarang 
 */
public class LogRecord implements com.blobcity.lib.data.Record {

    @Override
    public String getId() {
        return null;
    }

    @Override
    public RecordType getType() {
        return null;
    }

    @Override
    public JSONObject asJson() {
        return null;
    }

    @Override
    public String asCsv() {
        return null;
    }

    @Override
    public String asText() {
        return null;
    }

    @Override
    public String asXml() {
        return null;
    }

    @Override
    public boolean uniqueCheckRequired() {
        return false;
    }
}
