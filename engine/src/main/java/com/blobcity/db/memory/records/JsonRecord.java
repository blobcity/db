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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.lib.query.RecordType;
import org.json.JSONObject;
import java.util.UUID;

/**
 * @author sanketsarang
 */
public class JsonRecord implements com.blobcity.lib.data.Record {

    private final String id;
    private final JSONObject jsonObject;
    private final boolean uniqueCheckRequired;

    public JsonRecord(final String jsonString) {
        this.jsonObject = new JSONObject(jsonString);

        if(this.jsonObject.has("_id")) {
            this.id = this.jsonObject.get("_id").toString();
            this.uniqueCheckRequired = true;
        } else {
            this.id = UUID.randomUUID().toString();
            this.jsonObject.put("_id", this.id);
            this.uniqueCheckRequired = false;
        }
    }

    public JsonRecord(final JSONObject jsonObject) {
        this.jsonObject = jsonObject;

        if(this.jsonObject.has("_id")) {
            this.id = this.jsonObject.get("_id").toString();
            this.uniqueCheckRequired = true;
        } else {
            this.id = UUID.randomUUID().toString();
            this.jsonObject.put("_id", this.id);
            this.uniqueCheckRequired = false;
        }
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public RecordType getType() {
        return RecordType.JSON;
    }

    @Override
    public JSONObject asJson() {
        return this.jsonObject;
    }

    @Override
    public String asCsv() {
        throw new RuntimeException("Converstion from JSON to CSV not supported");
    }

    @Override
    public String asText() {
        return this.jsonObject.toString();
    }

    @Override
    public String asXml() {
        throw new RuntimeException("Conversion from JSON to XML not supported");
    }

    @Override
    public boolean uniqueCheckRequired() {
        return this.uniqueCheckRequired;
    }

    @Override
    public boolean equals(Object record) {
        if(record instanceof com.blobcity.lib.data.Record) {
            return ((com.blobcity.lib.data.Record) record).getId().equals(this.id);
        }

        return false;
    }

    public String toString() {
        return asJson().toString();
    }
}
