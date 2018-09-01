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

import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Schema;
import com.blobcity.lib.query.RecordType;
import org.json.JSONObject;

/**
 * @author sanketsarang
 */
public class RecordPassivation {

    public String getPassivationStringFromRecord(final com.blobcity.lib.data.Record record, final Schema schema) throws OperationException {
        final JSONObject jsonObject = new JSONObject();

        switch(record.getType()) {
            case JSON:
            case CSV:
            case SQL:
                jsonObject.put("t", RecordType.JSON.getTypeCode());
                jsonObject.put("d", record.asJson());
                break;
            case TEXT:
                jsonObject.put("t", RecordType.TEXT.getTypeCode());
                jsonObject.put("d", record.asJson());
                break;
            case XML:
                jsonObject.put("t", RecordType.XML.getTypeCode());
                jsonObject.put("d", record.asXml());
                break;
        }

        return jsonObject.toString();
    }

    public com.blobcity.lib.data.Record getRecordFromPassivatedString(final String passivatedString, final Schema schema) {
        final JSONObject jsonObject = new JSONObject(passivatedString);
        switch(RecordType.fromTypeCode(jsonObject.getString("t"))) {
            case JSON:
            case CSV:
            case SQL:
                return new JsonRecord(jsonObject.getJSONObject("d"));
            case TEXT:
                return new TextRecord(jsonObject.getString("d"));
            case XML:
                return new XmlRecord(jsonObject.getString("d"));
        }

        return null;
    }
}
