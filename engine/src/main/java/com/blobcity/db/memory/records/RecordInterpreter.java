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

import com.blobcity.lib.data.Record;
import com.blobcity.lib.query.RecordType;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

/**
 * @author sanketsarang
 */
public class RecordInterpreter {

    public static RecordType getBestMatchedType(Object record) {

        /* Testing for JSON */
        if(record instanceof JSONObject) {
            return RecordType.JSON;
        }

        try {
            new JSONObject(record.toString());
            return RecordType.JSON;
        } catch (JSONException ex) {
            //do nothing
        }

        /* Testing for XML */
        try {
            XML.toJSONObject(record.toString());
            return RecordType.XML;
        } catch(JSONException ex) {
            //do nothing
        }

        /* Testing for SQL INSERT INTO query */
        //TODO: Improve logic to fully validate insert into query format
        if(record.toString().toLowerCase().startsWith("insert into")) {
            return RecordType.SQL;
        }

        return RecordType.TEXT; //default return even in case for CSV data as CSV cannot be auto-inferred.
    }
}
