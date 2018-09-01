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
import com.blobcity.db.schema.Schema;
import com.blobcity.lib.query.RecordType;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * @author sanketsarang
 */
public class CsvRecord implements com.blobcity.lib.data.Record {

    private final String id;
    private final List<String> columnNames;
    private final List<String> values;
    private final JSONObject jsonObject;
    private final boolean uniqueCheckRequired = false;

    public CsvRecord(final String csvLine, Schema schema) {
        this.columnNames = schema.getOrderedVisibleColumnNames();
        this.values = Arrays.asList(csvLine.split(","));
        this.jsonObject = new JSONObject();

        /** Defines primary key and adds at the beginning, as the columns picked up from the schema will always have
         * _id as the first column
         */
        this.id = UUID.randomUUID().toString();
        values.add(0, this.id);

        for(int i = 0; i < columnNames.size(); i++) {
            jsonObject.put(columnNames.get(i), i < values.size() - 1 ? values.get(i) : null);
        }
    }

    public CsvRecord(final List<String>columnNames, final List<String> values) {
        this.columnNames = columnNames;
        this.values = values;
        this.jsonObject = new JSONObject();

        for(int i = 0; i < columnNames.size(); i++) {
            jsonObject.put(columnNames.get(i), i < values.size() - 1 ? values.get(i) : null);
        }

        this.id = UUID.randomUUID().toString();
        jsonObject.put("_id", id);
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public RecordType getType() {
        return RecordType.CSV;
    }

    @Override
    public JSONObject asJson() {
        return this.jsonObject;
    }

    @Override
    public String asCsv() {
        StringBuffer sb = new StringBuffer();
        for(String value : values) {
            if(!sb.toString().isEmpty()) {
                sb.append(",");
            }
            sb.append(value);
        }

        return sb.toString();
    }

    @Override
    public String asText() {
        return asCsv();
    }

    @Override
    public String asXml() {
        throw new RuntimeException("Conversion from CSV to XML not supported");
    }

    public List<String> getColumnNames() {
        return this.columnNames;
    }

    public List<String> getValues() {
        return this.values;
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
}
