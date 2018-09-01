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

package com.blobcity.lib.data;

import com.blobcity.lib.query.RecordType;
import org.json.JSONObject;

/**
 * @author sanketsarang
 */
public interface
Record {

    public String getId();

    public RecordType getType();

    public JSONObject asJson();

    public String asCsv();

    public String asText();

    public String asXml();

    /**
     * Should be marked true by default, except in situtations when the _id primary key value is set by implementation
     * of this class, and there by checking for an existing primary key at time of insert is not requied.
     * @return true if a primary key check is required during insert, and false if same is not required.
     */
    public boolean uniqueCheckRequired();
}
