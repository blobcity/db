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

package com.blobcity.hooks.transactions;

import com.blobcity.hooks.Protocol;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

/**
 * @author sanketsarang
 */
public class UpdateTransaction implements HookTransaction {
    private final JSONObject jsonObject;

    public UpdateTransaction(final String ds, final String collection, final JSONObject record) {
        jsonObject = new JSONObject();
        jsonObject.put(Protocol.TYPE.key(), Protocol.TRANSACTION_HOOK.key());
        jsonObject.put(Protocol.EVENT.key(), Protocol.UPDATE_EVENT.key());
        jsonObject.put(Protocol.DS.key(), ds);
        jsonObject.put(Protocol.COLLECTION.key(), collection);

        JSONArray jsonArray = new JSONArray();
        jsonArray.put(record);

        jsonObject.put(Protocol.PAYLOAD.key(), jsonArray);
    }

    public UpdateTransaction(final String ds, final String collection, List<JSONObject> records) {
        jsonObject = new JSONObject();
        jsonObject.put(Protocol.TYPE.key(), Protocol.TRANSACTION_HOOK.key());
        jsonObject.put(Protocol.EVENT.key(), Protocol.UPDATE_EVENT.key());
        jsonObject.put(Protocol.DS.key(), ds);
        jsonObject.put(Protocol.COLLECTION.key(), collection);
        jsonObject.put(Protocol.PAYLOAD.key(), records);
    }

    @Override
    public JSONObject asJson() {
        return jsonObject;
    }
}
