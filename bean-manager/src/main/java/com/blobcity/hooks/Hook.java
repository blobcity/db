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

package com.blobcity.hooks;

import org.json.JSONObject;

/**
 * @author sanketsarang
 */
public class Hook {
    private final String id;
    private final String ds;
    private final String collection;
    private final HookType hookType;
    private final String url;

    public Hook(JSONObject jsonObject) {
        if(jsonObject.has("id")) {
            id = jsonObject.getString("id");
        } else {
            id = null;
        }

        if(jsonObject.has("ds")) {
            ds = jsonObject.getString("ds");
        } else {
            ds = null;
        }

        if(jsonObject.has("c")) {
            collection = jsonObject.getString("c");
        } else {
            collection = null;
        }

        if(jsonObject.has("type")) {
            hookType = HookType.fromString(jsonObject.getString("type"));
            if(hookType == null) {
                throw new RuntimeException("Hook type unrecognisable. type = " + jsonObject.getString("type"));
            }
        } else {
            throw new RuntimeException("Hook being created in an invalid state");
        }

        if(jsonObject.has("url")) {
            url = jsonObject.getString("url");
        } else {
            throw new RuntimeException("Hook cannot have null url");
        }
    }

    public Hook(final String id, final String ds, final String collection, final HookType hookType, final String url) {
        this.id = id;
        this.ds = ds;
        this.collection = collection;
        this.hookType = hookType;
        this.url = url;
    }

    public JSONObject asJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", this.id);
        jsonObject.put("ds", this.ds);
        if(this.collection != null) {
            jsonObject.put("collection", this.collection);
        }
        jsonObject.put("type", this.hookType.getType());
        jsonObject.put("url", this.url);
        return jsonObject;
    }
}
