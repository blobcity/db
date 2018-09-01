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

package com.blobcity.query;

import com.blobcity.db.bquery.SQLExecutorBean;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This is the default class comment, please change it! If you're committing this, your merge WILL NOT BE ACCEPTED. You have been warned!
 *
 * @author javatarz (Karun Japhet)
 */
@Component
@Deprecated //by Sanket Sarang
public class QueryExecutorImpl implements QueryExecutor {

    private final Logger logger = LoggerFactory.getLogger(QueryExecutorImpl.class + ":" + System.currentTimeMillis());
    @Autowired
    private SQLExecutorBean sqlExecutor;

    @Override
    public <T> List<T> run(String appId, String query, Class<T> clazz) {
        logger.trace("QueryExecutor.run(appId=\"{}\", query=\"{}\", clazz=\"{}\")", appId, query, clazz);
//        final String responseStr = sqlExecutor.runQuery(appId, query);
//        logger.trace("reponse string = \"{}\"", responseStr);
//
//        final JsonArray payloadJsonArray = new JsonParser().parse(responseStr).getAsJsonObject().get("p").getAsJsonArray();
//        final Gson gson = new Gson();
//        final List<T> responseObjList = new ArrayList<>();
//        for (final JsonElement jsonElement : payloadJsonArray) {
//            responseObjList.add(gson.fromJson(jsonElement, clazz));
//        }
//
//        logger.trace("reponse (Count: {}) = {}", responseObjList.size(), responseObjList);
//        return responseObjList;
        
        logger.error("QueryExecutor.run function is no longer supported");
        return Collections.EMPTY_LIST;
    }

}
