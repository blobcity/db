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

package com.blobcity.db.code.webservices;

import com.blobcity.code.WebServiceExecutor;
import com.blobcity.db.exceptions.OperationException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author sanketsarang
 */
@Component
public class WebServiceExecutorBean implements WebServiceExecutor {

    @Autowired
    private WebServiceStore webServiceStore;

    @Override
    public JSONObject executePost(final String datatore, final String path, final JSONObject jsonBody, final JSONObject requestParmas) {

        try {
            return webServiceStore.getNewInstance(datatore, path).post(jsonBody, requestParmas);
        } catch (OperationException e) {
            e.printStackTrace();
            JSONObject responseJson = new JSONObject();
            responseJson.put("error", e.getErrorCode().getErrorCode());
            return responseJson;
        }
    }

    public JSONObject executeGet(final String datastore, final String path, final JSONObject requestParams) {

        try {
            return webServiceStore.getNewInstance(datastore, path).get(requestParams);
        } catch (OperationException e) {
            e.printStackTrace();
            JSONObject responseJson = new JSONObject();
            responseJson.put("error", e.getErrorCode().getErrorCode());
            return responseJson;
        }
    }
}
