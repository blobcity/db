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

package com.blobcity.db.util;

import com.blobcity.db.exceptions.OperationException;
import org.json.JSONObject;

/**
 * Creates a generic error response from an {@link OperationException}
 *
 * @author akshaydewan
 */
public class JSONOperationException {

    public static JSONObject create(OperationException ex) {
        JSONObject errorJson = new JSONObject();
        errorJson.put("ack", "0");
        if (ex.getErrorCode() == null) {
            return errorJson;
        }
        errorJson.put("code", ex.getErrorCode().getErrorCode());
        if (ex.getMessage() != null && !ex.getMessage().isEmpty()) {
            errorJson.put("cause", ex.getMessage());
        } else {
            errorJson.put("cause", ex.getErrorCode().getErrorMessage());
        }
        return errorJson;
    }

}
