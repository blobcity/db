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

package com.blobcity.db.billing;

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.OperationException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author sanketsarang
 */
@Component
public class SelectActivityLog {

    private static final Logger logger = LoggerFactory.getLogger(SelectActivityLog.class.getName());

    @Autowired
    private BSqlDataManager dataManager;

    public void registerActivity(final String ds, final long rows) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ds", ds);
        jsonObject.put("rows", rows);
        try {
            dataManager.insert(".systemdb", "SelectActivityLog", jsonObject);
        } catch (OperationException e) {
            e.printStackTrace();
        }
    }
}
