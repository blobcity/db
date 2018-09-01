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

package com.blobcity.db.operations;

import com.blobcity.db.exceptions.OperationException;
import java.util.concurrent.Future;

/**
 *w
 * @author sanketsarang
 */
public interface Operable {

    public Future<OperationStatus> start(String app, String table, String opid) throws OperationException;

    public Future<OperationStatus> start(String app, String table, String opid, OperationLogLevel level) throws OperationException;

    public void stop(String app, String table, String opid) throws OperationException;

//    public OperationStatus getStatus(String app, String table, String opid);
//    public JSONObject getJson(String app, String table, String opid);
//    public boolean isActive(String app, String table, String opid);
    public OperationTypes getType();
}
