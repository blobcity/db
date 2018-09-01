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

package com.blobcity.lib.database.bean.manager.interfaces.engine;

import java.util.Map;

/**
 *
 * @author sanketsarang
 */
public interface QueryStore {

    public void register(final String appId, final String requestId, final QueryData queryData);

    public void unregister(final String appId, final String requestId);

    public Map<String, QueryData> getAppQueries(final String appId);
}
