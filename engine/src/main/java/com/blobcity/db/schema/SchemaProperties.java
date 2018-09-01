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

package com.blobcity.db.schema;

/**
 *
 * @author sanketsarang
 */
public class SchemaProperties {
    
    /* Meta properties */
    public static final String META = "meta";
    public static final String REPLICATION_TYPE = "replication-type";
    public static final String REPLICATION_FACTOR = "replication-factor";
    public static final String TABLE_TYPE = "type";
    public static final String FLEXIBLE_SCHEMA = "flexible-schema";
    public static final String PRIMARY_KEY_COL_NAME = "_id";
    
    /* Column properties */
    public static final String COLS = "cols";
    public static final String PRIMARY = "primary";
    public static final String COLS_DATA_TYPE = "type";
    public static final String COLS_DATA_SUB_TYPE = "sub-type";
    public static final String COLS_AUTO_DEFINE = "auto-define";
    public static final String COLS_NAME = "name";
    public static final String COLS_INDEX = "index";
    public static final String COLS_LENGTH = "length";
}
