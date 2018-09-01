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

package com.blobcity.db.license;

import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
public class LicenseRules {

    /**
     * Default configuration that is used for the free edition of the product
     */

    public static boolean DATA_CACHING = false;
    public static boolean INDEX_CACHING = false;
    public static boolean CACHE_INSERTS = false; // move this somewhere else as this is a user preference and not license setting.
    public static boolean QUERY_RESULT_CACHING = false;
    public static boolean CLUSTERING_AVAILABLE = false;
    public static boolean CLI_QUERY_ANALYSER = false;
    public static boolean FLEXIBLE_SCHEMA = true;
    public static boolean FILE_INTERPRETED_WATCH_SERVICE = false;
    public static boolean TABLEAU_AUTO_PUBLISH = false;
    public static boolean TABLEAU_DS_LEVEL_AUTO_PUBLISH = false;
    public static boolean MEMORY_DURABLE_TABLES = false;
    public static boolean MEMORY_NON_DURABLE_TABLES = false;
    public static boolean VISUALISATION = false;
    public static boolean VISUALISATION_ONLY = false; //if set to true, DB features are disabled
    public static boolean GEO_REP = false;
    public static boolean STORED_PROCEDURES = false;

    /* Dynamic Elements */
    public static long DATA_LIMIT = -1; // -1 means unlimited data. Limit is otherwise specified in GB's and applies to whole cluster this node is a part of
    public static long EXPIRES = -1; //-1 means no expiry. Value is otherwise a long timestamp.

    /* These should be moved to user permissions in the future */
    public static boolean ALLOW_LIST_DS = true; // if true will allow list-ds operation to work. Set to false for cloud.
    public static boolean BYPASS_ROOT_ONLY = false; //if true will prevent root user login. Set to true for cloud.

    /**
     * For enterprise edition
     */

//    public static final boolean DATA_CACHING = true;
//    public static final boolean INDEX_CACHING = true;
//    public static final boolean CACHE_INSERTS = true; // move this somewhere else as this is a user preference and not license setting.
//    public static final boolean CLUSTERING_AVAILABLE = true;
//    public static final boolean CLI_QUERY_ANALYSER = true;
//    public static final boolean FLEXIBLE_SCHEMA = true;
//    public static final boolean FILE_INTERPRETED_WATCH_SERVICE = true;
//    public static final boolean SP_WEB_SERVICES = true;
//    public static final boolean TABLEAU_AUTO_PUBLISH = true;
//    public static final boolean TABLEAU_DS_LEVEL_AUTO_PUBLISH = true;
//    public static final boolean ALLOW_LIST_DS = true;
//    public static final boolean BYPASS_ROOT_ONLY = true;
//
//    /* For future use */
//    public static final boolean MEMORY_DURABLE_TABLES = true;
//    public static final boolean MEMORY_NON_DURABLE_TABLES = true;


    /**
     * For hosted cloud edition
     */

//    public static final boolean DATA_CACHING = true;
//    public static final boolean INDEX_CACHING = true;
//    public static final boolean CACHE_INSERTS = true; // move this somewhere else as this is a user preference and not license setting.
//    public static final boolean CLUSTERING_AVAILABLE = true;
//    public static final boolean CLI_QUERY_ANALYSER = true;
//    public static final boolean FLEXIBLE_SCHEMA = true;
//    public static final boolean FILE_INTERPRETED_WATCH_SERVICE = true;
//    public static final boolean SP_WEB_SERVICES = true;
//    public static final boolean TABLEAU_AUTO_PUBLISH = true;
//    public static final boolean TABLEAU_DS_LEVEL_AUTO_PUBLISH = false;
//    public static final boolean ALLOW_LIST_DS = false;
//    public static final boolean BYPASS_ROOT_ONLY = false;
//
//    /* For future use */
//    public static final boolean MEMORY_DURABLE_TABLES = false;
//    public static final boolean MEMORY_NON_DURABLE_TABLES = false;
}
