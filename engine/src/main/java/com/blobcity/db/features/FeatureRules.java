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

package com.blobcity.db.features;


/**
 *
 * @author sanketsarang
 */
public class FeatureRules {

    /**
     * Default configuration that is used
     */
    public static boolean QUERY_RESULT_CACHING = true;
    public static boolean DATA_CACHING = false;  // enable when stable
    public static boolean INDEX_CACHING = false; // enable when stable
    public static boolean CACHE_INSERTS = false; // enable when stable, move this somewhere else as this is a user preference and not license setting.

    public static boolean TABLEAU_ENABLED = false; //service temporarily defunct
    public static boolean TABLEAU_AUTO_PUBLISH = false; //service temporarily defunct
    public static boolean TABLEAU_DS_LEVEL_AUTO_PUBLISH = false; //service temporarily defunct

    public static boolean VISUALISATION_ONLY = true; // if set to true, DB features are disabled
    public static boolean QPS = false; // used to enable/disable Query Performance Analysis tracking
    public static boolean QPS_BLOBCITY_SYNC = false; //enable to sync QPS data to BlobCity for it to analyse it

    /* Dynamic Elements */
    public static long COMMIT_OP_TIMEOUT = 60; //in seconds
    public static long READ_OP_TIMEOUT = 300; //in seconds

    /* These should be moved to user permissions in the future */
    public static boolean ALLOW_LIST_DS = true; // if true will allow list-ds operation to work. Set to false for cloud.
    public static boolean BYPASS_ROOT_ONLY = true; //if true will prevent root user login. Set to true for cloud.
}
