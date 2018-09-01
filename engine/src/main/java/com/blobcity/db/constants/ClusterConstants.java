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

package com.blobcity.db.constants;

/**
 * Stores constants specific to the clustering service. Clustering port numbers should be externalized in the future.
 *
 * @author sanketsarang
 */
public class ClusterConstants {

    public static int CLUSTER_PORT = 8094;
    public static int BEACON_PORT = 23234;
    public static int DEFAULT_CONNECTION_POOL = 5;
}
