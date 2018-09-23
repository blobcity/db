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

/**
 * @author sanketsarang
 */
public class Performance {

    public static final int PROCESSING_BATCH;
    public static final int THREAD_POOL_SIZE;
    public static final int HASH_BUCKET = 1000000; //1 million records per memory hash set

    static {
        PROCESSING_BATCH = Runtime.getRuntime().availableProcessors() * 10000;
        THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    }
}
