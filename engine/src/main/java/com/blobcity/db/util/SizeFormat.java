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

import org.slf4j.LoggerFactory;

/**
 *
 * @author sanketsarang
 */
public class SizeFormat {

    private static final long KB = 1000;
    private static final long MB = KB * 1000;
    private static final long GB = MB * 1000;
    private static final long TB = GB * 1000;

    public static long toBytes(String formattedSize) {
        try {
            if (formattedSize.endsWith("KB")) {
                formattedSize = formattedSize.substring(0, formattedSize.length() - 2);
                return Long.parseLong(formattedSize) * KB;
            } else if (formattedSize.endsWith("MB")) {
                formattedSize = formattedSize.substring(0, formattedSize.length() - 2);
                return Long.parseLong(formattedSize) * MB;
            } else if (formattedSize.endsWith("GB")) {
                formattedSize = formattedSize.substring(0, formattedSize.length() - 2);
                return Long.parseLong(formattedSize) * GB;
            } else if (formattedSize.endsWith("TB")) {
                formattedSize = formattedSize.substring(0, formattedSize.length() - 2);
                return Long.parseLong(formattedSize) * TB;
            } else {
                return Long.parseLong(formattedSize);
            }
        } catch (NumberFormatException ex) {
            LoggerFactory.getLogger(SizeFormat.class.getName()).error(null, ex);
        }

        return -1;
    }
}
