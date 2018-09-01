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

package com.blobcity.db.watchservice;

/**
 * @author sanketsarang
 */
public enum WatchServiceImportType {

    LINE("line"),
    FILE("file"),
    FILE_INTERPRETED("file-interpreted");

    final String type;

    WatchServiceImportType(final String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public static WatchServiceImportType fromString(final String typeString) {
        switch(typeString.toLowerCase()) {
            case "line":
                return LINE;
            case "file":
                return FILE;
            case "file-interpreted":
                return FILE_INTERPRETED;
        }

        return null;
    }
}
