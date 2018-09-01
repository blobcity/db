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

package com.blobcity.lib.export;

/**
 * @author sanketsarang
 */
public enum ExportType {
    CSV("csv"),
    EXCEL("excel"),
    PDF("pdf"),
    TEXT("text"),
    JSON("json"),
    XML("xml");

    private final String typeString;

    ExportType(final String typeString) {
        this.typeString = typeString;
    }

    public String getTypeString() {
        return this.typeString;
    }

    public static ExportType fromTypeString(final String typeString) {
        switch(typeString.toLowerCase()){
            case "csv":
                return CSV;
            case "excel":
                return EXCEL;
            case "pdf":
                return PDF;
            case "text":
                return TEXT;
            case "json":
                return JSON;
            case "xml":
                return XML;
            default:
                return null;
        }
    }
}
