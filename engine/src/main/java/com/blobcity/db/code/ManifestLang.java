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

package com.blobcity.db.code;

/**
 * Stores the reserved works on language literals for the in-database code loading manifest specification format.
 *
 * @author sanketsarang
 */
@Deprecated
public enum ManifestLang {

    VERSION("MANIFEST-VERSION"),
    PROCEDURES("PROCEDURES"),
    TRIGGERS("TRIGGERS"),
    FILTERS("FILTERS"),
    MAPPERS("MAPPERS"),
    REDUCERS("REDUCERS"),
    DATAINTERPRETERS("DATAINTERPRETERS");
    private final String literal;

    ManifestLang(final String literal) {
        this.literal = literal;
    }

    public String getLiteral() {
        return literal;
    }

    public static boolean isLiteral(final String literal) {
        for (ManifestLang ml : ManifestLang.values()) {
            if (ml.getLiteral().matches(literal)) {
                return true;
            }
        }

        return false;
    }
}
