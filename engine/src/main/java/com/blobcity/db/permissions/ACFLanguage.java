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

package com.blobcity.db.permissions;

/**
 *
 * @author sanketsarang
 */
public enum ACFLanguage {
    PRIORITY("PRIORITY"),
    NAME("NAME"),
    DATA_PERMISSIONS("DATA_PERMISSIONS"),
    SCHEMA_PERMISSIONS("SCHEMA_PERMISSIONS"),
    SYSTEM_ADMINISTRATION("SYSTEM_ADMINISTRATION"),
    USER_ADMINISTRATION("USER_ADMINISTRATION");
    
    
    private final String literal;

    private ACFLanguage(String literal) {
        this.literal = literal;
    }
    
    public String getLiteral() {
        return literal;
    }
        
    public static boolean isLiteral(final String literal) {
        for (ACFLanguage acl : ACFLanguage.values()) {
            if (acl.getLiteral().matches(literal)) {
                return true;
            }
        }

        return false;
    }
    
}
