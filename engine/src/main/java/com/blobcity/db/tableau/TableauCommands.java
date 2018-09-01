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

package com.blobcity.db.tableau;

/**
 * @author sanketsarang
 */
public enum TableauCommands {

    PUBLISH("publish"),
    AUTO_PUBLISH("auto-publish");

    final String code;

    TableauCommands(final String code) {
        this.code = code;
    }

    public static TableauCommands fromCode(final String code) {
        switch(code.toLowerCase()) {
            case "publish":
                return TableauCommands.PUBLISH;
            case "auto-publish":
                return TableauCommands.AUTO_PUBLISH;
            default:
                return null;
        }
    }
}
