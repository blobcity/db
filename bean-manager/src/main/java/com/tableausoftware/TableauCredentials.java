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

package com.tableausoftware;

/**
 * @author sanketsarang
 */
public class TableauCredentials {

    private final String serverIp;
    private final String user;
    private final String password;

    public TableauCredentials(final String serverIp, final String user, final String password) {
        this.serverIp = serverIp;
        this.user = user;
        this.password = password;
    }

    public static TableauCredentials getDefault() {
        return new TableauCredentials("visual.blobcity.com", "apiuser", "Dcs-H5H-mkz-LmY");
    }

    public String getServerIp() {
        return serverIp;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
