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

package com.blobcity.lib.database.bean.manager.interfaces.security;

/**
 * Interface for the bean which handles all the security for the database 
 * 
 * @author sanketsarang
 */
public interface SecurityManager {
    
    /**
     * verify credentials for a user for a login request
     * 
     * @param username
     * @param password
     * @return true if user is authentic, false otherwise
     */ 
    public boolean verifyCredentials(final String username, final String password);

    /**
     * Checks if the specified api key has access. This function can be used to validator master keys as well as ds
     * level keys.
     * @param apiKey the api key to check
     * @return <code>true</code> if the key is valid; <code>false</code> otherwise
     */
    public boolean verifyKey(final String apiKey);

}
