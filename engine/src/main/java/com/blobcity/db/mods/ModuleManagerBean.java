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

package com.blobcity.db.mods;

import com.blobcity.db.exceptions.OperationException;

/**
 * @author sanketsarang
 */
public class ModuleManagerBean {

    /**
     * Loads module with the specified name by unloading a module with the same name first if one is already loaded.
     * This function is also to be used for first time loading of modules.
     * @param moduleName name of module to load / reload
     * @throws OperationException if an exception occurs
     */
    public void reloadModule(final String moduleName) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Unloads a module with the specified name if a module with the specified name is currently loaded. The operation
     * is a no-op if no module with the specified name is found
     * @param moduleName name of module
     * @throws OperationException if an exception occurs
     */
    public void unloadModule(final String moduleName) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
