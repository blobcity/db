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

package com.blobcity.lib.database.bean.manager.factory;

import org.springframework.context.ApplicationContext;

/**
 * Provides generalized structure for the bean configurations of all projects
 *
 * @author javatarz Karun Japhet
 */
public interface ModuleApplicationContextHolder {

    /**
     * @return {@link ApplicationContext} for the project to be used for intra-project bean access when beans cannot be auto-wired
     */
    public ApplicationContext getApplicationContext();

    /**
     * Allows the application context to be set for application for intra project bean access when beans cannot be auto-wired
     *
     * @param applicationContext {@link ApplicationContext} for the project
     */
    public void setApplicationContext(final ApplicationContext applicationContext);
}
