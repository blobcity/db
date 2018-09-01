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

package com.blobcity.pom.database.engine.bean;

import com.blobcity.lib.database.bean.manager.factory.ModuleApplicationContextHolder;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * Holds the instance {@link ApplicationContext} associated with the project
 *
 * @author javatarz (Karun Japhet)
 */
public class ApplicationContextHolder implements ModuleApplicationContextHolder {

    private ApplicationContext applicationContext;

    @Override
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) {
        LoggerFactory.getLogger(ApplicationContextHolder.class).trace("Setting an instance of " + applicationContext.toString() + " to " + this.getClass());

        this.applicationContext = applicationContext;
    }
}
