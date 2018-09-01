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

package com.blobcity.lib.database.web.end.point.factory;

import com.blobcity.lib.database.bean.manager.factory.ModuleApplicationContextHolder;
import com.blobcity.lib.database.web.end.point.bean.ApplicationContextHolder;
import com.blobcity.lib.database.web.end.point.bean.DbEndPointServer;
import com.blobcity.lib.database.web.end.point.bean.InternalEndPointServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * Handles bean definition for the project
 *
 * @author sanketsarang
 */
@Configuration
public class WebEndPointBeanConfig {

    private static final Logger logger = LoggerFactory.getLogger(WebEndPointBeanConfig.class);

    /*
     * Startup beans: Start
     */
    @Bean
    public DbEndPointServer dbEndPointServer() { // startup bean
        logger.trace("Creating an instance of DbEndPointServer");

        return new DbEndPointServer();
    }

    @Bean
    public InternalEndPointServer internalEndPointServer() { // startup bean
        logger.trace("Creating an instance of InternalEndPointServer");

        return new InternalEndPointServer();
    }
    /*
     * Startup beans: End
     */

    /*
     * Lazy Singletons: Start
     */
    @Bean
    @Lazy
    public ModuleApplicationContextHolder applicationContextHolder() { // singleton bean
        logger.trace("Creating an instance of ApplicationContextHolder");

        return new ApplicationContextHolder();
    }
    /*
     * Lazy Singletons: End
     */
}
