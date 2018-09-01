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

package com.blobcity.lib.database.tcp.end.point.factory;

import com.blobcity.lib.database.bean.manager.factory.ModuleApplicationContextHolder;
import com.blobcity.lib.database.tcp.end.point.bean.ApplicationContextHolder;
import com.blobcity.lib.database.tcp.end.point.bean.TcpEndPointServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

/**
 * Handles bean definition for the project
 *
 * @author javatarz (Karun Japhet)
 */
public class TcpEndPointBeanConfig {

    private static final Logger logger = LoggerFactory.getLogger(TcpEndPointBeanConfig.class);

    /*
     * Startup beans: Start
     */
    @Bean
    public TcpEndPointServer tcpEndPointServer() { // startup bean
        logger.trace("Creating an instance of TcpEndPointServer");

        return new TcpEndPointServer();
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
