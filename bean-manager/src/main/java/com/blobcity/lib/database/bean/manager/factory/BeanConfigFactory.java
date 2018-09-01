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

import java.util.HashMap;
import java.util.Map;
import org.springframework.context.ApplicationContext;

/**
 * Class to hold {@link ApplicationContext}s for all projects centrally to allow cross project access.
 *
 * @author javatarz (Karun Japhet)
 */
public class BeanConfigFactory {

    public static final Map<String, ApplicationContext> beanConfigMap = new HashMap<>();

    public static ApplicationContext addApplicationContext(final String projectName, final ApplicationContext context) {
        context.getBean(ModuleApplicationContextHolder.class).setApplicationContext(context);
        return beanConfigMap.put(projectName, context);
    }

    public static ApplicationContext getConfigBean(final String projectName) {
        return beanConfigMap.get(projectName);
    }
}
