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

package com.blobcity.db.code.webservices;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sp.RestWebService;
import com.blobcity.db.sp.annotations.Rest;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sanketsarang
 */
@Component //singleton bean
public class WebServiceStore {

    private final Map<String, Map<String, Class>> wsMap = new HashMap<>();

    public void loadWebServiceCode(final String datastore, final Class clazz) throws OperationException {

        if(!clazz.getName().startsWith("com.blobcity")) {
            return;
        }

        Object instance = null;
        try {
            instance = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.CODE_LOAD_ERROR);
        }

        Annotation restAnnotation = null;
        Annotation [] annotations = instance.getClass().getAnnotations();
        for(int i=0; i < annotations.length; i++) {
            Annotation annotation = annotations[i];
            System.out.println("Found annotation : " + annotation.toString());
            if(annotation.annotationType().equals(Rest.class)) {
                System.out.println("Found the @Rest annotation");
                restAnnotation = annotation;
            }
        }

        if(restAnnotation == null) {
            System.out.println("Cannot process WebService class as @Rest annotation was missing");
            throw new OperationException(ErrorCode.CODE_LOAD_ERROR, "Missing @Rest annotation on a WebService implementation");
        }

        Rest rest = (Rest) restAnnotation;
        registerWS(datastore, rest.path(), clazz);
    }

    private void registerWS(final String datastore, final String path, final Class ws) {
        if(!wsMap.containsKey(datastore)) {
            wsMap.put(datastore, new HashMap<>());
        }

        wsMap.get(datastore).put(path, ws);
    }

    public void unregisterWs(final String datastore, final String path) {
        Map<String, Class> dsMap = wsMap.get(datastore);

        if(dsMap != null) {
            dsMap.remove(path);
        }
    }

    public void unregisterWs(final String datastore) {
        wsMap.remove(datastore);
    }

    public RestWebService getNewInstance(final String datastore, final String path) throws OperationException {
        Map<String, Class> dsMap = wsMap.get(datastore);

        if(dsMap == null) {
            throw new OperationException(ErrorCode.INVALID_WEBSERVICE_ENDPOINT);
        }

        Class restWebServiceClass = dsMap.get(path);

        if(restWebServiceClass == null) {
            throw new OperationException(ErrorCode.INVALID_WEBSERVICE_ENDPOINT);
        }

        try {
            return (RestWebService) restWebServiceClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.CODE_LOAD_ERROR);
        }
    }
}
