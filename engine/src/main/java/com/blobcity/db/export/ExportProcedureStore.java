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

package com.blobcity.db.export;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sp.DataExporter;
import com.blobcity.db.sp.annotations.Export;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sanketsarang
 */
@Component
public class ExportProcedureStore {
    private static final Logger logger = LoggerFactory.getLogger(ExportProcedureStore.class.getName());

    private final Map<String, Map<String, Class>> exporters = new HashMap<>();

    public void loadExporter(final String ds, Class clazz) throws OperationException {
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

        Annotation exportAnnotation = null;
        Annotation [] annotations = instance.getClass().getAnnotations();
        for(int i=0; i < annotations.length; i++) {
            Annotation annotation = annotations[i];
            System.out.println("Found annotation : " + annotation.toString());
            if(annotation.annotationType().equals(Export.class)) {
                exportAnnotation = annotation;
            }
        }

        if(exportAnnotation == null) {
            System.out.println("Cannot process WebService class as @Export annotation was missing");
            throw new OperationException(ErrorCode.CODE_LOAD_ERROR, "Missing @Export annotation on a WebService implementation");
        }

        Export export = (Export) exportAnnotation;
        registerExporter(ds, export.name(), clazz);
    }

    public void registerExporter(final String ds, final String name, final Class exporter) {
        if(!exporters.containsKey(ds)) {
            exporters.put(ds, new HashMap<>());
        }
        exporters.get(ds).put(name, exporter);
    }

    public Class getExporter(final String ds, final String name) throws OperationException{
        if(!exporters.containsKey(ds)) {
            throw new OperationException(ErrorCode.STORED_PROCEDURE_NOT_LOADED, "No Export procedures registered for ds: " + ds);
        }

        if(!exporters.get(ds).containsKey(name)) {
            throw new OperationException(ErrorCode.STORED_PROCEDURE_NOT_LOADED, "No Export procedure found with name: " + name);
        }

        return exporters.get(ds).get(name);
    }

    public DataExporter newInstance(final String ds, final String name) throws OperationException {
        try {
            return (DataExporter) getExporter(ds, name).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            logger.error("Error occurred instantiating Export procedure", e);
            throw new OperationException(ErrorCode.STORED_PROCEDURE_EXECUTION_ERROR, "Unable to create instance of " + name);
        }
    }

    public void clear(final String ds) {
        exporters.remove(ds);
    }

    public void remove(final String ds, final String name) {
        if(!exporters.containsKey(ds)) {
            return;
        }

        exporters.get(ds).remove(name);
    }

    public void clearAll() {
        exporters.clear();
    }
}
