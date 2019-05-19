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

package com.blobcity.db.code;

import com.blobcity.db.annotations.Interpreter;
import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlDatastoreManager;
import com.blobcity.db.code.datainterpreter.InterpreterStoreBean;
import com.blobcity.db.code.filters.FilterStoreBean;
import com.blobcity.db.code.procedures.ProcedureStoreBean;
import com.blobcity.db.code.triggers.TriggerStoreBean;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.export.ExportProcedureStore;
import com.blobcity.db.sp.interpreter.DataInterpretable;
import com.blobcity.db.sp.*;
import com.blobcity.db.sp.annotations.Export;
import com.blobcity.db.sp.annotations.Named;
import com.blobcity.db.sp.annotations.Rest;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.code.webservices.WebServiceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This bean is responsible for loading user-provided code (trigger, filter, stored procedures) into database
 * 
 * @author sanketsarang
 */
@Component
public class CodeLoader {
    private static final Logger logger = LoggerFactory.getLogger(CodeLoader.class);
    
    @Autowired
    private BSqlCollectionManager collectionManager;
    @Autowired
    private BSqlDatastoreManager datastoreManager;
    @Autowired
    private FilterStoreBean filterStore;
    @Autowired
    private InterpreterStoreBean interpreterStore;
    @Autowired
    private ManifestParserBean manifestParser;
    @Autowired
    private ProcedureStoreBean procedureStore;
    @Autowired
    private TriggerStoreBean triggerStore;
    @Autowired
    private WebServiceStore webServiceStore;
    @Autowired
    private ExportProcedureStore exportProcedureStore;

    /**
     * activate trigger/triggers specified related to specified dsSet and collection
     *
     * @param datastore : name of dsSet
     * @param collection: name of collection
     * @param triggerName : name of trigger to be activated
     * @throws OperationException
     */
    public void activateTrigger(final String datastore, final String collection, final String triggerName) throws OperationException{
        if( !datastoreManager.exists(datastore) )
            throw new OperationException(ErrorCode.DATASTORE_INVALID, "Given dsSet " + datastore + " doesn't exist");
        if( !collectionManager.exists(datastore, collection) )
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No collection named " + collection + " found inside dsSet " + datastore);

        if( triggerName.equals("*"))
            triggerStore.activateTrigger(datastore, collection);
        else
            triggerStore.activateTrigger(datastore, collection, triggerName);
    }

    /**
     * deactivate specified trigger/triggers related to the given dsSet and collection
     *
     * @param datastore : dsSet
     * @param collection : collection
     * @param triggerName : name of trigger to be deactivated
     * @throws OperationException
     */
    public void deActivateTrigger(final String datastore, final String collection, final String triggerName) throws OperationException{
        if( !datastoreManager.exists(datastore) )
            throw new OperationException(ErrorCode.DATASTORE_INVALID, "Given dsSet " + datastore + " doesn't exist");
        if( !collectionManager.exists(datastore, collection) )
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No collection named " + collection + " found inside dsSet " + datastore);

        if(triggerName.equals("*"))
            triggerStore.deActivateTrigger(datastore, collection);
        else
            triggerStore.deActivateTrigger(datastore, collection, triggerName);
    }

    /**
     * Unload all classes from database and then delete them from disk also
     *
     * @param datastore : name of dsSet
     * @throws OperationException
     */
    public void deleteAllClasses(final String datastore) throws OperationException{
        if( !datastoreManager.exists(datastore) )
            throw new OperationException(ErrorCode.DATASTORE_INVALID, "Given dsSet " + datastore + " doesn't exist");
        // it will delete all classes from filesystem.
        //TODO: ask  Sanket if we want to support this.
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED , "This is not yet supported");
    }

    /**
     * list all the codes loaded in the database
     *
     * @param datastore : dsSet
     * @return
     * @throws OperationException
     */
    public String listAllCode(final String datastore) throws OperationException{
        StringBuilder sb = new StringBuilder();
        sb.append("Following classes are loaded for this dsSet:");
        sb.append("\nPROCEDURES:\t");
        procedureStore.listProcedures(datastore).stream().forEach((procedure) -> {
            sb.append(procedure).append(", ");
        });
        sb.append("\nTRIGGERS:\t");
        triggerStore.getTriggers(datastore).stream().forEach((trigger) -> {
            sb.append(trigger).append(", ");
        });
        sb.append("\nFILTERS:\t");
        filterStore.getFilters(datastore).stream().forEach((filter) -> {
            sb.append(filter).append(", ");
        });
        sb.append("\nINTERPRETERS:\t");
        interpreterStore.getInterpreters(datastore).stream().forEach((interpreter) -> {
            sb.append(interpreter).append(", ");
        });

        return sb.toString();
    }

    /**
     * this is a debug function, this prints out the trigger map as loaded in database memory
     *
     * @param datastore : dsSet
     * @return list of trigger related to given database
     * @throws OperationException
     */
    public String listTriggers(final String datastore) throws OperationException{
        if( !datastoreManager.exists(datastore) )
            throw new OperationException(ErrorCode.DATASTORE_INVALID, "Given dsSet " + datastore + " doesn't exist");
        return triggerStore.getTriggerMappings(datastore);
    }

    /**
     *
     * @param datastore : dsSet
     * @param collection : collection
     * @return list of triggers associated with given dsSet and collection
     * @throws OperationException
     */
    public String listTriggers(final String datastore, final String collection) throws OperationException{
        if( !collectionManager.exists(datastore, collection))
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No collection named " + collection + " found inside dsSet " + datastore);
        return triggerStore.getTriggerMappings(datastore, collection);
    }

    /**
     * Loading all classes added by user (triggers and procedures both) in database
     * this is the entry point to this class
     * 
     * @param datastore : name of dsSet
     * @throws OperationException 
     */
    public void loadAllClasses(final String datastore) throws OperationException{
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Depricated operation. Use loadJar() instead");

//        if( !datastoreManager.exists(datastore) )
//            throw new OperationException(ErrorCode.DATASTORE_INVALID, "Given dsSet " + datastore + " doesn't exist");
//
//        List<String> allProcedures = manifestParser.getProcedures(datastore);
//        List<String> allTriggers = manifestParser.getTriggers(datastore);
//        List<String> allFilters = manifestParser.getFilters(datastore);
//        List<String> allInterpreters = manifestParser.getInterpreters(datastore);
//        try{
//            procedureStore.loadClasses(datastore, allProcedures);
//            triggerStore.loadClasses(datastore, allTriggers);
//            filterStore.loadClasses(datastore, allFilters);
//            interpreterStore.loadClasses(datastore, allInterpreters);
//        }
//        catch(OperationException ex){
//            logger.error("Error while loading user classes into database for dsSet: "+ datastore, ex);
//            procedureStore.removeAll(datastore);
//            triggerStore.removeAll(datastore);
//            filterStore.removeAll(datastore);
//            interpreterStore.removeAll(datastore);
//            throw new OperationException(ex.getErrorCode(), ex.getMessage());
//        }
    }

    public void loadJar(final String datastore, final String jarFilePath) throws OperationException {
        logger.info("Attempting to load jar: " + jarFilePath);

        if(jarFilePath.contains("..")){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Relative path for jar file location not permitted. Consider using absolute path instead");
        }

        List<String> classNames = new ArrayList<>();
        ZipInputStream zip = null;
        String jarPath;
        try {
            jarPath = new File(PathUtil.getCustomCodeJarFilePath(datastore, jarFilePath)).getCanonicalPath();
            zip = new ZipInputStream(new FileInputStream(jarPath));
            for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                if (!entry.isDirectory() && entry.getName().endsWith(".class")) {
                    // This ZipEntry represents a class. Now, what class does it represent?
                    String className = entry.getName().replace('/', '.'); // including ".class"
                    classNames.add(className.substring(0, className.length() - ".class".length()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        URLClassLoader loader;
        try {
            loader = new URLClassLoader(new URL[]{new URL("file:" + jarPath)});
            classNames.forEach(clazz -> {
                try {
                    System.out.println(clazz.toString());
                    if(!clazz.toString().startsWith("com.blobcity")) {

                        return;
                    }
                    Class loadedClazz = loader.loadClass(clazz);
                    Object instance;
                    try {
                        if(loadedClazz.isAnnotationPresent(Rest.class) || loadedClazz.isAnnotationPresent(Named.class)
                                || loadedClazz.isAnnotationPresent(Export.class) || loadedClazz.isAnnotationPresent(Interpreter.class)){
                            instance = loadedClazz.newInstance();
                            logger.debug("Instance: " + instance.getClass().getName());
                        } else {
                            return;
                        }
                    } catch (InstantiationException | IllegalAccessException e) {
                        logger.error("Error loading class " + loadedClazz.getCanonicalName(), e);
                        return;
                    }

                    if(instance instanceof RestWebService) {
                        try {
                            webServiceStore.loadWebServiceCode(datastore, loadedClazz);
                        } catch (OperationException e) {
                            e.printStackTrace();
                        }
                        return;
                    } else if(instance instanceof InsertTrigger) {
                        return;
                    } else if(instance instanceof UpdateTrigger) {
                        return;
                    } else if(instance instanceof DeleteTrigger) {
                        return;
                    } else if(instance instanceof DataExporter) {
                        exportProcedureStore.loadExporter(datastore, loadedClazz);
                    } else if(instance instanceof DataInterpretable) {
                        interpreterStore.loadClass(datastore, loadedClazz);
                    }

                    Method []methods = loadedClazz.getMethods();

                    for(int i=0; i<methods.length; i++) {
                        Method method = methods[i];

                        Annotation []annotations = method.getAnnotations();
                        for(int j=0; j < annotations.length; j++) {
                            Annotation annotation = annotations[j];

                            if(annotation.annotationType().equals(Named.class)) {
                                logger.debug("Found a named procedure");
                            }

                            logger.debug(annotations[j].toString());
                        }
                    }
                } catch (Exception ex) {
                    logger.warn("Error loading class", ex);
                }
            });
        } catch (MalformedURLException e) {
            logger.error("Malformed URL Exception when loading stored procedures JAR", e);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error in loading code for datastore " + datastore + " from jar " + jarFilePath);
        }

        logger.debug("Stored Procedures JAR loaded");
    }
    
    /**
     * remove all classes added by user from the database. This will not delete them from disk.
     * 
     * @param datastore : name of dsSet
     * @throws OperationException 
     */
    public void removeAllClasses(final String datastore) throws OperationException{
        if( !datastoreManager.exists(datastore) )
            throw new OperationException(ErrorCode.DATASTORE_INVALID, "Given dsSet " + datastore + " doesn't exist");
        
        procedureStore.removeAll(datastore);
        triggerStore.removeAll(datastore);
        filterStore.removeAll(datastore);
        interpreterStore.removeAll(datastore);
        webServiceStore.unregisterWs(datastore);
    }

}
