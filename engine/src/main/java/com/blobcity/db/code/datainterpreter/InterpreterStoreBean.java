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

package com.blobcity.db.code.datainterpreter;

import com.blobcity.db.annotations.Interpreter;
import com.blobcity.db.code.LoaderStore;
import com.blobcity.db.code.ManifestParserBean;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sp.interpreter.DataInterpretable;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This bean stores all the data interpreters information and process the interpreter class when they loaded
 * 
 * @author sanketsarang
 */
@Component
public class InterpreterStoreBean {

    private static final Logger logger = LoggerFactory.getLogger(InterpreterStoreBean.class.getName());

    @Autowired
    private LoaderStore loaderStore;
    @Autowired
    private ManifestParserBean manifestParser;
    
    /**
     * Outer key mapped on dsSet name and inner key map on interpreterName with classFileName
     * ( dsSet ->( interpreterName(defined in annotation) -> ClassFile Full Name ))
     */
    private final Map<String, Map<String, String>> interpreterMap = new HashMap<>();
    private final Map<String, Map<String, Class>> classMap = new HashMap<>();

    /**
     * whether a given interpreter is present (loaded in memory)
     *
     * @param datastore : dsSet name
     * @param interpreterName : name of interpreter as defined in annotation DataInterpreter
     * @return true if loaded, false otherwise
     */
    public boolean isPresent(final String datastore, final String interpreterName){
        if(!classMap.containsKey(datastore) ) return false;
        return classMap.get(datastore).containsKey(interpreterName);
    }
    
    /**
     * Get the class name associated with given interpreter
     *
     * @param datastore : dsSet name
     * @param interpreterName : name of interpreter as defined in annotation DataInterpreter
     * @return canonical name of Class associated with given interpreter Name
     */
    public String getClassName(final String datastore, final String interpreterName) {
        return classMap.get(datastore).get(interpreterName).getName();
    }

    /**
     * Get all interpreters present in the dsSet
     * @param datastore : dsSet name
     * @return list of names of all interpreter
     */
    public List<String> getInterpreters(final String datastore){
        if(!classMap.containsKey(datastore))
            return Collections.EMPTY_LIST;
        return new ArrayList<>(classMap.get(datastore).keySet());
    }

    /**
     * unload all interpreters
     */
    public void invalidate(){
        interpreterMap.clear();
        classMap.clear();
    }

    /**
     * Load the given list of class files related to data interpreters
     *
     * @param datastore : dsSet name
     * @param classList : class names of all interpreters
     * @throws OperationException
     */
    public void loadClasses(final String datastore, final List<Class> classList) throws OperationException{
        logger.trace("Loading datainterpreters for app \"{}\"", datastore);
              
        if (interpreterMap.containsKey(datastore)) {
            logger.debug("Clearing activation map for app \"{}\"", datastore);
            interpreterMap.clear();
            classMap.clear();
        } else {
            logger.debug("Creating new entry in sequence map for app \"{}\"", datastore);
            interpreterMap.put(datastore, new HashMap<>());
            classMap.put(datastore, new HashMap<>());
        }

        try {
            for (Class className : classList) {
                System.out.println("Attempting to load class: " + className.getName());
                loadClass(datastore, className);
            }
        } catch (OperationException ex) {
            interpreterMap.remove(datastore);
            classMap.remove(datastore);
            throw new OperationException(ex.getErrorCode(), ex.getMessage());
        }
    }

    /**
     * Load a given data interpreter class file
     *
     * @param datastore: dsSet name
     * @param className: full name of class file
     * @throws OperationException
     */
    public void loadClass(final String datastore, final Class clazz) throws OperationException{
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

        Annotation interpreterAnnotation = null;
        Annotation [] annotations = instance.getClass().getAnnotations();
        for(int i=0; i < annotations.length; i++) {
            Annotation annotation = annotations[i];
            if(annotation.annotationType().equals(Interpreter.class)) {
                logger.trace("Found the @Interpreter annotation: " + annotation.toString());
                interpreterAnnotation = annotation;
            }
        }

        if(interpreterAnnotation == null) {
            logger.warn("Cannot process Interpreter class as @Interpreter annotation was missing");
            throw new OperationException(ErrorCode.CODE_LOAD_ERROR, "Missing @Interpreter annotation on a DataInterpreter implementation");
        }

        Interpreter annotation = (Interpreter) interpreterAnnotation;
        registerInterpreter(datastore, annotation.name(), clazz);


//        RestrictedClassLoader blobCityClassLoader = loaderStore.getLoaderWithCreate(datastore);
//
//        if (blobCityClassLoader.isReloadRequired(className)) {
//            logger.debug("Reload required for {}", className);
//            interpreterMap.remove(datastore);
//            classMap.remove(datastore);
//            blobCityClassLoader = loaderStore.getNewLoader(datastore);
//        }
//
//        try {
//            final Class loadedClass = blobCityClassLoader.loadClass(className, true);
//            processClass(datastore, loadedClass);
//        } catch (ClassNotFoundException ex) {
//            logger.error("Error while loading interpreter class \"" + className + "\" for app id \"" + datastore + "\"", ex);
//            logger.error(null, ex);
//            throw new OperationException(ErrorCode.CLASS_LOAD_ERROR, "Could not load interpreter class. " + ex.getMessage());
//        }
    }

    public void registerInterpreter(final String ds, final String name, final Class interpreter) {
        if(!classMap.containsKey(ds)) {
            classMap.put(ds, new HashMap<>());
        }
        classMap.get(ds).put(name, interpreter);
        logger.debug("Registered interpreter: " + name);
    }

    /**
     * process a given interpreter class to check for all requirements for a data interpreter
     *
     * @param datastore : dsSet name
     * @param interpreterClass : class for the interpreter
     * @throws OperationException:
     *  1. if interface is not implemented
     *  2. if not defined in manifest file
     *  3. if no DataInterpreter found
     *  4. if more than one annotation found
     */
    public void processClass(final String datastore, final Class interpreterClass) throws OperationException{
        // checking if interface is implemented or not.
        if( !DataInterpretable.class.isAssignableFrom(interpreterClass)) {
            throw new OperationException(ErrorCode.DATAINTERPRETER_LOAD_ERROR, "Specified datainterpreter " + interpreterClass.getCanonicalName() + " inside db " + datastore + " doesn't implement DataInterpretable interface" );
        }
        
        logger.trace("processClass({}, {}) as interpreter", new Object[]{datastore, interpreterClass.getCanonicalName()});
        // checking for interpreters in Manifest file
        List<String> interpreters = manifestParser.getInterpreters(datastore);
        if (!interpreters.contains(interpreterClass.getCanonicalName())){
            throw new OperationException(ErrorCode.DATAINTERPRETER_LOAD_ERROR, "No such Interpreter is defined in Manifest file");
        }
        
        String interpreterName = null;
        // checking for 'DataInterpreter' annotation
        final Annotation[] annotations = interpreterClass.getAnnotations();
        for(Annotation annotation: annotations){
            if( "com.blobcity.db.annotations.Interpreter".equals(annotation.annotationType().getCanonicalName()) ){
                // name is supplied in annotation or not
                if(interpreterName == null){
                    if(annotation.toString().contains("name")){
                        String interpreterAnnotation = annotation.toString();
                        interpreterName = interpreterAnnotation.substring(interpreterAnnotation.indexOf("name=") + 5, interpreterAnnotation.length()-1);
                    }
                    else{
                        interpreterName = interpreterClass.getSimpleName();
                        logger.debug("{} found annotation Interpreter with interpreter name {}", new Object[]{interpreterClass.getCanonicalName(), interpreterName});
                    }
                }
                else{
                    logger.warn("{} found duplicate annotation Interpreter", new Object[]{interpreterClass.getCanonicalName()});
                    throw new OperationException(ErrorCode.DATAINTERPRETER_LOAD_ERROR, "Duplicate @Interpreter "
                            + "annotation found on class " + interpreterClass.getCanonicalName() + ". A single interpreter class "
                            + "may have only one @DataInterpreter annotation associated with it.");
                }
            }
        }

        if(!interpreterMap.containsKey(datastore)){
            interpreterMap.put(datastore, new HashMap<>());
            classMap.put(datastore, new HashMap<>());
        }
        
        interpreterMap.get(datastore).put(interpreterName, interpreterClass.getCanonicalName());
        classMap.get(datastore).put(interpreterName, interpreterClass);
    }

    public Object getNewInstance(final String datastore, final String interpreterName) {
        try {
            return classMap.get(datastore).get(interpreterName).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Class getClass(final String datastore, final String interpreterName) {
        return classMap.get(datastore).get(interpreterName);
    }

    /**
     * remove/ unload all interpreters from database for a given dsSet
     *
     * @param datastore : dsSet name
     */
    public void removeAll(String datastore){
        interpreterMap.remove(datastore);
        classMap.remove(datastore);
    }

    /**
     * remove/unload a given interpreter from database. This will not delete it from the disk
     *
     * @param datastore : dsSet name
     * @param interpreterName : name of interpreter as defined in annotation DataInterpreter
     */
    public void removeClass(String datastore, String interpreterName){
        interpreterMap.get(datastore).remove(interpreterName);
        classMap.get(datastore).remove(interpreterName);
    }

}
