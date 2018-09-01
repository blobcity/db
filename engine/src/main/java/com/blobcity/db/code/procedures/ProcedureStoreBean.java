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

package com.blobcity.db.code.procedures;

import com.blobcity.db.api.Db;
import com.blobcity.db.bquery.AdapterExecutorBean;
import com.blobcity.db.code.LoaderStore;
import com.blobcity.db.code.RestrictedClassLoader;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.lib.database.bean.manager.common.Constants;
import com.blobcity.lib.database.bean.manager.factory.ModuleApplicationContextHolder;
import com.blobcity.lib.database.launcher.util.ConfigUtil;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Keeps track of all loaded procedures and processes them when they are loaded into db
 *
 * @author sanketsarang
 * @author javatarz (Karun Japhet)
 */
@Component
public class ProcedureStoreBean {

    private static final Logger logger = LoggerFactory.getLogger(ProcedureStoreBean.class.getName());
    @Autowired
    private LoaderStore loaderStore;
    @Autowired
    private ModuleApplicationContextHolder applicationContextHolder;
    private final List<Class> injectableClassList;

    public ProcedureStoreBean() {
        this.injectableClassList = new ArrayList<>();
    }

    @PostConstruct
    private void init() {
        final List<String> injectableClassNamesList = ConfigUtil.getConfigData(Constants.APP_CONFIG_FILE, "/config/inject/class").get("");

        injectableClassNamesList.stream().forEach((injectableClassName) -> {
            try {
                injectableClassList.add(Class.forName(injectableClassName));
            } catch (ClassNotFoundException ex) {
                logger.error("Unable to load injectable class", ex);
            }
        });
    }

    /**
     * Outer map keyed on appId, inner map keyed on fully qualified name of the class.
     * appId->Full Class Name ->Class
     */
    private Map<String, Map<String, Class>> classMap = new HashMap<>();
    /**
     * Outer map keyed on appId, inner map keyed on name parameter of {@link @ProcedureStoreBean} annotation
     * appId->ProcedureStoreName (as mentioned in{@link @ProcedureStoreBea})->class
     */
    private final Map<String, Map<String, Class>> nameMap = new HashMap<>();
    /**
     * Outer map keyed on appId, inner map keyed on name parameter of {@link @ProcedureStoreBean} annotation appended with name parameter of
     * {@link @NamedProcedure} within the specific class; separated by '.'. Example name is ProcedureStoreBean.namedProcedure1
     * appId->ProcedureStoreName.NamedProcedure->Method(Named Procedure)
     */
    private final Map<String, Map<String, Method>> methodMap = new HashMap<>();
    /**
     * Outer map is keyed on appId. 
     * appId->ProcedureStoreName->FieldType-FieldName->field
     */
    private final Map<String, Map<String, Map<String, Field>>> fieldMap  = new HashMap<>();

    public Class getClass(String appId, String className) {
        return classMap.get(appId).get(className);
    }

    public Object getNewInstance(final String requestId, String appId, String procedureStoreName) throws OperationException {
        try {
            if (!nameMap.containsKey(appId) || !nameMap.get(appId).containsKey(procedureStoreName)) {
                throw new OperationException(ErrorCode.STORED_PROCEDURE_NOT_LOADED, "Could not find procedure store " + procedureStoreName);
            }

            final Object instance = nameMap.get(appId).get(procedureStoreName).newInstance();
            for (final Map.Entry<String, Field> appEntry : fieldMap.get(appId).get(procedureStoreName).entrySet()) {
                final Field field = appEntry.getValue();
                final boolean isAccessible = field.isAccessible();

                try {
                    if (isAccessible) {
                        field.setAccessible(true);
                    }
                    if(field.getType() == Db.class || field.getType() == AdapterExecutorBean.class) {
                        AdapterExecutorBean adapterExecutorBean = (AdapterExecutorBean) applicationContextHolder.getApplicationContext().getBean(field.getType());
                        adapterExecutorBean.setRequest(requestId);
                        field.set(instance, adapterExecutorBean);
                    }else {
                        field.set(instance, applicationContextHolder.getApplicationContext().getBean(field.getType()));
                    }
                } finally { // Though this try shouldn't be required, it's been added to avoid fields having incorrect accessibility due to errors
                    if (isAccessible) {
                        field.setAccessible(false);
                    }
                }
            }

            return instance;
        } catch (InstantiationException | IllegalAccessException ex) {
            logger.error("Error occurred when attempting to instatiate procedure store named \"" + procedureStoreName + "\" under app id \"" + appId + "\"", ex);
            throw new OperationException(ErrorCode.STORED_PROCEDURE_EXECUTION_ERROR, "Could not instantiate procedure store " + procedureStoreName);
        }
    }

    public Method getNamedProcedure(String appId, String namedProcedure) throws OperationException {
        if (!methodMap.containsKey(appId)) {
            throw new OperationException(ErrorCode.STORED_PROCEDURE_NOT_LOADED, "No stored procedures found loaded for app " + appId);
        }

        if (!methodMap.get(appId).containsKey(namedProcedure)) {
            throw new OperationException(ErrorCode.STORED_PROCEDURE_NOT_LOADED, "No stored procedures found with name " + namedProcedure);
        }

        return methodMap.get(appId).get(namedProcedure);
    }

    public void loadClasses(final String app, final List<String> classesList) throws OperationException {
        logger.trace("Loading data for app \"{}\"", app);

        if (classMap.containsKey(app)) {
            logger.debug("Clearing class map for app \"{}\"", app);
            classMap.clear();
        } else {
            logger.debug("Creating new entry in class map for app \"{}\"", app);
            classMap.put(app, new HashMap<>());
        }

        if (methodMap.containsKey(app)) {
            logger.debug("Clearing method map for app \"{}\"", app);
            methodMap.clear();
        } else {
            logger.debug("Creating new entry in method map for app \"{}\"", app);
            methodMap.put(app, new HashMap<>());
        }

        if (nameMap.containsKey(app)) {
            logger.debug("Clearing name map for app \"{}\"", app);
            nameMap.clear();
        } else {
            logger.debug("Creating new entry in name map for app \"{}\"", app);
            nameMap.put(app, new HashMap<>());
        }

        try {
            for (String className : classesList) {
                loadClass(app, className);
            }
        } catch (OperationException ex) {
            classMap.remove(app);
            methodMap.remove(app);
            nameMap.remove(app);
            throw new OperationException(ex.getErrorCode(), ex.getMessage());
        }
    }

    public void loadClass(String appId, String className) throws OperationException {
        RestrictedClassLoader blobCityClassLoader = loaderStore.getLoaderWithCreate(appId);

        if (blobCityClassLoader.isReloadRequired(className)) {
            logger.debug("Reload required for {}", className);
            classMap.remove(appId);
            methodMap.remove(appId);
            nameMap.remove(appId);
            blobCityClassLoader = loaderStore.getNewLoader(appId);
        }

        try {
            final Class loadedClass = blobCityClassLoader.loadClass(className, true);
            processClass(appId, loadedClass);
        } catch (ClassNotFoundException ex) {
            logger.error("Error while loading class \"" + className + "\" for appl id \"" + appId + "\"", ex);
            logger.error(ex.getMessage(), ex);
            throw new OperationException(ErrorCode.CLASS_LOAD_ERROR, "Could not load class. " + ex.getMessage());
        }
    }

    public void removeClass(String appId, String className) {
        classMap.get(appId).remove(className);
    }

    public void removeAll(String appId) {
        logger.debug("Removing all stored procedures for app \"{}\" ...", appId);
        logger.debug("Clearing class map for app \"{}\"", appId);
        classMap.remove(appId);
        logger.debug("Clearing name map for app \"{}\"", appId);
        nameMap.remove(appId);
        logger.debug("Clearing method map for app \"{}\"", appId);
        methodMap.remove(appId);
        logger.debug("Clearing field map for app \"{}\"", appId);
        fieldMap.remove(appId);
        logger.debug("All stored procedures removed for app \"{}\"", appId);
    }

    /**
     * Empties the map and destroys all instances
     */
    public void invalidate() {
        classMap = new HashMap<>();
    }

    private void processClass(final String app, final Class clazz) throws OperationException {
        String procedureStoreName = null;
        /* Check if the class has an @ProcedureStoreBean annotation and if so pick up the name of the procedure store*/
        logger.trace("processClass({}, {})", new Object[]{app, clazz.getCanonicalName()});

        final Annotation[] annotationArray = clazz.getAnnotations();
        for (final Annotation annotation : annotationArray) {
            logger.debug("Processing {} => Annotation: {}", new Object[]{clazz.getCanonicalName(), annotation.annotationType().getCanonicalName()});

            //TODO: Get annotation with the getAnnotation method. Done this way because the method "no work" for unknown reasons
            if ("com.blobcity.db.annotations.ProcedureStore".equals(annotation.annotationType().getCanonicalName())) {
                if (procedureStoreName == null) {
                    procedureStoreName = annotation.toString().substring(annotation.toString().indexOf("name=") + 5, annotation.toString().length() - 1);
                    logger.debug("{} found annotation ProcedureStoreBean[name={}]", new Object[]{clazz.getCanonicalName(), procedureStoreName});
                } else {
                    logger.debug("{} found duplicate annotation ProcedureStoreBean[name={}]", new Object[]{clazz.getCanonicalName(), procedureStoreName});
                    throw new OperationException(ErrorCode.STORED_PROCEDURE_LOAD_ERROR, "Duplicate @ProcedureStore "
                            + "annotation found on class " + clazz.getCanonicalName() + ". A single procedure store class "
                            + "may have only one @ProcedureStore annotation associated with it.");
                }
            }
        }

        if (nameMap.containsKey(app) && nameMap.get(app).containsKey(procedureStoreName)) {
            throw new OperationException(ErrorCode.STORED_PROCEDURE_LOAD_ERROR, "Duplicate Procedure Store found with name " + procedureStoreName);
        }

        if (!classMap.containsKey(app)) {
            classMap.put(app, new HashMap<>());
        }

        if (!nameMap.containsKey(app)) {
            nameMap.put(app, new HashMap<>());
        }

        if (!methodMap.containsKey(app)) {
            methodMap.put(app, new HashMap<>());
        }

        if (!fieldMap.containsKey(app)) {
            fieldMap.put(app, new HashMap<>());
        }
        if(!fieldMap.get(app).containsKey(procedureStoreName)){
            fieldMap.get(app).put(procedureStoreName, new HashMap<>());
        }

        /* Identify all methods with @NamedProcedure annotation */
        final Method[] classMethods = clazz.getMethods();
        logger.debug("Class: {} | Found method count: {}", new Object[]{clazz.toString(), classMethods.length});
        for (Method method : classMethods) {
            String procedureName = null;

            final Annotation[] methodAnnotationArray = method.getAnnotations();
            logger.debug("Reading {} from {} which has {} annotations", new Object[]{method.getName(), clazz.toString(), methodAnnotationArray.length});

            for (final Annotation methodAnnotation : methodAnnotationArray) {
                if ("com.blobcity.db.annotations.NamedProcedure".equals(methodAnnotation.annotationType().getCanonicalName())) {
                    if (procedureName == null) {
                        procedureName = methodAnnotation.toString().substring(methodAnnotation.toString().indexOf("name=") + 5, methodAnnotation.toString().length() - 1);
                        logger.debug("Found named procedure: {}.{}", new Object[]{procedureStoreName, procedureName});
                    } else {
                        logger.debug("Found duplicate named procedure annotation in procedure store {} for method with name {}", new Object[]{procedureStoreName, method.getName()});
                        throw new OperationException(ErrorCode.STORED_PROCEDURE_LOAD_ERROR, "Duplicate @NamedProcedure "
                                + "annotation found on method " + method.getName() + " of class " + clazz.getCanonicalName()
                                + ". A single procedure store method may have only one @NamedProcedure annotation associated with it.");
                    }
                }
            }

            if (procedureName == null) {
                continue;
            }

            final String namedProcedure = procedureStoreName + "." + procedureName;

            if (methodMap.containsKey(app) && methodMap.get(app).containsKey(namedProcedure)) {
                throw new OperationException(ErrorCode.STORED_PROCEDURE_LOAD_ERROR, "Duplicate named procedure found in app " + app + " with name " + namedProcedure);
            }

            methodMap.get(app).put(namedProcedure, method);
            logger.debug("Registered named procedure: {}", namedProcedure);
        }

        classMap.get(app).put(clazz.getCanonicalName(), clazz);
        nameMap.get(app).put(procedureStoreName, clazz);

        final Field[] fields = clazz.getDeclaredFields();
        logger.debug("Class: {} | Found field count: {}", new Object[]{clazz.toString(), fields.length});
        for (final Field field : fields) {
            logger.debug("Found field: {}", field.getType().toString());

            if (field.isAnnotationPresent(Inject.class) && injectableClassList.contains(field.getType())) {
                logger.debug("Marking {} for injection", field);
                // maped to field.class-field.name to ensure that all fiels have a different entry
                fieldMap.get(app).get(procedureStoreName).put(field.getType().getCanonicalName()+"-"+field.getName(), field);
            }
        }
    }

    public void processWebService(Class clazz) {

    }
    
    public List<String> listProcedures(final String app){
        if ( !nameMap.containsKey(app))
            return Collections.EMPTY_LIST;
        return new ArrayList<>( methodMap.get(app).keySet());
    }

}