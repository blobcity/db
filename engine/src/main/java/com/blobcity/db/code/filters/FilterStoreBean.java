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

package com.blobcity.db.code.filters;

import com.blobcity.db.code.LoaderStore;
import com.blobcity.db.code.ManifestParserBean;
import com.blobcity.db.code.RestrictedClassLoader;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.functions.Filterable;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * For Storing various filter information and processing them while loading
 * filters are universal i.e. not associated with any table, they can be used with any table provided during invoking runtime
 * 
 * @author sanketsarang
 */
@Component
public class FilterStoreBean {
    private static final Logger logger = LoggerFactory.getLogger(FilterStoreBean.class.getName());
    
    @Autowired
    private LoaderStore loaderStore;
    @Autowired
    private ManifestParserBean manifestParser;
    
    /**
     * Outer key mapped on dsSet name and inner key map on filterName with name full of class file present in the deploy-db-hot folder
     * ( dsSet ->( filterName(defined in annotation) -> ClassFile Name ))
     */
    Map<String, Map<String, String>> classMap;

    @PostConstruct
    public void init(){
        classMap = new HashMap<>();
    }
    
    /**
     * Get the name of class file for a given filter name as defined in the annotation
     *
     * @param datastore : dsSet name
     * @param filterName : name of filter as defined in annotation Filter
     * @return canonical name of Class associated with given filter Name
     */
    public String getClass(final String datastore, final String filterName) {
        return classMap.get(datastore).get(filterName);
    }

    /**
     * get the list of filters inside a given dsSet
     *
     * @param datastore : dsSet name
     * @return: list of filters present in this dsSet
     */
    public List<String> getFilters(final String datastore){
        if(!classMap.containsKey(datastore))
            return Collections.EMPTY_LIST;
        return new ArrayList<>(classMap.get(datastore).keySet());
    }

    /**
     * Unload all filters
     */
    public void invalidate(){
        classMap = new HashMap<>();
    }

    /**
     * Check whether a given filter is present(loaded) in a dsSet or not
     *
     * @param datastore : dsSet name
     * @param filterName : filter name as defined in annotation
     * @return true if present, false otherwise
     */
    public boolean isPresent(final String datastore, final String filterName){
        if(!classMap.containsKey(datastore) ) return false;
        return classMap.get(datastore).containsKey(filterName);
    }

    /**
     * Load the list of given filters inside database
     *
     * @param datastore : dsSet name
     * @param classList : list of filter class files with full name as defined in Manifest file
     * @throws OperationException
     */
    public void loadClasses(final String datastore, final List<String> classList) throws OperationException{
        logger.trace("Loading filters for dsSet \"{}\"", datastore);
        
        if(classMap == null ){
            classMap = new HashMap<>();
        }

        if (classMap.containsKey(datastore)) {
            logger.debug("Clearing class map for dsSet \"{}\"", datastore);
            classMap.clear();
        } else {
            logger.debug("Creating new entry in class map for dsSet \"{}\"", datastore);
            classMap.put(datastore, new HashMap<>());
        }

        try {
            for (String className : classList) {
                loadClass(datastore, className);
            }
        } catch (OperationException ex) {
            classMap.remove(datastore);
            throw ex;
        }
    }

    /**
     * Load a given filter class in the database
     *
     * @param datastore : dsSet name
     * @param className : canonical name of filter class to be loaded
     * @throws OperationException
     */
    public void loadClass(final String datastore, final String className) throws OperationException{
        RestrictedClassLoader blobCityClassLoader = loaderStore.getLoaderWithCreate(datastore);

        if (blobCityClassLoader.isReloadRequired(className)) {
            logger.debug("Reload required for {}", className);
            classMap.remove(datastore);
            blobCityClassLoader = loaderStore.getNewLoader(datastore);
        }

        try {
            final Class loadedClass = blobCityClassLoader.loadClass(className, true);
            processClass(datastore, loadedClass);
        } catch (ClassNotFoundException ex) {
            logger.error("Error while loading class " + className + "\" for dsSet \"" + datastore + "\"");
            logger.error(null, ex);
            throw new OperationException(ErrorCode.CLASS_LOAD_ERROR, "Could not load filter class. " + ex.getMessage());
        }
    }

    /**
     * Process a given filter class to check for all criteria required to be a filter
     *
     * @param datastore : dsSet name
     * @param filterClass : Class for the filter
     * @throws OperationException:
     *      1. if Interface is not implemented
     *      2. if the class is not present in Manifest file
     *      3. if annotation Filter is not present
     *      4. if more than one Filter annotation is found
     */
    public void processClass(final String datastore, final Class filterClass) throws OperationException{
        // checking if interface is implemented or not.
        if( !Filterable.class.isAssignableFrom(filterClass)) {
            throw new OperationException(ErrorCode.FILTER_LOAD_ERROR, "Specified filter " + filterClass.getCanonicalName() + " inside dsSet " + datastore + " doesn't implement Filterable interface" );
        }
        
        logger.trace("processClass({}, {}) as filter", new Object[]{datastore, filterClass.getCanonicalName()});
        // checking for filters in Manifest file
        List<String> filters = manifestParser.getFilters(datastore);
        if ( !filters.contains(filterClass.getSimpleName()) ){
            throw new OperationException(ErrorCode.FILTER_LOAD_ERROR, "No such filter " +filterClass.getSimpleName() + " is defined in Manifest file");
        }
        
        String filterName = null;
        // checking for 'filter' annotation
        final Annotation[] annotations = filterClass.getAnnotations();
        for(Annotation annotation: annotations){
            if( "com.blobcity.db.annotations.Filter".equals(annotation.annotationType().getCanonicalName()) ){
                // name is supplied in annotation or not
                if(filterName == null){
                    if(annotation.toString().contains("name")){
                        String filterAnnotation = annotation.toString();
                        filterName = filterAnnotation.substring(filterAnnotation.indexOf("name=") + 5, filterAnnotation.length()-1);
                    }
                    else{
                        filterName = filterClass.getSimpleName();
                        logger.debug("{} found annotation FilterStoreBean with filter name {}", new Object[]{filterClass.getCanonicalName(), filterName});
                    }
                }
                else{
                    logger.debug("{} found duplicate annotation FilterName", new Object[]{filterClass.getCanonicalName()});
                    throw new OperationException(ErrorCode.FILTER_LOAD_ERROR, "Duplicate @Filter "
                            + "annotation found on class " + filterClass.getCanonicalName() + ". A single filter class "
                            + "may have only one @Filter annotation associated with it.");
                }
            }
        }
        if(!classMap.containsKey(datastore)){
            classMap.put(datastore, new HashMap<>());
        }
        
        classMap.get(datastore).put(filterName, filterClass.getCanonicalName() );
    }

    /**
     * Unload all filters from the database for a given dsSet
     *
     * @param datastore : dsSet name
     */
    public void removeAll(String datastore){
        logger.debug("Removing all filters for dsSet \"{}\" ...", datastore);
        classMap.remove(datastore);
        logger.debug("All filters were removed for dsSet \"{}\"", datastore);
    }

    /**
     * Remove a single filter from given dsSet
     *
     * @param datastore : dsSet name
     * @param filterName : filter name as defined in annotation
     */
    public void removeClass(String datastore, String filterName){
        classMap.get(datastore).remove(filterName);
    }

}
