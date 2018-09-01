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
import com.blobcity.db.code.RestrictedClassLoader;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class FilterParallelExecutor {
    private final Logger logger = LoggerFactory.getLogger(FilterParallelExecutor.class);
    
    @Autowired
    private FilterStoreBean filterStore;
    @Autowired
    private LoaderStore loaderStore;
    private Map<String, Object[]> classMap = new HashMap<>();
    private Map<String, Object[]> instanceMap = new HashMap<>();
    
    
    /**
     * this will create a new instance of a filter class and call method loadCriteria on the instance with given parameters
     * 
     * @param appId : database id
     * @param filterName : filter name as defined in annotation
     * @param number : no of instances to create for parallel processing
     * @param params : arguments of function loadCriteria provided by user (type: varargs) 
     * @throws OperationException : when there is some issue with either loading the class or running the methods inside it
     */
    public void createNewInstance(String appId, String filterName, int number, Object[] params) throws OperationException{
        if( !filterStore.isPresent(appId, filterName) )
            throw new OperationException(ErrorCode.FILTER_NOT_LOADED, "No such filter found for given database");
        RestrictedClassLoader blobCityLoader =  loaderStore.getNewLoader(appId);
        try {
            Object[] classes = new Object[number];
            Object[] instances = new Object[number];
            // this is a work around when no parameters are passed by user for loadCriteria argument 
            // TODO: improve in future to accept null arguments
            if(params == null ){
                params = new Object[1];
                params[0] = 1;    
            }
            for(int i=0; i<number; i++){
                Class filterClass = blobCityLoader.loadClass(filterStore.getClass(appId, filterName));
                Object instance = filterClass.newInstance();
                Method method = filterClass.getDeclaredMethod( FilterFunction.LOAD.getName(), Object[].class);
                method.invoke(instance, new Object[]{params});
                classes[i] = filterClass;
                instances[i] = instance;
            }
            classMap.put(appId, classes);
            instanceMap.put(appId, instances);
        } catch (NoSuchMethodException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "No such method is defined for given filter");
        } catch  (SecurityException ex){
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "Internal security error occured while executing filter");
        } catch (IllegalAccessException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "Specified method does not have public access");
        } catch (IllegalArgumentException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_INCORRECT_PARAMS, "Illegal arguments passed to method " + FilterFunction.LOAD.getName() );
        } catch (InvocationTargetException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "Execution of filter method "+ FilterFunction.LOAD.getName() +" failed");
        } catch (ClassNotFoundException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "No such filter class is found");
        } catch (InstantiationException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "Error while creating instance of class");
        }
    }
    
    /**
     * this function will call the match function of given filter for given record.
     * 
     * @param appId : database id
     * @param filterName: filter name as defined in annotation
     * @param rowObject : JSONObject of row which to be tested against the filter
     * @param seq
     * @return 
     * @throws OperationException 
     */
    public boolean executeCheckMethod(final String appId, final String filterName, final JSONObject rowObject, final int seq) throws OperationException{
        try {
            Class filterClass = (Class) classMap.get(appId)[seq];
            Object instance  = instanceMap.get(appId)[seq];
            Method method = filterClass.getDeclaredMethod(FilterFunction.MATCH.getName(), JSONObject.class);
            return (Boolean) method.invoke(instance, rowObject);
        } 
        catch (NoSuchMethodException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "No such method is defined for given filter");
        }
        catch(SecurityException ex){
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "Internal security error occured while executing filter");
        }
        catch (IllegalAccessException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "Specified method does not have public access");
        } catch (IllegalArgumentException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_INCORRECT_PARAMS, "Illegal arguments passed to method " + FilterFunction.MATCH.getName() );
        } catch (InvocationTargetException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.FILTER_EXECUTION_ERROR, "Execution of filter method "+ FilterFunction.MATCH.getName() +" failed");
        }
    }
    
    
}
