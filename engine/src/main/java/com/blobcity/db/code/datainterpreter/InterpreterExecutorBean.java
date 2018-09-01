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

import com.blobcity.db.bsql.ClusterDataManager;
import com.blobcity.db.code.LoaderStore;
import com.blobcity.db.code.RestrictedClassLoader;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.blobcity.db.util.Performance;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * this bean executes the given data interpreter
 *
 * @author sanketsarang
 */
@Component
public class InterpreterExecutorBean {
    private static final Logger logger = LoggerFactory.getLogger(InterpreterExecutorBean.class);
    
    @Autowired
    private InterpreterStoreBean interpreterStore;
    @Autowired
    private LoaderStore loaderStore;
    @Autowired(required = false)
    @Lazy
    private ClusterDataManager clusterDataManager;
    
    
    /**
     * insert a given JSONObject into database through clustering layer
     * 
     * @param datastore: dsSet name
     * @param collection: collection name
     * @param insertJSON: JSONObject to be inserted
     * @return: JSONObject as inserted into database.
     * @throws OperationException: if there is an error in insertion
     */
    public JSONObject insertIntoDatabase(final String datastore, final String collection, 
            final JSONObject insertJSON) throws OperationException{
        return clusterDataManager.insert(datastore, collection, insertJSON);
    }
    
    
    /**
     * insert the Interpreter and return the JSONObject returned by the method. 
     * This will not insert row in the database
     * 
     * @param datastore: dataStore name
     * @param interpreter: name of the interpreter
     * @param row: row to be converted
     * @return: JSONObject as converted
     * @throws OperationException  
     */
    public JSONObject convert(final String datastore, final String interpreter, final String row) throws OperationException{
        if( !interpreterStore.isPresent(datastore, interpreter) ) {
            throw new OperationException(ErrorCode.DATAINTERPRETER_NOT_LOADED);
        }

        try{
            Class interpreterClass = interpreterStore.getClass(datastore, interpreter);
            Object instance = interpreterClass.newInstance();
            Method method = interpreterClass.getDeclaredMethod("interpret", String.class);
            JSONObject interpretedJson = (JSONObject) method.invoke(instance, row);
            return interpretedJson;
        } catch (InstantiationException | IllegalAccessException |
                NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.DATAINTERPRETER_EXECUTION_ERROR, "Error in executing data interpreter. Check logs for more info");
        }
    }
    

    /**
     * Execute the interpreter on given rows. 
     * Conversion is sequential and only one object of given interpreter class is used to convert all rows
     * 
     * @param datastore: dsSet name
     * @param interpreter: name of the interpreter
     * @param inputRows: Array of rows to be converted
     * @return: list of JSONObject after conversion
     * @throws OperationException 
     */
    public List<JSONObject> convert(final String datastore, final String interpreter, final JSONArray inputRows) throws OperationException{
        if( !interpreterStore.isPresent(datastore, interpreter) )
            throw new OperationException(ErrorCode.DATAINTERPRETER_NOT_LOADED);

        List<JSONObject> responseList = new ArrayList<>();
        for(int i = 0; i < inputRows.length(); i ++) {
            responseList.add(convert(datastore, interpreter, inputRows.get(i).toString()));
        }

        return responseList;
    }
    
    /**
     * Execute the interpreter on given rows in JSONArray and insert them in database through clustering layer.
     * Only one object of interpreter class is used to convert all rows.
     * Insert and conversion happen in one go and are sequential.
     * 
     * @param datastore: dsSet name
     * @param collection: collection name
     * @param interpreter: name of interpreter to be executed
     * @param inputRows: rows to be converted 
     * @return: empty list now, (in future, list of JSONObject as inserted into database)
     * @throws OperationException 
     */
    public List<JSONObject> insert(final String datastore, final String collection, final String interpreter, final JSONArray inputRows) throws OperationException{
        if( !interpreterStore.isPresent(datastore, interpreter) )
            throw new OperationException(ErrorCode.DATAINTERPRETER_NOT_LOADED);
        
        try{
            Class interpreterClass = interpreterStore.getClass(datastore, interpreter);
            Object instance = interpreterClass.newInstance();
            Method method = interpreterClass.getDeclaredMethod("interpret", String.class);
            
            List<JSONObject> tmp = new ArrayList<>();
            for(int i=0; i<inputRows.length();i++){
                JSONObject converted = (JSONObject) method.invoke(instance, inputRows.get(i).toString());
                insertIntoDatabase(datastore, collection, converted);
            }
            return tmp;
        } catch (InstantiationException | IllegalAccessException |
                NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.DATAINTERPRETER_EXECUTION_ERROR, "Error in executing data interpreter. Check logs for more info");
        }
    }
    
    /**
     * Executes the interpreter on given rows in inputRows and inserts them in database through clustering layer.
     * Rows are converted,inserted in a parallel manner. 
     * (conversion and insertion for a single row is sequential)
     * 
     * @param datastore: dsSet name
     * @param interpreter: name of the interpreter
     * @param inputRows: Array of rows to be converted
     * @return: list of JSONObject after conversion
     * @throws OperationException 
     */
    public JSONArray insertParallel(final String datastore, final String collection, final String interpreter, final JSONArray inputRows) throws OperationException{
        if( !interpreterStore.isPresent(datastore, interpreter) )
            throw new OperationException(ErrorCode.DATAINTERPRETER_NOT_LOADED);
        RestrictedClassLoader blobCityLoader =  loaderStore.getNewLoader(datastore);
        ExecutorService  executorService = Executors.newFixedThreadPool(Performance.THREAD_POOL_SIZE);
        
        List<Callable<Object>> callableList  = new ArrayList<>();
        List<Future> futures = new ArrayList<>();
        
        
//        executorService.submit(new InterpreterThread());
        
        
        
        return null;
    }

}
