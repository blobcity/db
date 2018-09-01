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

import com.blobcity.db.code.filters.ThreadRun;
import com.blobcity.db.code.filters.FilterParallelExecutor;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.code.datainterpreter.InterpreterExecutorBean;
import com.blobcity.db.code.filters.FilterExecutorBean;
import com.blobcity.db.code.procedures.ProcedureExecutorBean;
import com.blobcity.db.code.triggers.TriggerExecutorBean;
import com.blobcity.db.code.triggers.TriggerFunction;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.blobcity.db.code.webservices.WebServiceStore;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

/**
 * This bean is responsible to execute any piece of User Defined Code.
 * This handles all the execution for triggers, filters, procedures and data interpreters
 * 
 * @author sanketsarang
 */
@Component
public class CodeExecutor {
    
    private static final Logger logger = LoggerFactory.getLogger(CodeExecutor.class);

    @Autowired(required = false) @Lazy
    private BSqlDataManager dataManager;
    @Autowired
    private FilterExecutorBean filterExecutor;
    @Autowired
    private InterpreterExecutorBean interpreterExecutor;
    @Autowired
    private FilterParallelExecutor parallelFilterExecutor;
    @Autowired
    private ProcedureExecutorBean procedureExecutor;
    @Autowired
    private TriggerExecutorBean triggerExecutor;
    @Autowired
    private WebServiceStore webServiceStore;
    
    /**
     * Execute the specified filter on given collection.
     * This will execute the filter in a sequential manner
     * 
     * @param datastore : dsSet name
     * @param collection : collection name
     * @param filterName : name of filter as defined in annotation
     * @param params : parameters for loadCriteria function
     * @return : list of primary keys which passed the filter criteria
     * @throws OperationException 
     */
    public List<Object> executeFilter(final String datastore, final String collection,
            final String filterName, Object[] params) throws OperationException{
        List<Object> data = new ArrayList<>();
        filterExecutor.createNewInstance(datastore, filterName, params);
        Iterator<String> keys = dataManager.selectAllKeysAsStream(datastore, collection);
        while(keys.hasNext()){
            String key = keys.next();
            if(filterExecutor.executeCheckMethod(datastore, filterName, dataManager.select(datastore, collection, key))){
                data.add(key);
            }
        }
        return data;
    }

    /**
     * Execute the filter on given collection in a parallel manner
     *
     * @param datastore : dsSet name
     * @param collection : collection name
     * @param filterName : name of filter as defined in annotation
     * @param params : parameters for loadCriteria function
     * @return : list of primary keys which passed the filter criteria
     * @throws OperationException
     */
    public List<String> executeParallelFilter(final String datastore, final String collection,
            final String filterName, Object[] params) throws OperationException{

        /** Initialize the thread pool and stuff **/
        int nProcessors = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(nProcessors);
        parallelFilterExecutor.createNewInstance(datastore, filterName, nProcessors, params);

        List<Future> futures = new ArrayList<>();

        /** Select all keys from the table **/
        // TODO: this method needs to store all keys. In future, let's try to remove this.
        List<String> keys = dataManager.selectAllKeys(datastore, collection);
        int ptr = 0; int cnt =0;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Iterator<String> keyItr = keys.iterator();
        while(keyItr.hasNext()){
            JSONObject rowJson =  dataManager.select(datastore, collection, keyItr.next());
            ThreadRun tmp1 = new ThreadRun(parallelFilterExecutor, datastore, filterName, rowJson, ptr);
            futures.add(executorService.submit(tmp1));
            ptr++; ptr %= nProcessors; cnt++;
        }
        /** Waiting for executor service to finish **/
        executorService.shutdown();
        while(!executorService.isShutdown()) {
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                logger.error("Timeout happened during execution of filter {}.{}, {}", new Object[]{datastore, collection, filterName});
                logger.error(ex.getMessage(), ex);
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
            }
        }
        /** assemble the output **/
        List<String> filteredKeys = new ArrayList<>();
        try {
            for(int i=0;i<futures.size();i++){
                if((Boolean)futures.get(i).get()){
                    filteredKeys.add(keys.get(i));
                }
            }
        } catch (InterruptedException | ExecutionException ex) {
            logger.error("Error during execution of filter {}.{} , {}", new Object[]{datastore, collection, filterName});
            logger.error(ex.getMessage(), ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        stopWatch.stop();
        logger.debug("Filtering data for collection {}.{}, {}, total count: {}", new Object[]{datastore, collection, filterName, cnt});
        logger.debug("time taken {} : ", new Object[]{stopWatch.getTotalTimeSeconds()} );
        return filteredKeys;
    }

    /**
     * Execute a stored procedure
     *
     * @param requestId : request Id as obtained from request store when request is made to execute it
     * @param datastore: dsSet name
     * @param procedureName: procedure name as defined in the annotation
     * @param parameters: parameters to the procedure
     * @return: object as returned by the procedure
     * @throws OperationException
     */
    public Object executeProcedure(final String requestId, final String datastore,
            final String procedureName, Object[] parameters) throws OperationException {
        return procedureExecutor.executeProcedure(requestId, datastore, procedureName, parameters);
    }

    /**
     * Execute a given trigger with operations which require only one version of row, (INSERT, DELETE)
     *
     * @param datastore: dsSet name
     * @param collection: collection name
     * @param function: which trigger function to execute (BEFORE or AFTER)
     * @param rowJSON: jsonObject of the row
     * @throws OperationException
     */
    public void executeTrigger( final String datastore, final String collection,
            final TriggerFunction function, JSONObject rowJSON) throws OperationException{
        triggerExecutor.executeTrigger(datastore, collection, function, rowJSON);
    }

    /**
     * Execute a given trigger with functions which require two versions of a row (UPDATE)
     *
     * @param datastore: dsSet name
     * @param collection: collection name
     * @param function: which trigger function to execute (BEFORE or AFTER)
     * @param oldObj: old jsonObject of the row
     * @param newObj: new jsonObject of the row
     * @throws OperationException
     */
    public void executeTrigger( final String datastore, final String collection,
            final TriggerFunction function, JSONObject oldObj, JSONObject newObj) throws OperationException{
        triggerExecutor.executeTrigger(datastore, collection, function, oldObj, newObj);
    }

    /**
     * Execute a data interpreter on a single row of unstructured data
     *
     * @param datastore: name of dsSet
     * @param interpreterName: name of interpreter to be executed
     * @param row: one row to be converted into JSONObject
     * @return: JSONObject as returned by the interpreter
     * @throws OperationException
     */
    public JSONObject executeDataInterpreter(final String datastore,
            final String interpreterName, final String row) throws OperationException{
        return interpreterExecutor.convert(datastore, interpreterName, row);
    }

    /**
     * Execute a given data interpreter on the array of rows stored as JSONAray.
     * Note: this will use only one object to convert all rows
     *
     * @param datastore: name of dsSet
     * @param interpreterName: name of interpreter to be executed
     * @param inputRows: JSONArray containing all rows to be converted
     * @return: List of objects as returned by the interpreter, one for each input row
     * @throws OperationException
     */
    public List<JSONObject> executeDataInterpreter(final String datastore,
            final String interpreterName, final JSONArray inputRows) throws OperationException{
        return interpreterExecutor.convert(datastore, interpreterName, inputRows);
    }

    /**
     * This will execute a data interpreter on given rows and will insert them in the given collection
     * data Interpreter and collection should be in same dsSet)
     * NOTE: This uses clustering layer to insert the converted row
     *
     * @param datastore: dsSet name
     * @param collection: collection name
     * @param interpreterName: name of interpreter to be executed
     * @param inputRows: JSONArray containing the input rows
     * @return: List of JSONObject as inserted into the database
     * @throws OperationException
     */
    public List<JSONObject> executeDataInterpreter(final String datastore, final String collection,
            final String interpreterName, final JSONArray inputRows) throws OperationException{
        return interpreterExecutor.insert(datastore, collection, interpreterName, inputRows);
    }

}
