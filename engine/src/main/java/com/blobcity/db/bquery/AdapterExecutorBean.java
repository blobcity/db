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

package com.blobcity.db.bquery;

import com.blobcity.db.api.Db;
import com.blobcity.db.api.InternalAdapterException;
import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.bsql.BSqlDatastoreManager;
import com.blobcity.db.code.CodeExecutor;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;
import com.blobcity.lib.requests.RequestData;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;

/**
 * This bean handles all the data operations on server-side adapter.
 * 
 * @author sanketsarang
 * @author Prikshit Kumar
 */
@Component
public class AdapterExecutorBean implements Db {

    private static final Logger logger = LoggerFactory.getLogger(AdapterExecutorBean.class);
    
    private String datastore;
    @Autowired
    private CodeExecutor codeExecutor;
    @Autowired
    private BSqlCollectionManager collectionManager;
    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private BSqlDatastoreManager datastoreManager;
    @Autowired
    private RequestStore requestStore;
    @Autowired
    private SQLExecutorBean sqlExecutorBean;
    
    /**
     * Contains the connection credentials on which the operations of all functions within this class have to operate. 
     * Data also contains the requestId for passing to other functions.
     */
    private RequestData requestData; //architecture needs to be reconsidered with introduction of Query class

    /**
     * set the request parameters as obtained from the client-side invocation of SPs or filters
     *
     * @param requestId 
     */
    public void setRequest(final String requestId) {

        //TODO: Reconsider architecture with the introduction of Query class

//        requestData = requestStore.getRequest(requestId);
//        dsSet = requestData.getDs();

        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * Whether a given row with given id isPresent or not inside given collection
     * 
     * @param collection: name of collection
     * @param _id: _id of the row
     * @return: boolean, true if isPresent, no otherwise
     * @throws InternalAdapterException 
     */
    @Override
    public boolean contains(final String collection, final String _id) throws InternalAdapterException {
        verifyDCInfo(collection);
        try {
            return dataManager.exists(datastore, collection, _id);
        } catch (OperationException ex) {
            throw new InternalAdapterException(ex.getMessage());
        }
    }

    /**
     * run a loaded filter on the given collection with given arguments to the filter
     *
     * @param collection: name of collection
     * @param filterName: name of filter
     * @param params: arguments to the filter
     * @return: Iterator of keys to the rows which passed the filter criteria
     * @throws InternalAdapterException
     */
    @Override
    public Iterator<String> runFilter(final String collection, final String filterName, Object... params) throws InternalAdapterException{
        verifyDCInfo(collection);
        try {
            return codeExecutor.executeParallelFilter(datastore, collection, filterName, params).iterator();
        } catch (OperationException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }

    /**
     * execute a SQL query on given collection
     * 
     * @param collection: name of collection
     * @param sql
     * @return
     * @throws InternalAdapterException 
     */
    @Override
    public String executeSqlQuery(final String collection, final String sql) throws InternalAdapterException {
        try {
            verifyDCInfo(collection);
//            return  sqlExecutorBean.runQuery(requestData.getRequestId(), requestData.getDs(), sql);
            return sqlExecutorBean.executePrivileged(requestData.getDs(), sql);
        } catch (InternalAdapterException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }
    
    /**
     * insert a JSON Object in the given collection 
     * 
     * @param collection: name of collection
     * @param insertRow: jsonObject to be inserted
     * @throws InternalAdapterException 
     */
    @Override
    public void insert(final String collection, final JSONObject insertRow) throws InternalAdapterException {
        verifyDCInfo(collection);
        try {
            dataManager.insert(datastore, collection, insertRow);
        } catch (OperationException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }
    
    /**
     * Invoke a loaded stored procedure 
     * 
     * @param <U>: return class of stored procedure
     * @param procedureName: name of procedure to be executed
     * @param returnClass: return class 
     * @param params: arguments to the stored procedure
     * @return
     * @throws InternalAdapterException 
     */
    @Override
    public <U> U invokeProcedure(final String procedureName, final Class<U> returnClass, final Object... params) throws InternalAdapterException {
        try {   
            Object returnObject = codeExecutor.executeProcedure(requestData.getRequestId(), datastore, procedureName, params);
            try{
                return (returnObject == null) ? null : returnClass.cast(returnObject);
            }
            catch(ClassCastException ex){
                logger.debug(null, ex);
                throw new InternalAdapterException("Error in casting the output of stored procedure "
                        + procedureName+" to " + returnClass.getName() + " class");
            }
        } catch (OperationException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }
    
    /**
     * select a row with given id
     * 
     * @param collection: name of collection
     * @param _id: id of the row
     * @return
     * @throws InternalAdapterException 
     */
    @Override
    public JSONObject load(final String collection, final String _id) throws InternalAdapterException {
        verifyDCInfo(collection);
        try {
            return dataManager.select(datastore, collection, _id);
        } catch (OperationException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }
    
    /**
     * Removes data corresponding to the specified primary key from the collection if corresponding record is found, else is
     * a no-op
     * 
     * @param collection: name of collection
     * @param _id: id of the row
     * @throws InternalAdapterException 
     */
    @Override
    public void remove(final String collection ,final String _id) throws InternalAdapterException {
        verifyDCInfo(collection);
        try {
            dataManager.remove(datastore, collection, _id);
        } catch (OperationException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }

    /**
     * 
     * @param collection: name of collection
     * @param colsToSelect
     * @param params
     * @return
     * @throws InternalAdapterException 
     */
    @Override
    public Iterator<Object> search(final String collection, final List<String> colsToSelect, final List<JSONObject> params) throws InternalAdapterException{
        verifyDCInfo(collection);
        try {
            JSONObject[] whereParams = new JSONObject[params.size()];
            params.toArray(whereParams); // fill the array
            return dataManager.searchCols(datastore, collection, colsToSelect, whereParams);
        } catch (OperationException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }
    
    /**
     * select all rows in a collection
     * 
     * @param collection: name of collection
     * @return iterator to the list of all rows in given collection
     * @throws InternalAdapterException 
     */
    @Override
    public Iterator<Object> selectAll(final String collection )throws InternalAdapterException {
        verifyDCInfo(collection);
        throw new InternalAdapterException("Operation no longer supported. Will be reimplemented in future release");
//        try {
//            return dataManager.selectAll(datastore, collection);
//        } catch (OperationException ex) {
//            logger.debug(null, ex);
//            throw new InternalAdapterException(ex.getMessage());
//        }
    }
    
    /**
     * select all rows with complete data from given collection as stream
     * 
     * @param collection: name of collection
     * @return iterator to the list of rows of a given collection
     * @throws InternalAdapterException 
     */
    @Override
    public Iterator<String> selectAllAsStream(final String collection) throws InternalAdapterException {
        verifyDCInfo(collection);
        try {
            return dataManager.selectAllKeysAsStream(datastore, collection);
        } catch (OperationException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }
    
    @Override
    public Iterator<Object> selectAllFromCols(final String collection, final List<String> colsToSelect) throws InternalAdapterException {
        verifyDCInfo(collection);
        try {
            return dataManager.selectAllFromCols(datastore, collection, colsToSelect);
        } catch (OperationException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }
    
    /**
     * update a the row with given id in given collection
     * 
     * @param collection: name of collection
     * @param _id: id of the row
     * @param updateJSON: new JSONObject for the row 
     * @throws InternalAdapterException 
     */
    @Override
    public void update(final String collection, final String _id, JSONObject updateJSON) throws InternalAdapterException {
        verifyDCInfo(collection);
        try {
            dataManager.save(datastore, collection, updateJSON);
        } catch (OperationException ex) {
            logger.debug(null, ex);
            throw new InternalAdapterException(ex.getMessage());
        }
    }
    
    /**
     * it verifies whether the database and table provided isPresent or not
     * database is automatically fetched from requestData
     * 
     * @param collection: name of collection
     * @throws InternalAdapterException if incorrect database or collection provided
     */
    public void verifyDCInfo(final String collection) throws InternalAdapterException{
        if(datastore == null  || datastore.isEmpty() ){
            throw new InternalAdapterException("Datastore was not set before stored procedure was invoked");
        }
        if(!datastoreManager.exists(datastore))
            throw new InternalAdapterException("Specified dsSet " + datastore + "doesn't exist");

        if(!collectionManager.exists(datastore, collection)) {
            throw new InternalAdapterException("Specified collection " + collection + "doesn't exist inside dsSet " + datastore);
        }
    }

}
