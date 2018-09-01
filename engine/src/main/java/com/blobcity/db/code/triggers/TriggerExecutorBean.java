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

package com.blobcity.db.code.triggers;

import com.blobcity.db.code.LoaderStore;
import com.blobcity.db.code.RestrictedClassLoader;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * this class is used to execute various trigger specified by appId && triggerName
 * 
 * @author sanketsarang
 */
@Component
public class TriggerExecutorBean {
    @Autowired
    private TriggerStoreBean triggerStore;
    @Autowired
    private LoaderStore loaderStore;
    
    /**
     * execute the specified function of all triggers related to given app and table.
     * This function should be called when there is only one json string of record like in INSERT or DELETE.
     *
     * @param appId : id of database, trigger is related to.
     * @param table: table to which trigger is related.
     * @param function: which trigger function to execute.
     * @param jsonObj: jsonObject of the recored which is inserted or deleted.
     * @throws OperationException 
     */
    public void executeTrigger( final String appId, final String table, final TriggerFunction function, JSONObject jsonObj) throws OperationException{
        List<String> activatedTriggers = triggerStore.getTriggers(appId, table, Boolean.TRUE);
        for(String trigger: activatedTriggers) executeTrigger(appId, table, trigger, function, jsonObj);
    }
    
    /**
     * execute the specified function of all triggers related to given app and table. 
     * This is to be called when there is an UPDATE operation to an existing record.
     * 
     * @param appId
     * @param table
     * @param function : name of trigger function to execute 
     * @param oldObj : original json of record to be updated
     * @param newObj : updated json of record which was just updated
     * @throws OperationException
     */
    public void executeTrigger( final String appId, final String table, final TriggerFunction function, JSONObject oldObj, JSONObject newObj) throws OperationException{
        List<String> activatedTriggers = triggerStore.getTriggers(appId, table, Boolean.TRUE);
        for(String trigger: activatedTriggers) executeTrigger(appId, table, trigger, function, oldObj, newObj);
    }
    
    /**
     * 
     * @param appId
     * @param triggerName: name of trigger to be executed
     * @param function: which trigger function to be executed (AFTER/BEFORE for INSERT/DELETE only)
     * @param jsonObj: json string of the record being inserted or deleted
     * @throws OperationException 
     */
    private void executeTrigger( final String appId, final String table, final String triggerName, final TriggerFunction function, JSONObject jsonObj) throws OperationException{
        if( !triggerStore.isActivated(appId, triggerName)) return ;
        RestrictedClassLoader blobCityClassLoader = loaderStore.getLoaderWithCreate(appId);
        try {
            Class triggerClass = blobCityClassLoader.loadClass(triggerName);
            // two ways to do this
            // 1. loading directly from className
            Object object = triggerClass.newInstance();
            // loading from triggerStroreBean (basically doing the same thing)
            //Object object = triggerStoreBean.getNewInstance(app, trigger);
            Method method = triggerClass.getDeclaredMethod(function.getFunctionName(), JSONObject.class);
            method.invoke(object, jsonObj);
        } catch (ClassNotFoundException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not load class for trigger: " + triggerName);
        } catch (InstantiationException |  NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Internal error occured while loading triger: " + triggerName);
        }
    }
    
    /**
     * 
     * @param appId
     * @param triggerName
     * @param function: trigger function type (after or before)
     * @param oldObj: json string of record to be updated
     * @param newObj: updated json string of record
     * @throws OperationException 
     */
    private void executeTrigger( final String appId, final String table, final String triggerName, final TriggerFunction function, JSONObject oldObj, JSONObject newObj) throws OperationException{
        if( !triggerStore.isActivated(appId, triggerName)) return ;
        RestrictedClassLoader blobCityClassLoader = loaderStore.getLoaderWithCreate(appId);
        // get all activated triggers
        try {
            Class triggerClass = blobCityClassLoader.loadClass(triggerName);
            // two ways to do this
            // 1. loading directly from className
            Object object = triggerClass.newInstance();
            // loading from triggerStroreBean (basically doing the same thing)
            //Object object = triggerStoreBean.getNewInstance(app, trigger);
            Method method = triggerClass.getDeclaredMethod(function.getFunctionName(), JSONObject.class, JSONObject.class );
            method.invoke(object, oldObj, newObj);
        } catch (ClassNotFoundException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not load class for trigger: "+ triggerName);
        } catch (InstantiationException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Internal error occured while loading triger: "+ triggerName);
        } 
    }
    
}
