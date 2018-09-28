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
import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.code.LoaderStore;
import com.blobcity.db.code.ManifestParserBean;
import com.blobcity.db.code.RestrictedClassLoader;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.functions.Triggerable;
import com.blobcity.db.sp.annotations.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.annotation.Annotation;
import java.util.*;

/**
 * this bean stores information about triggers and processes them when they are about to be loaded into database
 * 
 * @author sanketsarang
 */
@Component
public class TriggerStoreBean {
    
    private static final Logger logger = LoggerFactory.getLogger(TriggerStoreBean.class.getName());
    
    @Autowired
    private LoaderStore loaderStore;
    @Autowired
    private ManifestParserBean manifestParser;
    @Autowired
    private BSqlCollectionManager collectionManager;

    /**
     * stores mapping of triggers with collection and dsSet
     * (dsSet -> triggerClassName -> collection)
     */
    private Map<String, Map<String, String>> collectionMap;
    /**
     * stores mapping of triggers with their sequence and corresponding dsSet
     * (dsSet -> triggerName -> sequence of trigger)
     */
    private Map<String, Map<String, Integer>> sequenceMap;
    /**
     * stores activation status of triggers with app
     * (dsSet -> triggerName -> activation-status)
     */
    private Map<String, Map<String, Boolean>> activationMap;
    
    @PostConstruct
    public void init(){
        collectionMap = new HashMap<>();
        sequenceMap = new HashMap<>();
        activationMap = new HashMap<>();
    }

    /**
     * activate all triggers specific to given dsSet
     * 
     * @param datastore: name of dsSet
     * @throws OperationException: if no such dsSet isPresent in the database
     */
    public void activateTrigger(final String datastore) throws OperationException{
        if( !activationMap.containsKey(datastore) ){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given dsSet " + datastore + " doesn't isPresent");
        }
        Map<String, Boolean> activationStatus = activationMap.get(datastore);
        for(String trigger: activationStatus.keySet())
            activationStatus.put(trigger, Boolean.TRUE);
    }
    
    /**
     * activate all triggers associated with specified dsSet and collection
     *
     * @param datastore: dsSet name
     * @param collection: collection name
     * @throws OperationException : if given dsSet and collection doesn't exist
     */
    public void activateTrigger(final String datastore, final String collection) throws OperationException{
        if( !activationMap.containsKey(datastore) ){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given dsSet " + datastore + " doesn't exist");
        }
        if( !collectionMap.get(datastore).containsValue(collection)){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No collection named " + collection + " found in dsSet " + datastore);
        }
        Map<String, String> tableTriggers = collectionMap.get(datastore);
        for( String triggerName : tableTriggers.keySet() ){
            if(collection.equals(tableTriggers.get(triggerName))) activationMap.get(datastore).put(triggerName, Boolean.TRUE);
        }
        
    }
    
    /**
     * activate the specified trigger
     * 
     * @param datastore: dsSet name
     * @param collection: collection name
     * @param triggerName: name of trigger to be activated
     * @throws OperationException : if either dsSet or collection or trigger doesn't exist
     */
    public void activateTrigger(final String datastore, final String collection, final String triggerName) throws OperationException{
        if( !isPresent(datastore, collection, triggerName))
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such trigger " + triggerName +
                    " found with collection: " + collection + " and dsSet: "+ datastore );
        activationMap.get(datastore).put(triggerName, Boolean.TRUE);
    }
    
    /**
     * deactivate all triggers specific to given dsSet
     * 
     * @param datastore: dsSet name
     * @throws OperationException: if no such dsSet isPresent in the database
     */
    public void deActivateTrigger(final String datastore) throws OperationException{
        if( !activationMap.containsKey(datastore) ){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given dsSet " + datastore + " doesn't exist");
        }
        Map<String, Boolean> activationStatus = activationMap.get(datastore);
        for(String trigger: activationStatus.keySet())
            activationStatus.put(trigger, Boolean.FALSE);
    }
    
    /**
     * deactivate all triggers related to specified dsSet and collection
     *
     * @param datastore: dsSet name
     * @param collection: collection name
     * @throws OperationException : if given dsSet or collection doesn't exist
     */
    public void deActivateTrigger(final String datastore, final String collection) throws OperationException{
        if( !activationMap.containsKey(datastore) ){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given dsSet " + datastore + " doesn't exist");
        }
        if( !collectionMap.get(datastore).containsValue(collection)){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No collection named " + collection + " found in dsSet " + datastore);
        }
        Map<String, String> collectionTriggers = collectionMap.get(datastore);
        for( String triggerName : collectionTriggers.keySet() ){
            if(collection.equals(collectionTriggers.get(triggerName))) activationMap.get(datastore).put(triggerName, Boolean.FALSE);
        }
        
    }
    
    /**
     * deactivate a given trigger related to specific dsSet and collection
     * 
     * @param datastore: dsSet name
     * @param collection: collection name
     * @param triggerName: name of trigger
     * @throws OperationException : if either dsSet or collection or trigger doesn't exist.
     */
    public void deActivateTrigger(final String datastore, final String collection, final String triggerName) throws OperationException{
        if( !isPresent(datastore, collection, triggerName))
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Trigger " + triggerName + " was not found with collection "
                    + collection + " and dsSet " + datastore);
        activationMap.get(datastore).put(triggerName, Boolean.FALSE);
    }
    
    /**
     * list all trigger for a given dsSet
     *
     * @param datastore: dsSet name
     * @return: list of trigger associated with given dsSet
     */
    public List<String> getTriggers(final String datastore){
        if ( !collectionMap.containsKey(datastore) ){
            return Collections.EMPTY_LIST;
        }
        return new ArrayList<>( collectionMap.get(datastore).keySet());
    }

    /**
     * List all triggers associated with specified dsSet and collection
     *
     * @param datastore: dsSet name
     * @param collection: collection name
     * @return: List of triggers associated with given dsSet and collection
     */
    public List<String> getTriggers(final String datastore, final String collection){
        if ( !collectionMap.containsKey(datastore) ){
            return Collections.EMPTY_LIST;
        }
        List<String> triggers = new ArrayList<>();
        Map<String, String> collectionTriggers = collectionMap.get(datastore);

        collectionTriggers.keySet().stream().filter((triggerTable) -> ( triggerTable.equals(collection))).forEach((triggerTable) -> {
            triggers.add( collectionTriggers.get(triggerTable) );
        });
        return triggers;
    }
    
    /**
     * returns list of trigger specified by activation status
     * 
     * @param datastore: dsSet name
     * @param activationStatus : activation status
     * @return 
     */
    public List<String> getTriggers(final String datastore, final boolean activationStatus){
        if ( !collectionMap.containsKey(datastore) ){
            return Collections.EMPTY_LIST;
        }
        List<String> triggerList = new ArrayList<>();
        Map<String, String> triggerTableMap  = collectionMap.get(datastore);
        Map<String, Boolean> triggerActivationMap = activationMap.get(datastore);
        for(String trigger: triggerTableMap.keySet()){
            if( triggerActivationMap.get(trigger) == activationStatus ) triggerList.add(trigger);
        }
        return triggerList;
    }
    
    /**
     * List all triggers for a given dsSet and collection with given activation status
     * 
     * @param datastore: dsSet name
     * @param collection: collection name
     * @param activationStatus: activation status of trigger
     * @return : list of triggers matching criteria
     */
    public List<String> getTriggers(final String datastore, final String collection, final boolean activationStatus){
        if ( !collectionMap.containsKey(datastore) ){
            return Collections.EMPTY_LIST;
        }
        List<String> triggerList = new ArrayList<>();
        Map<String, String> triggerTableMap = collectionMap.get(datastore);
        Map<String, Boolean> triggerActivationMap = activationMap.get(datastore);
        for(String trigger : triggerTableMap.keySet()){
            if( triggerActivationMap.get(trigger)==activationStatus && collection.equals(triggerTableMap.get(trigger)) )
                triggerList.add(trigger);
        }
        return triggerList;
    }

    /**
     * Invalidate all triggers.
     * This is equivalent to unloading all triggers from the database
     */
    public void invalidate(){
        collectionMap = new HashMap<>();
        sequenceMap = new HashMap<>();
        activationMap = new HashMap<>();
    }

    /**
     * Check whether a given trigger is activated or not
     *
     * @param datastore : dsSet name
     * @param triggerName : name of trigger
     * @return : whether the trigger is activated or not
     */
    public boolean isActivated(final String datastore, final String triggerName){
        if(!activationMap.containsKey(datastore)) return false;
        if(!activationMap.get(datastore).containsKey(triggerName)) return false;
        return activationMap.get(datastore).get(triggerName);
    }

    /**
     * checks whether is a specific trigger related to given dsSet and collection is present(loaded) in database
     *
     * @param datastore : dsSet name
     * @param collection : collection name
     * @param triggerName : trigger name
     * @return : true if present, false otherwise
     */
    public boolean isPresent(final String datastore, final String collection, final String triggerName){
        // checking whether app is present or not.
        if(collectionMap.containsKey(datastore)){
            // checking whether trigger is present or not
            if(collectionMap.get(datastore).containsKey(triggerName)){
                // checking if trigger is associated with specified collection or not
                return collectionMap.get(datastore).get(triggerName).equals(collection);
            }
            else return false;
        }
        return false;
    }

    /**
     * load the list of given triggers into the database
     *
     * @param datastore
     * @param classList
     * @throws OperationException
     */
    public void loadClasses(final String datastore, final List<String> classList) throws OperationException{
        logger.trace("Loading data for dsSet \"{}\"", datastore);

        if(collectionMap == null ){
            collectionMap = new HashMap<>();
            activationMap = new HashMap<>();
            sequenceMap = new HashMap<>();
        }

        if (collectionMap.containsKey(datastore)) {
            logger.debug("Clearing collection map for dsSet \"{}\"", datastore);
            collectionMap.clear();
        } else {
            logger.debug("Creating new entry in collection map for dsSet \"{}\"", datastore);
            collectionMap.put(datastore, new HashMap<>());
        }

        if (sequenceMap.containsKey(datastore)) {
            logger.debug("Clearing sequence map for dsSet \"{}\"", datastore);
            sequenceMap.clear();
        } else {
            logger.debug("Creating new entry in sequence map for dsSet \"{}\"", datastore);
            sequenceMap.put(datastore, new HashMap<>());
        }

        if (activationMap.containsKey(datastore)) {
            logger.debug("Clearing activation map for dsSet \"{}\"", datastore);
            activationMap.clear();
        } else {
            logger.debug("Creating new entry in sequence map for dsSet \"{}\"", datastore);
            activationMap.put(datastore, new HashMap<>());
        }

        try {
            for (String className : classList) {
                loadClass(datastore, className);
            }
        } catch (OperationException ex) {
            collectionMap.remove(datastore);
            sequenceMap.remove(datastore);
            activationMap.remove(datastore);
            throw new OperationException(ex.getErrorCode(), ex.getMessage());
        }
    }

    /**
     * load a specific trigger class into the database
     *
     * @param datastore : name of dsSet
     * @param className : name of class to be loaded into memory
     * @throws OperationException
     */
    private void loadClass(final String datastore, final String className) throws OperationException{
        RestrictedClassLoader blobCityClassLoader = loaderStore.getLoaderWithCreate(datastore);

        if (blobCityClassLoader.isReloadRequired(className)) {
            logger.debug("Reload required for {}", className);
            collectionMap.remove(datastore);
            sequenceMap.remove(datastore);
            activationMap.remove(datastore);
            blobCityClassLoader = loaderStore.getNewLoader(datastore);
        }

        try {
            final Class loadedClass = blobCityClassLoader.loadClass(className, true);
            processClass(datastore, loadedClass);
        } catch (ClassNotFoundException ex) {
            logger.error("Error while loading class \"" + className + "\" for dsSet \"" + datastore + "\"",  ex);
            throw new OperationException(ErrorCode.CLASS_LOAD_ERROR, "Could not load class. " + ex.getMessage());
        }
    }

    /**
     * process a trigger class to check whether it matches all the criteria for it to be trigger
     *
     * @param datastore
     * @param triggerClass
     * @throws OperationException
     */
    private void processClass(final String datastore, final Class triggerClass) throws OperationException {

        // checking if interface is implemented or not.
        if( !Triggerable.class.isAssignableFrom(triggerClass)) {
            throw new OperationException(ErrorCode.TRIGGER_LOAD_ERROR, "Specified Trigger " + triggerClass.getCanonicalName() + " inside dsSet " + datastore + " doesn't implement Triggerable interface" );
        }

        logger.trace("processClass({}, {}) as trigger", new Object[]{datastore, triggerClass.getCanonicalName()});

        // checking for trigger in Manifest file
        List<String> triggers = manifestParser.getTriggers(datastore);
        if ( !triggers.contains(triggerClass.getSimpleName()) ){
            throw new OperationException(ErrorCode.TRIGGER_LOAD_ERROR, "No such trigger is defined in Manifest file");
        }

        String triggerTableName = null;
        Integer sequenceNum = -1;
        Boolean isActivated = false;

        // checking for 'trigger' annotation
        final Annotation[] annotations = triggerClass.getAnnotations();
        for(Annotation annotation: annotations){
            if( "com.blobcity.db.sp.annotations.Trigger".equals(annotation.annotationType().getCanonicalName()) ){
                if(triggerTableName == null){
                    logger.info(annotation.toString());
                    if(!annotation.toString().contains("sequence")){
                        triggerTableName = annotation.toString().substring(annotation.toString().indexOf("table=") + 6, annotation.toString().length() - 1);
                        // no sequence number found, set to default
                        sequenceNum = -1;
                        logger.debug("{} found annotation TriggerStoreBean[collection={}] with no sequence number", new Object[]{triggerClass.getCanonicalName(), triggerTableName});
                    }
                    else{
                        Trigger triggerAnnotation = (Trigger) annotation;

                        String triggerAnno = annotation.toString();
                        Integer commaIndex = triggerAnno.indexOf(",");
                        Integer sequenceIndex = triggerAnno.indexOf("sequence=");
                        Integer tableIndex = triggerAnno.indexOf("table=");
                        if(sequenceIndex < tableIndex){
                            triggerTableName = triggerAnno.substring(triggerAnno.indexOf("table=") + 6, triggerAnno.length()-1);
                            sequenceNum = Integer.valueOf(triggerAnno.substring(triggerAnno.indexOf("sequence=") + 9, commaIndex));
                        }
                        else{
                            triggerTableName = triggerAnno.substring(triggerAnno.indexOf("table=") + 6, commaIndex);
                            sequenceNum = Integer.valueOf(triggerAnno.substring(triggerAnno.indexOf("sequence=") + 9, triggerAnno.length()-1));
                        }
                        logger.debug("{} found annotation TriggerStoreBean[collection={}] with sequence number {}", new Object[]{triggerClass.getCanonicalName(), triggerTableName, sequenceNum});
                    }
                }
                else{
                    logger.debug("{} found duplicate annotation TriggerName[table={}]", new Object[]{triggerClass.getCanonicalName(), triggerTableName});
                    throw new OperationException(ErrorCode.TRIGGER_LOAD_ERROR, "Duplicate @Trigger "
                            + "annotation found on class " + triggerClass.getCanonicalName() + ". A single trigger class "
                            + "may have only one @Trigger annotation associated with it.");
                }
            }
        }
        if( triggerTableName.isEmpty() || triggerTableName == null){
            throw new OperationException(ErrorCode.TRIGGER_LOAD_ERROR, "collection Name can't be null for a trigger");
        }
        // check if there is entry for that specific db
        if(!collectionMap.containsKey(datastore)){
            collectionMap.put(datastore, new HashMap<>());
        }
        if(!sequenceMap.containsKey(datastore)){
            sequenceMap.put(datastore, new HashMap<>());
        }
        if(!activationMap.containsKey(datastore)){
            activationMap.put(datastore, new HashMap<>());
        }

        if( !collectionManager.exists(datastore, triggerTableName) )
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Table specified in trigger (" + triggerTableName + ")doesn't exist inside dsSet "+ datastore);

        // all checks verified
        collectionMap.get(datastore).put(triggerClass.getCanonicalName(), triggerTableName);
        sequenceMap.get(datastore).put(triggerClass.getCanonicalName(), sequenceNum);
        activationMap.get(datastore).put(triggerClass.getCanonicalName(), isActivated);

    }

    /**
     * unload all triggers from the database for a given dsSet
     *
     * @param datastore : name of dsSet
     */
    public void removeAll(final String datastore){
        logger.debug("Clearing trigger collection map for dsSet \"{}\"", datastore);
        collectionMap.remove(datastore);
        logger.debug("Clearing trigger sequence map for dsSet \"{}\"", datastore);
        sequenceMap.remove(datastore);
        logger.debug("Clearing trigger activation map for dsSet \"{}\"", datastore);
        activationMap.remove(datastore);
        logger.debug("All triggers unloaded from database for dsSet \"{}\"", datastore);
    }

    /**
     * unload a specific trigger from database for a given dsSet
     *
     * @param datastore : name of dsSet
     * @param triggerClassName : full name of trigger class
     */
    public void removeClass(final String datastore, final String triggerClassName) {
        collectionMap.get(datastore).remove(triggerClassName);
        sequenceMap.get(datastore).remove(triggerClassName);
        activationMap.get(datastore).remove(triggerClassName);
    }


    /** DEBUG FUNCTIONS **/

    /**
     * This is a debug function, it returns the complete mapping for every hashmap in here
     *
     * @param datastore : database id
     * @return all info (name, table, activation status, sequence)  about the triggers related to given database
     */
    public String getTriggerMappings(final String datastore){
        StringBuilder sb = new StringBuilder();
        Map<String, String> currTriggerMap = collectionMap.get(datastore);
        Map<String, Boolean> currActMap = activationMap.get(datastore);
        Map<String, Integer> currSeqMap = sequenceMap.get(datastore);
        for(String trigger: currTriggerMap.keySet()){
            sb.append( trigger ).append(",");
            sb.append( currTriggerMap.get(trigger) ).append(",");
            sb.append( currActMap.get(trigger) ).append(",");
            sb.append( currSeqMap.get(trigger) ).append("\n");
        }
        return sb.toString();
    }
    
    /**
     * This is a debug function, it returns the complete mapping for every hashmap in here
     *
     * @param datastore : database id
     * @param collection : collection name
     * @return all info about all triggers related to given database and collection
     */
    public String getTriggerMappings(final String datastore, final String collection){
        StringBuilder sb = new StringBuilder();
        Map<String, String> currTriggerMap = collectionMap.get(datastore);
        Map<String, Boolean> currActMap = activationMap.get(datastore);
        Map<String, Integer> currSeqMap = sequenceMap.get(datastore);
        for(String trigger: currTriggerMap.keySet()){
            if( currTriggerMap.get(trigger).equals(collection) ){
                sb.append( trigger ).append(",");
                sb.append( currTriggerMap.get(trigger) ).append(",");
                sb.append( currActMap.get(trigger) ).append(",");
                sb.append( currSeqMap.get(trigger) ).append("\n");
            }
        }
        return sb.toString();
    }
    
}
