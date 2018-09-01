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

package com.blobcity.db.security;

import com.blobcity.db.bquery.SQLExecutorBean;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.DbRuntimeException;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.util.json.JsonUtil;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * This bean manages the user groups for the database.
 * The name of groups should be unique across database
 * 
 * @author sanketsarang
 */
@Component
public class UserGroupManager {
    private static final Logger logger = LoggerFactory.getLogger(UserGroupManager.class);
    
    @Autowired @Lazy
    private BSqlDataManager dataManager;
    @Autowired
    private SQLExecutorBean sqlExecutor;
    @Autowired @Lazy
    private UserManager userManager;
    
    
    /**
     * creates a new user group 
     * 
     * @param userGroup
     * @throws com.blobcity.db.exceptions.OperationException: if there is some issue in adding user
     */
    public synchronized void createGroup(final UserGroup userGroup) throws OperationException{
        if(userGroup == null){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Invalid information provided");
        }
        if( StringUtils.isEmpty(userGroup.getName())){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Group name can't be empty");
        }
        
        Optional<UserGroup> existing = getUserGroup(userGroup.getName());
        if(existing.isPresent()){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Group with this name already isPresent");
        }
        
        dataManager.insert(BSql.SYSTEM_DB, "usergroups", userGroup.toJSON());
    }
    
    /**
     * Get a user group with given name
     * 
     * @param groupName
     * @return : returns a userGroup object if isPresent otherwise empty object
     * @throws com.blobcity.db.exceptions.OperationException 
     */
    public Optional<UserGroup> getUserGroup(String groupName) throws OperationException{
        if(StringUtils.isEmpty(groupName)){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "group name can't be empty or null");
        }
        String sqlQuery = "select * from `usergroups` where name = '" + groupName + "'";
        logger.debug("Finding usergroup: " + groupName);
        String result = sqlExecutor.executePrivileged(".systemdb", sqlQuery);
        logger.debug("query result: " + result);
        if (!JsonUtil.isAck(result)) {
            return Optional.empty();
        }
        JSONObject userJSON = new JSONObject(result);
        JSONArray usergroupResults = userJSON.getJSONArray("p");
        if (usergroupResults.length() == 0) {
            return Optional.empty();
        } else if (usergroupResults.length() > 1) {
            String err = "More than one group found for name: " + groupName;
            logger.error(err);
            throw new DbRuntimeException(err);
        }

        assert usergroupResults.length() == 1;
        try {
            UserGroup userGroup = new Gson().fromJson(usergroupResults.getJSONObject(0).toString(), UserGroup.class);
            logger.debug("UserGroup " + groupName + " found: " + userGroup.toString());
            return Optional.of(userGroup);
        } catch (JsonSyntaxException e) {
            logger.debug("Usergroup " + groupName + " not found: ", result);
            return Optional.empty();
        }
    }
    
    /**
     * deletes a user group
     * 
     * @param name: name of group to be deleted
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public void deleteGroup(final String name) throws OperationException{
        if( StringUtils.isEmpty(name)){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Group name can't be empty");
        }
        
        Optional<UserGroup> existing = getUserGroup(name);
        if(existing.isPresent()){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Group with this name already isPresent");
        }
        dataManager.remove(BSql.SYSTEM_DB, "usergroups", name);
    }
    
    /**
     * rename an existing group
     * 
     * @param oldName: old name of group
     * @param newName: new name of group
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public void renameGroup(final String oldName, final String newName) throws OperationException{
        if( StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName) ){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Group name can't be empty");
        }
        Optional<UserGroup> existingGroup  = getUserGroup(oldName);
        if( !existingGroup.isPresent() ){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such group isPresent");
        }
        
        Optional<UserGroup> newGroup = getUserGroup(newName);
        if( newGroup.isPresent() ){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Group with new name already isPresent");
        }
        
        JSONObject json = existingGroup.get().toJSON();
        json.put("name", newName);
        dataManager.save(BSql.SYSTEM_DB, "usergroups", json);
    }
    
    /**
     * add an existing user to an existing group
     * 
     * @param groupName: name of group
     * @param username: name of user to be added to this group
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public void addUserToGroup(final String groupName, final String username) throws OperationException{
        Optional<UserGroup> usergroup = getUserGroup(groupName);
        if(!usergroup.isPresent()){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such group isPresent");
        }
        
        Optional<User> user = userManager.fetchUser(username);
        if(!user.isPresent()){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such user isPresent");
        }
        
        UserGroup group = usergroup.get();
        
        String existingUsers = group.getUsers();
        if( existingUsers.contains(username) ){
           return ; 
        }
        existingUsers = existingUsers+username+",";
        group.setUsers(existingUsers);
        
        dataManager.save(BSql.SYSTEM_DB, "usergroups", group.toJSON());
    }
    
    /**
     * remove an existing user from an existing group
     * 
     * @param groupName: name of group
     * @param username: name of user to be removed from this group
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public void removeUserFromGroup(final String groupName, final String username) throws OperationException{
        Optional<UserGroup> usergroup = getUserGroup(groupName);
        if(!usergroup.isPresent()){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such group isPresent");
        }
        
        Optional<User> user = userManager.fetchUser(username);
        if(!user.isPresent()){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such user isPresent");
        }
        
        UserGroup group = usergroup.get();
        
        String existingUsers = group.getUsers();
        if( !existingUsers.contains(username) ){
           return ; 
        }
        existingUsers = existingUsers.replace(username+",", "");
        group.setUsers(existingUsers);
        
        dataManager.save(BSql.SYSTEM_DB, "usergroups", group.toJSON());
        
    }
    
    /**
     * list all the groups present in the database
     * @return 
     */
    public String listGroups(){
        String sql = "Select * from `usergroups`";
        String result = sqlExecutor.executePrivileged(".systemdb", sql);
        // Error occured, tell user to try again
        if(!JsonUtil.isAck(result)){
            return "Internal Error Occured. Please try again";
        }
        
        JSONArray usergroups = new JSONObject(result).getJSONArray("p");
        if(usergroups.length() == 0){
            return "No groups present";
        }
        List<String> allGroups = new ArrayList<>();
        for(int i=0;i<usergroups.length();i++){
            UserGroup currGroup = new Gson().fromJson(usergroups.get(i).toString(), UserGroup.class);
            allGroups.add(currGroup.getName());
        }
        return allGroups.toString();
    }
    
    /**
     * get the information for a given group
     * 
     * @param groupname
     * @return
     * @throws OperationException 
     */
    public JSONObject getGroupInformation(final String groupname) throws OperationException{
        return getUserGroup(groupname).get().toJSON();
    }
    
    /**
     * check whether a group isPresent or not
     * 
     * @param groupname
     * @return
     * @throws OperationException if group name is empty or null
     */
    public boolean exists(final String groupname) throws OperationException{
        return getUserGroup(groupname).isPresent();
    }
    
}
