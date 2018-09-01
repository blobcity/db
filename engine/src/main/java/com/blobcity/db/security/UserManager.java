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
import com.blobcity.util.security.PasswordHash;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * This class is used to maintain operations related to users
 * 
 * @author sanketsarang
 */
@Component
public class UserManager {
    private static final Logger logger = LoggerFactory.getLogger(UserManager.class);
    
    @Autowired
    private BSqlDataManager dataManager;
    @Autowired @Lazy
    private SQLExecutorBean sqlExecutor;
    
    /**
     * Adds a user to the systemdb.User table
     *
     * @param user A user object containing a valid username and password
     * @throws com.blobcity.db.exceptions.OperationException If the insert operation fails
     */
    public synchronized void addUser(final User user) throws OperationException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }
        if (StringUtils.isBlank(user.getUsername())) {
            throw new IllegalArgumentException("username cannot be null");
        }
        if (StringUtils.isBlank(user.getPassword())) {
            throw new IllegalArgumentException("password cannot be null");
        }
        String defaultPassHash = PasswordHash.createHash(user.getPassword());
        JSONObject json = new JSONObject();
        json.put("username", user.getUsername());
        json.put("password", defaultPassHash);
        json.put("defaultRole", user.getDefaultRole());
        dataManager.insert(BSql.SYSTEM_DB, "user", json);
    }
    
    public synchronized void renameUser() throws OperationException{
        
    }
    
    /**
     * Deletes the user with the specified username
     *
     * @param username The user to delete
     * @throws OperationException If the delete operation fails
     */
    public synchronized void deleteUser(final String username) throws OperationException {
        if (username == null) {
            throw new IllegalArgumentException("Username must not be null");
        }
        dataManager.remove(BSql.SYSTEM_DB, "user", username);
    }
    
    /**
     * Updates the specified user's password
     *
     * @param username The username of the user. Must not be null
     * @param newPassword The new password of the user. Must not be null
     * @throws com.blobcity.db.exceptions.OperationException If the update operation fails
     */
    public synchronized void updatePassword(final String username, final String newPassword) throws OperationException {
        if (username == null) {
            throw new IllegalArgumentException("Username must not be null");
        }
        if (newPassword == null) {
            throw new IllegalArgumentException("Password must not be null");
        }
        Optional<User> optUser = fetchUser(username);
        if (!optUser.isPresent()) {
            throw new OperationException(ErrorCode.PRIMARY_KEY_INEXISTENT);
        }
        User user = optUser.get();
        JSONObject json = new JSONObject();
        if(user.get_id() == null || user.get_id().isEmpty()) {
            json.put("_id", UUID.randomUUID().toString());
        } else {
            json.put("_id", user.get_id());
        }
        json.put("username", user.getUsername());
        json.put("password", PasswordHash.createHash(newPassword));
        json.put("defaultRole", user.getDefaultRole());
        dataManager.save(BSql.SYSTEM_DB, "user", json);
    }
    
    /**
     * Fetches a User object for the specified username
     *
     * @param username The username of the user to fetch. Must not be null
     * @return An Optional User object
     */
    public Optional<User> fetchUser(String username) {
        if (username == null) {
            throw new IllegalArgumentException("username must not be null");
        }
        //TODO escape the username when parameterized queries are supported
        String sql = "SELECT * FROM `user` WHERE username='" + username + "'";
        logger.debug("Finding user: " + username);
        String result = sqlExecutor.executePrivileged(".systemdb", sql);
        logger.debug("query result: " + result);
        if (!JsonUtil.isAck(result)) {
            return Optional.empty();
        }
        JSONObject userJSON = new JSONObject(result);
        JSONArray userResults = userJSON.getJSONArray("p");
        if (userResults.length() == 0) {
            return Optional.empty();
        } else if (userResults.length() > 1) {
            String err = "More than one users found for username: " + username;
            logger.error(err);
            throw new DbRuntimeException(err);
        }

        assert userResults.length() == 1;
        try {
            User user = new Gson().fromJson(userResults.getJSONObject(0).toString(), User.class);
            logger.debug("User " + username + " found: " + user.toString());
            return Optional.of(user);
        } catch (JsonSyntaxException e) {
            logger.debug("User " + username + " not found: ", result);
            return Optional.empty();
        }
    }
    
}
