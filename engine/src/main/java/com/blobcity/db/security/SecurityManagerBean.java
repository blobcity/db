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

import com.blobcity.db.bsql.BSqlDatastoreManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.security.exceptions.BadPasswordException;
import com.blobcity.db.security.exceptions.BadUsernameException;
import com.blobcity.db.security.exceptions.InvalidCredentialsException;
import com.blobcity.util.security.PasswordHash;

import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.asn1.cms.CMSObjectIdentifiers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.expression.Operation;
import org.springframework.stereotype.Service;
import com.blobcity.lib.database.bean.manager.interfaces.security.SecurityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author akshaydewan
 */
@Service
public class SecurityManagerBean implements SecurityManager {
    
    private static final Logger logger = LoggerFactory.getLogger(SecurityManagerBean.class);
 
    @Autowired @Lazy
    private UserManager userManager;
    @Autowired @Lazy
    private ApiKeyManager apiKeyManager;
    @Autowired @Lazy
    private BSqlDatastoreManager datastoreManager;

    /**
     * Verifies whether the password of the specified user
     *
     * @param username The username of the user
     * @param password The user's password
     * @return <code>true</code> if the username isPresent and the password is correct, <code>false</code> otherwise
     */
    @Override
    public boolean verifyCredentials(final String username, final String password) {
        Optional<User> user = userManager.fetchUser(username);
        if (user.isPresent()) {
            return PasswordHash.validatePassword(password, user.get().getPassword());
        } else {
            return false;
        }
    }

    /**
     * Verifies if the provided access key has an entry in the ApiKeys table
     * @param apiKey the api key to check
     * @return <code>true</code> if an entry for the key is found, <code>false</code> otherwise
     */
    @Override
    public boolean verifyKey(final String apiKey) {
        try {
            apiKeyManager.validateKey(apiKey);
            return true;
        } catch (OperationException e) {
            return false;
        }
    }

    /**
     * Verifies if the api key is valid if that key is a DS level key and permits access to the specified ds.
     * @param apiKey the api key
     * @param ds name of datastore
     * @return <code>true</code> if the key and ds combination is valid, <code>false</code> otherwise
     */
    public boolean verifyDsKey(final String apiKey, final String ds) {
        try {
            apiKeyManager.validateDsAccess(apiKey, ds);
            return true;
        } catch (OperationException e) {
            return false;
        }
    }

    /**
     * Creates a new master key. Only an existing master key holder or the root user may create a new master key. It is
     * responsibility of the invoker to ensure proper privileges before calling this function.
     * @return a new master key
     * @throws OperationException if an internal error occurs
     */
    public String createMasterKey() throws OperationException {
        return apiKeyManager.createMasterKey();
    }

    /**
     * Creates a new Ds level key. Only an existing master key holder or the same DS key holder or the root user may
     * perform this operation. It is responsibility of the invoker to ensure proper privileges before calling this
     * function.
     * @param ds name of datastore
     * @return the newly created DS level key
     * @throws OperationException if an internal error occurs
     */
    public String createDsKey(final String ds) throws OperationException {
        if(!datastoreManager.exists(ds)) {
            throw new OperationException(ErrorCode.INVALID_DATASTORE_NAME, "No datastore found with name: " + ds);
        }
        return apiKeyManager.createDsKey(ds);
    }

    /**
     * Gets a list of all API keys. Includes master and ds keys
     * @return a list of all API keys
     * @throws OperationException if an error occurs in fetching the keys
     */
    public List<String> getApiKeys() throws OperationException {
        return apiKeyManager.listKeys();
    }

    public List<String> getDsApiKeys(final String ds) throws OperationException {
        return apiKeyManager.listDsKeys(ds);
    }

    public void dropApiKey(final String key) throws OperationException {
        apiKeyManager.revokeKey(key);
    }

    /**
     * Changes the password of the specified user after verifying the current password. (TODO Add support for superuser
     * changing user's password without verifying current password)
     *
     * @param username The user's username
     * @param oldPassword The user's current password
     * @param newPassword The user's new password. Must not be blank
     * @throws InvalidCredentialsException If the user cannot be found or if the `oldPassword` is invalid
     * @throws BadPasswordException If the `newPassword` is not valid
     * @throws com.blobcity.db.exceptions.OperationException If the DB operation fails.
     */
    public void changePassword(final String username, final String oldPassword, final String newPassword)
            throws InvalidCredentialsException, BadPasswordException, OperationException {
        Optional<User> user = userManager.fetchUser(username);
        if (!user.isPresent()) {
            throw new InvalidCredentialsException("No such user: " + username);
        }
        if (!PasswordHash.validatePassword(oldPassword, user.get().getPassword())) {
            throw new InvalidCredentialsException("Authentication failed");
        }
        if (StringUtils.isBlank(newPassword)) {
            throw new BadPasswordException("Password cannot be blank");
        }
        userManager.updatePassword(user.get().getUsername(), newPassword);
    }

    /**
     *
     * Adds a user with the specified username and password. (TODO The session user must be verified before adding a
     * new user)
     *
     * @param username The username of the new user
     * @param password The password of the new user
     * @throws BadUsernameException If the username is invalid or if it already isPresent
     * @throws BadPasswordException If the password is invalid
     * @throws com.blobcity.db.exceptions.OperationException If the DB operation fails
     */
    public void addUser(final String username, final String password) throws BadUsernameException, BadPasswordException, OperationException {
        if (StringUtils.isBlank(username)) {
            throw new BadUsernameException("Username cannot be blank");
        }
        if (StringUtils.isBlank(password)) {
            throw new BadPasswordException("Password cannot be blank");
        }
        Optional<User> existingUser = userManager.fetchUser(username);
        if (existingUser.isPresent()) {
            throw new BadUsernameException("User " + username + " already isPresent");
        }
        User user = new User();
        user.setUsername(username);
        user.setPassword(password);
        user.setDefaultRole("");
        userManager.addUser(user);
    }

    /**
     * Deletes the specified user after validating their password. (TODO add support for superuser deleting users)
     *
     * @param username The user to delete
     * @param password The password of the user to delete
     * @throws BadUsernameException If the username is blank or does not exist
     * @throws InvalidCredentialsException If the username/password do not match
     * @throws com.blobcity.db.exceptions.OperationException If the DB operation fails
     */
    public void deleteUser(final String username, final String password) throws BadUsernameException, InvalidCredentialsException, OperationException {
        if (StringUtils.isBlank(username)) {
            throw new BadUsernameException("Username cannot be blank");
        }
        Optional<User> user = userManager.fetchUser(username);
        if (!user.isPresent()) {
            throw new BadUsernameException("User " + username + " does not exist");
        }
        if (!verifyCredentials(username, password)) {
            throw new InvalidCredentialsException("Authentication failed");
        }
        userManager.deleteUser(username);
    }

    /**
     * Deletes the specified user assuming that the current user has rights to delete the user. This function
     * will not do permission validation.
     *
     * @param username The user to delete
     * @throws BadUsernameException If the username is blank or does not exist
     * @throws com.blobcity.db.exceptions.OperationException If the DB operation fails
     */
    public void deleteUser(final String username) throws BadUsernameException,  OperationException {
        if (StringUtils.isBlank(username)) {
            throw new BadUsernameException("Username cannot be blank");
        }
        Optional<User> user = userManager.fetchUser(username);
        if (!user.isPresent()) {
            throw new BadUsernameException("User " + username + " does not exist");
        }
        
        userManager.deleteUser(username);
    }
}
