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

package com.blobcity.db.ftp;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.util.PathUtil;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.SaltedPasswordEncryptor;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sanketsarang
 */
@Component
public class FtpServerManager {

    private static final Logger logger = LoggerFactory.getLogger(FtpServerManager.class);

    private FtpServer ftpServer;
    private UserManager userManager;

    private final List<Authority> defaultAuthorities = new ArrayList<>();

    public void start() throws OperationException {

        PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        try {
            /* Make .ftpusers directory */
            new File(PathUtil.ftpUserCredentialsFolder()).mkdir();


            File userCredentialsFile = new File(PathUtil.ftpUserCredentialsFile());
            if(!userCredentialsFile.exists()) {
                userCredentialsFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not create local file for saving FTP credentials");
        }
        userManagerFactory.setFile(new File(PathUtil.ftpUserCredentialsFile()));
        userManagerFactory.setPasswordEncryptor(new SaltedPasswordEncryptor());
        userManager = userManagerFactory.createUserManager();

        FtpServerFactory serverFactory = new FtpServerFactory();

        ListenerFactory factory = new ListenerFactory();
        factory.setPort(2221);
        serverFactory.addListener("default", factory.createListener());
        serverFactory.setUserManager(userManager);

        FtpServer server = serverFactory.createServer();

        try {
            server.start();
            logger.info("FTP server started");
        } catch (FtpException e) {
            e.printStackTrace();
            logger.error("FTP server could not be started. Services related to FTP may not work");
        }

        /* Popuate default authority list */
        defaultAuthorities.add(new WritePermission());

    }

    public void setUser(final String datastore, final String password) throws OperationException {
        removeUser(datastore); //is a no-op if the user is not present

        BaseUser baseUser = new BaseUser();
        baseUser.setAuthorities(defaultAuthorities);
        baseUser.setName(datastore);
        baseUser.setPassword(password);
        baseUser.setHomeDirectory(PathUtil.datastoreFtpFolder(datastore));

        try {
            userManager.save(baseUser);
        } catch (FtpException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error occurred in creating FTP user for datastore: " + datastore);
        }
    }

    public void removeUser(final String datastore) throws OperationException {
        try{
            if(userManager.doesExist(datastore)) {
                userManager.delete(datastore);
            }
        } catch(FtpException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error removing FTP user");
        }
    }
}
