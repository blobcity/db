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

package com.blobcity.lib.database.console.end.point.service;

import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.database.bean.manager.interfaces.engine.ConsoleExecutor;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.blobcity.lib.database.bean.manager.interfaces.security.SecurityManager;
import java.util.Date;
import org.json.JSONObject;

/**
 * Thread running a single console session. One instance of this class is spawned in a separate thread for every console
 * based session that is opened. this service acts as a mediator between the users command line / terminal window and
 * the databases {@link ConsoleExecutor}
 *
 * @author sanketsarang
 */
public class ConsoleEndPointService implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleEndPointService.class);

    private final Socket socket;
    private final ConsoleExecutor consoleExecutor;
    private final BufferedWriter writer;
    
    public ConsoleEndPointService(final Socket socket) {
        OutputStreamWriter osWriter;
        this.socket = socket;
        consoleExecutor = (ConsoleExecutor) BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig").getBean("ConsoleExecutorBean");
        try {
            osWriter = new OutputStreamWriter(socket.getOutputStream());
        } catch (IOException ex) {
            logger.error("Error occured in starting CLI session.");
            writer = null;
            return;
        }

        writer = new BufferedWriter(osWriter);
        if (consoleExecutor == null) {
            logger.error("Error occured in starting CLI session.");
        }
    }

    @Override
    public void run() {
        String line;
        String username = null;

        if (consoleExecutor == null) {
            logger.error("CLI listener quitting as error occured during instantiation of required backing resources");
            exit();
            return;
        }

        if (writer == null) {
            logger.error("CLI listener quitting because of an IO error");
            exit();
            return;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
            SecurityManager securityManager = (SecurityManager) BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig").getBean("SecurityManagerBean");
            
            logger.info("Incoming connection on console end point from " + socket.getRemoteSocketAddress().toString());
            /* Validate credentials */
            boolean authenticated = false;
            int retryCount = 0;
            
            JSONObject loginAttempt;
            while (!authenticated) {
                writer.write("username>");
                writer.flush();
                username = reader.readLine();
                writer.write("password>");
                writer.flush();
                final String password = reader.readLine();
                authenticated = securityManager.verifyCredentials(username, password);

                loginAttempt = new JSONObject();
                loginAttempt.put("ip", socket.getRemoteSocketAddress().toString())
                        .put("time", new Date())
                        .put("user", username)
                        .put("authentication", authenticated ? "success":"failed");
                
                if(authenticated) logger.info(loginAttempt.toString());
                else logger.warn(loginAttempt.toString());
                
                if (!authenticated) {
                    writeLine(writer, "Invalid username or password entered. Please try again.");
                    retryCount++;
                    if (retryCount == 3) {
                        writeLine(writer, "Exceeded maximum permitted retries");
                        exit();
                        return;
                    }
                }
            }

            writeLine(writer, "You are now inside the BlobCity DB console");
            writeLine(writer, "Type 'help' for assistance and 'exit' to quit");
            writer.write("blobcity>");
            writer.flush();

            boolean insertMode = false;
            String insertDs = null;
            String insertCollection = null;
            String insertType = null;
            while ((line = reader.readLine()) != null) {

                if(insertMode) {
                    if(line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                        insertMode = false;
                        insertDs = null;
                        insertCollection = null;
                        insertType = null;
                        writeLineWithBC(writer, "Exited insert mode");
                        continue;
                    } else {
                        final String insertStatus = consoleExecutor.insertData(username, insertDs, insertCollection, insertType, line);
                        writeLine(writer, insertStatus);
                        continue;
                    }
                }

                if(line.startsWith("insert into")) {
                    String []elements = line.split(" ");
                    if(elements.length != 4 || !elements[2].contains(".")) {
                        writeLineWithBC(writer,"Invalid syntax. Must be: insert into ds.collection dataType");
                        continue;
                    }


                    insertDs = elements[2].substring(0, elements[2].indexOf("."));
                    insertCollection = elements[2].substring(elements[2].indexOf(".") + 1, elements[2].length());
                    insertType = elements[3];

                    switch(insertType.toUpperCase()) {
                        case "JSON":
                        case "XML":
                        case "CSV":
                        case "WORD":
                        case "EXCEL":
                        case "PPT":
                        case "TEXT":
                        case "IMAGE":
                        case "AUDIO":
                        case "VIDEO":
                            break;
                        default:
                            writeLineWithBC(writer, "Invalid data type. Must be one off: JSON, XML, CSV, WORD, EXCEL, PPT, TEXT, IMAGE, AUDIO, VIDEO");
                            continue;
                    }

                    insertMode = true;
                    writeLine(writer, "In insert mode. Type 1 " + insertType + " per line and press enter to insert");
                    continue;
                }

                switch (line) {
                    case "exit":
                    case "quit":
                        writer.write("Closing console. Bye!");
                        writer.newLine();
                        writer.flush();
                        exit();
                        return;
                    default:
                        String response = consoleExecutor.runCommand(username, line);
                        writer.write(response);
                        writer.newLine();
                        writer.write("blobcity>");
                        writer.flush();
                }
            }
        } catch (IOException ex) {
            logger.error("An IO exception occured while reading/writing on the CLI socket", ex);
        }
    }

    private void exit() {
        try {
            socket.close();
        } catch (IOException ex) {
            logger.error("Error occured in closing CLI session", ex);
        }
    }
    
    private void writeLine(BufferedWriter writer, String line) throws IOException {
        writer.write(line);
        writer.newLine();
        writer.flush();
    }

    private void writeLineWithBC(BufferedWriter writer, String line) throws IOException {
        writer.write(line);
        writer.newLine();
        writer.write("blobcity>");
        writer.flush();
    }
}
