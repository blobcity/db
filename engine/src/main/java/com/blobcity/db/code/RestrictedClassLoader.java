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

import com.blobcity.db.constants.BSql;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class loader to allow the database to load classes per application ID
 *
 * @author sanketsarang
 * @author javatarz (Karun Japhet)
 */
public class RestrictedClassLoader extends ClassLoader {

    private static final Logger logger = LoggerFactory.getLogger(RestrictedClassLoader.class.getName());
    private static final String ROOT = BSql.BSQL_BASE_FOLDER;
    private final Map<String, Long> lastModifiedTimestamp = new HashMap<>();
    private final String codeBase;

    public RestrictedClassLoader(final String codeBase) {
        this.codeBase = codeBase;
    }

    public String getCodeBase() {
        return codeBase;
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        // load from parent
        Class<?> result = findLoadedClass(name);
        if (result != null) {
            return result;
        }

        try {
            result = findSystemClass(name);
        } catch (Exception e) {
            // Ignore these
        }
        if (result != null) {
            return result;
        }
        //result = classesMap.get(name);
        if (result == null) {
            throw new ClassNotFoundException(name);
        }
        return result;
    }

    @Override
    public Class loadClass(String className, boolean resolve)
            throws ClassNotFoundException {

        Class cls = findLoadedClass(className);

        final String clsFilePath = ROOT + codeBase + className.replaceAll("[.]", "/") + ".class";
        final File f = new File(clsFilePath);
        if (cls != null) {
            if (logger.isDebugEnabled()) {
                final DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
                logger.debug("Found {} and didn't reload it. Existing timestamp: {}. Current timestamp: {}", new Object[]{className, df.format(new Date(lastModifiedTimestamp.get(clsFilePath))), df.format(new Date(f.lastModified()))});
            }

            return cls;
        }

        try {
            cls = findSystemClass(className);
            if (cls != null) {
                logger.debug("Inside codebase {} loaded system class {}", codeBase, className);
                return cls;
            }
        } catch (ClassNotFoundException ex) {
            //do nothing
        }

        // 3. get bytes for class
        // Get size of class file
        final int size = (int) f.length(); // max class file size = 2GB
        // Reserve space to read
        final byte[] classBytes = new byte[size];

        // Get stream to read from
        try (final FileInputStream fis = new FileInputStream(f); final DataInputStream dis = new DataInputStream(fis)) {
            // Read the file completely
            dis.readFully(classBytes);

            lastModifiedTimestamp.put(clsFilePath, f.lastModified());
            logger.debug("Inside codebase {}, loaded class {}", new Object[]{codeBase, className});
        } catch (IOException ex) {
            logger.error("Class file \"" + clsFilePath + "\" couldn't be read", ex);
            throw new ClassNotFoundException("Could not read class file for class " + clsFilePath, ex);
        }

        if (classBytes == null) {
            throw new ClassNotFoundException("Cannot load class: " + className);
        }

        // 4. turn the byte array into a Class
        try {
            cls = defineClass(className, classBytes, 0, classBytes.length);
            if (resolve) {
                resolveClass(cls);
            }
        } catch (SecurityException e) {
            if (isRestricted(className)) {
                throw new ClassNotFoundException(className);
            }

            // loading core java classes such as java.lang.String
            // is prohibited, throws java.lang.SecurityException.
            // delegate to parent if not allowed to load class
            cls = super.loadClass(className, resolve);
        } catch (ClassFormatError ex) {
            throw new ClassNotFoundException("Could not successfully parse class: " + className);
        }

        logger.debug("Inside codebase {}, class was successfully resolved and loaded: {}", codeBase, className);

        return cls;
    }

    /**
     * Returns whether a built in JDK classes is restricted from being used on the BlobCity Developer portal or not.
     *
     * @param className The fully qualified class name of the JDK class
     * @return false if the class is permitted, true otherwise
     *
     * @author sanketsarang
     * @since 1.0
     */
    private boolean isRestricted(String className) {
        return className.startsWith("java.io") || className.startsWith("java.nio");
    }

    public static boolean exists(String className) {
        String clsFile = ROOT + className.replaceAll("[.]", "/") + ".class";
        File file = new File(clsFile);
        return file.exists();
    }

    /**
     * Checks if the current class loader instance is outdated and needs to be abandoned to be replaced by a newer one. This method allows the database to check
     * if there is a newer class file uploaded after a {@code load-code} is requested
     *
     * @param className class to be tested
     * @return {@code true} if a reload is required, else {@code false}
     */
    public boolean isReloadRequired(final String className) {
        final String clsFilePath = ROOT + codeBase + className.replaceAll("[.]", "/") + ".class";
        final long fileLastModifiedTime = new File(clsFilePath).lastModified();
        final Long loadedLastModifiedTime = lastModifiedTimestamp.get(clsFilePath);

        final boolean reloadRequired = loadedLastModifiedTime != null && loadedLastModifiedTime < fileLastModifiedTime;

        if (logger.isDebugEnabled()) {
            final DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
            logger.debug("File: {}. Existing timestamp: {}. Current timestamp: {}. Reload Required: {}", new Object[]{className, loadedLastModifiedTime == null ? "null" : df.format(new Date(loadedLastModifiedTime)), df.format(new Date(fileLastModifiedTime)), reloadRequired});
        }

        return reloadRequired;
    }
}
