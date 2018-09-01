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

package com.blobcity.db.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sanketsarang
 */
public class SystemInputUtil {

    private static final Logger logger = LoggerFactory.getLogger(SystemInputUtil.class.getName());

    public static boolean captureYesNoInput(String message) {
        logger.debug(message);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                final String line = reader.readLine();

                /* If line is null it means DB is running on a server or within a container where input capture is
                not possible. Upgrade by default in such cases.
                 */
                if(line == null) {
                    return true;
                }

                switch (line.toLowerCase()) {
                    case "y":
                    case "yes":
                        return true;
                    case "n":
                    case "no":
                        return false;
                    default:
                        System.out.println("Invalid input. Please enter (y/n)?");
                }
            }
        } catch (IOException ex) {
            logger.error("Input capture from System.in failed", ex);
            return false;
        }
    }

    public static String captureLineInput(String message) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            return reader.readLine();
        } catch (IOException ex) {
            logger.error("Input capture from System.in failed", ex);
            return null;
        }
    }
}
