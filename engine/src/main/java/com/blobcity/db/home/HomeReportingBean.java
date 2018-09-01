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

package com.blobcity.db.home;

import com.blobcity.db.constants.HomeReporting;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class HomeReportingBean {

    private static final Logger logger = LoggerFactory.getLogger(HomeReportingBean.class);

    public void registerWithBlobCity(final String nodeId) {
        URI uri;

//        try {
//            logger.info("Registering and activating node");
//            uri = new URI(HomeReporting.REGISTER_NODE_ENDPOINT.replace(HomeReporting.NODE_ID_PARAM, nodeId));
//            try (BufferedReader reader = new BufferedReader(new InputStreamReader(uri.toURL().openStream()))) {
//                String line = reader.readLine();
//                try {
//                    JSONObject jsonObject = new JSONObject(line);
//                    logger.debug("Registeration response: ");
//                    if (jsonObject.has(HomeReporting.ACK_PARAM) && HomeReporting.ACK_SUCCESS_RESPONSE.equals(jsonObject.getString(HomeReporting.ACK_PARAM))) {
//                        logger.info("Remote activation with BlobCity successful");
//                    }
//                } catch (JSONException ex) {
//                    //logger.info("Remote activation with BlobCity failed!");
//                }
//            } catch (MalformedURLException ex) {
////                logger.error("Remote activation with BlobCity faileøød!", ex);
//            } catch (IOException ex) {
////                logger.error("Remote activation with BlobCity failed!", ex);
//            }
//        } catch (URISyntaxException ex) {
////            logger.error("Remote activation with BlobCity failed!", ex);
//        }
    }
}
