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

package com.blobcity.db.apache.tikka;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.*;

/**
 * @author sanketsarang
 */
public class TikaAutoInterpreter implements TikaInterpreter {

    @Override
    public String toText(String filePath) throws OperationException {
        AutoDetectParser parser = new AutoDetectParser();
        BodyContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        try (InputStream stream = new FileInputStream(new File(filePath))) {
            parser.parse(stream, handler, metadata);
            return handler.toString();
        } catch (IOException | SAXException | TikaException e) {
            throw new OperationException(ErrorCode.UNRECOGNISED_DOCUMENT_FORMAT, "Could not auto-detect document for reading");
        }
    }

    @Override
    public JSONObject toJson(String filePath) throws OperationException {

        AutoDetectParser parser = new AutoDetectParser();
        BodyContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        try (InputStream stream = new FileInputStream(new File(filePath))) {
            parser.parse(stream, handler, metadata);
        } catch (IOException | SAXException | TikaException e) {
            throw new OperationException(ErrorCode.UNRECOGNISED_DOCUMENT_FORMAT, "Could not auto-detect document for reading");
        }

        final String fileText = handler.toString();
        if(fileText == null || fileText.isEmpty()) {
            throw new OperationException(ErrorCode.UNRECOGNISED_DOCUMENT_FORMAT, "Attempting to import an empty document");
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("_txt", fileText);

        String[] metadataNames = metadata.names();
        for(String name : metadataNames) {
            jsonObject.put(name, metadata.get(name));
        }

        return jsonObject;
    }
}
