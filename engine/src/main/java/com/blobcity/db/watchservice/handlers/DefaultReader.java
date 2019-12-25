package com.blobcity.db.watchservice.handlers;

import com.blobcity.db.constants.BSql;
import org.json.JSONObject;

public class DefaultReader implements GenericFileReader {

    private final String baseFolder;
    private final String filePath;

    public DefaultReader(final String baseFolder, final String filePath) {
        this.baseFolder = baseFolder;
        this.filePath = filePath;
    }

    @Override
    public JSONObject getJsonRepresentation() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("__type", "unknown");
        jsonObject.put("__rc_type", "file");
        try {
            jsonObject.put("__type_spec", filePath.substring(filePath.lastIndexOf(".") + 1));
        }catch(Exception ex) {
            //do nothing
        }
        jsonObject.put("__base", this.baseFolder);
        try {
            jsonObject.put("__file", filePath.substring(filePath.lastIndexOf(BSql.SEPERATOR) + 1));
        } catch(Exception ex) {
            jsonObject.put("__file", filePath);
        }

        //TODO: Extract more meta information around images here

        return jsonObject;
    }
}
