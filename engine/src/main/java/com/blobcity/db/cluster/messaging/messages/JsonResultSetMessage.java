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

package com.blobcity.db.cluster.messaging.messages;

import com.blobcity.db.exceptions.OperationException;
import org.json.JSONObject;

/**
 * <p>
 * Message used to represent a query response in JSON format. Both JSON & SQL queries at this point in time will
 * generate a JSON response represented by this object.
 *
 * <p>
 * The object has a <code>page</code> and <code>pages</code> properties, where <code>page</code> represents the current
 * page number of a result set and <code>pages</code> represents in the number of pages expected. If <code>page</code>
 * and <code>pages</code> combination is expressed as (page, pages) then (1,1) indicates a single page response.
 *
 * <p>
 * A response of (1,0) indicates the current page is the first page but the total number of pages is not yet determined
 * in the case of streamed response. For such streamed responses, the last page will come in the form of (n,-1) where n
 * is the number of last page. The -1 will mark the end of stream.
 *
 * <p>
 * A response of (1,10) indicates that the current response is for page 1 out of a total of 10 pages. Typically
 * responses with ORDER BY clauses will have the total number of pages accurately mentioned.
 *
 * @author sanketsarang
 */
public class JsonResultSetMessage extends AbstractMessage implements Message {

    private final MessageTypes messageType = MessageTypes.JSON_RESULT_SET;
    private JSONObject jsonResult;
    private int page;
    private int pages;

    @Override
    public void init(JSONObject jsonObject) throws OperationException {
        superInit(jsonObject);
        super.request = false;
        JSONObject payloadJson = jsonObject.getJSONObject("p");
        this.jsonResult = payloadJson.getJSONObject("r");
        this.page = payloadJson.getInt("pg");
        this.pages = payloadJson.getInt("pgs");
    }

    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = superToJson();
        JSONObject payloadJson = new JSONObject();
        payloadJson.put("r", jsonResult);
        payloadJson.put("pg", page);
        payloadJson.put("pgs", pages);
        jsonObject.put("p", payloadJson);
        return jsonObject;
    }
    
    public void init(JsonQueryMessage jsonQueryMessage) {
        this.request = false;
        this.requestId = jsonQueryMessage.getRequestId();
        this.masterNodeId = jsonQueryMessage.getMasterNodeId();
    }

    @Override
    public MessageTypes getMessageType() {
        return messageType;
    }

    public JSONObject getJsonResult() {
        return jsonResult;
    }

    public void setJsonResult(JSONObject jsonResult) {
        this.jsonResult = jsonResult;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getPages() {
        return pages;
    }

    public void setPages(int pages) {
        this.pages = pages;
    }
}
