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

package com.blobcity.lib.query;

import com.blobcity.lib.data.Record;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.List;

/**
 * @author sanketsarang
 */
public class Query {
    private final JSONObject queryJson;

    public Query() {
        queryJson = new JSONObject();
    }

    public Query(final JSONObject queryJson) {
        this.queryJson = queryJson;
    }

    public Query user(final String user) {
        queryJson.put(QueryParams.USERNAME.getParam(), user);
        return this;
    }

    public Query requestId(final String requestId) {
        queryJson.put(QueryParams.REQUEST_ID.getParam(), requestId);
        return this;
    }

    public Query parentRequestId(final String parentRequestId) {
        queryJson.put(QueryParams.PARENT_REQUEST_ID.getParam(), parentRequestId);
        return this;
    }

    public Query masterNodeId(final String masterNodeId) {
        queryJson.put(QueryParams.MASTER_NODE_ID.getParam(), masterNodeId);
        return this;
    }

    public Query ack(final String ack) {
        queryJson.put(QueryParams.ACK.getParam(), ack);
        return this;
    }

    public Query ackSuccess() {
        queryJson.put(QueryParams.ACK.getParam(), "1");
        return this;
    }

    public Query ackFailure() {
        queryJson.put(QueryParams.ACK.getParam(), "0");
        return this;
    }

    public Query payload(Object payloadObject) {
        queryJson.put(QueryParams.PAYLOAD.getParam(), payloadObject);
        return this;
    }

    public Query time(long time) {
        queryJson.put(QueryParams.TIME.getParam(), time);
        return this;
    }

    public QueryType getQueryType() {
        return QueryType.fromString(queryJson.getString(QueryParams.QUERY.getParam()));
    }

    public String getUser() {
        return queryJson.getString(QueryParams.USERNAME.getParam());
    }

    public String getRequestId() {
        return queryJson.getString(QueryParams.REQUEST_ID.getParam());
    }

    public String getParentRequestId() {
        return queryJson.getString(QueryParams.PARENT_REQUEST_ID.getParam());
    }

    public boolean isSubRequest() {
        return queryJson.has(QueryParams.PARENT_REQUEST_ID.getParam());
    }

    public String getMasterNodeId() {
        return queryJson.getString(QueryParams.MASTER_NODE_ID.getParam());
    }

    public String getAck() {
        return queryJson.getString(QueryParams.ACK.getParam());
    }

    public Query fromNode(final String nodeId) {
        queryJson.put(QueryParams.FROM_NODE_ID.getParam(), nodeId);
        return this;
    }

    public String getFromNode() {
        return queryJson.getString(QueryParams.FROM_NODE_ID.getParam());
    }

    public boolean isAckSuccess() {
        if(!queryJson.has(QueryParams.ACK.getParam())) {
            return false;
        }

        return queryJson.get(QueryParams.ACK.getParam()).equals("1");
    }

    public Object getPayload() {
        return queryJson.get(QueryParams.PAYLOAD.getParam());
    }

    public long getTime() {
        return queryJson.getLong(QueryParams.TIME.getParam());
    }

    public String getDs() {
        return queryJson.getString(QueryParams.DATASTORE.getParam());
    }

    public String getCollection() {
        return queryJson.getString(QueryParams.COLLECTION.getParam());
    }

    public JSONObject toJson() {
        return queryJson;
    }

    public String toJsonString() {
        return queryJson.toString();
    }

    public Object get(final String param) {
        return queryJson.get(param);
    }

    public Object get(final QueryParams param) {
        return queryJson.get(param.getParam());
    }

    public String getString(final String param) {
        return queryJson.getString(param);
    }

    public String getString(final QueryParams param) {
        return queryJson.getString(param.getParam());
    }

    public JSONArray getJSONArray(final String param) {
        return queryJson.getJSONArray(param);
    }

    public JSONObject getJSONObject(final String param) {
        return queryJson.getJSONObject(param);
    }

    public JSONObject getJSONObject(final QueryParams param) {
        return queryJson.getJSONObject(param.getParam());
    }

    public void put(final QueryParams param, final Object value) {
        queryJson.put(param.getParam(), value);
    }

    public boolean contains(final QueryParams param) {
        return queryJson.has(param.getParam());
    }

    public Query errorCode(final String errorCode) {
        queryJson.put(QueryParams.ERROR_CODE.getParam(), errorCode);
        return this;
    }

    public String getErrorCode() {
        return queryJson.getString(QueryParams.ERROR_CODE.getParam());
    }

    public Query message(final String message) {
        queryJson.put(QueryParams.MESSAGE.getParam(), message);
        return this;
    }

    public String getMessage() {
        return queryJson.getString(QueryParams.MESSAGE.getParam());
    }

    /* Schema management queries */

    /**
     * Creates a query for creating a new datastore
     * @param ds name of new datastore
     * @return the modified query object
     */
    public Query createDs(final String ds) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.CREATE_DS.getQueryCode());
        queryJson.put(QueryParams.DATASTORE.getParam(), ds);
        return this;
    }

    /**
     * Creates a query for dropping a datastore
     * @param ds name of the datastore
     * @return the modified query object
     */
    public Query dropDs(final String ds) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.DROP_DS.getQueryCode());
        queryJson.put(QueryParams.DATASTORE.getParam(), ds);
        return this;
    }

    /**
     * Creates a query for creating a new collection
     * @param ds name of an existing datastore
     * @param collection name of new collection
     * @param collectionStorageType the {@link CollectionStorageType} of the collection
     * @return the modified query object
     */
    public Query createCollection(final String ds, final String collection, final CollectionStorageType collectionStorageType) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.CREATE_COLLECTION.getQueryCode());
        queryJson.put(QueryParams.DATASTORE.getParam(), ds);
        queryJson.put(QueryParams.COLLECTION.getParam(), collection);
        queryJson.put(QueryParams.STORAGE_TYPE.getParam(), collectionStorageType.getTypeCode());
        return this;
    }

    /* Node management queries  */

    public Query addNodeQuery(final String nodeId, final String ip) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.ADD_NODE.getQueryCode());
        queryJson.put(QueryParams.NODE_ID.getParam(), nodeId);
        queryJson.put(QueryParams.IP.getParam(), ip);
        return this;
    }

    public Query dropNodeQuery(final String nodeId) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.DROP_NODE.getQueryCode());
        queryJson.put(QueryParams.NODE_ID.getParam(), nodeId);
        return this;
    }

    public Query applyLicenseQuery(final String nodeId, final String licenseKey) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.APPLY_LICENSE.getQueryCode());

        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Query revokeLicenseQuery(final String nodeId) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.REVOKE_LICENSE.getQueryCode());

        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* Data queries */

    /**
     * Creates a query for inserting a collection of records with automatic type inference
     * @param ds name of datastore
     * @param collection name of collection
     * @param records a collection of records that are to be inserted
     * @return the generated query in JSON format
     */
    public Query insertQuery(final String ds, final String collection, List<Record> records) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.INSERT.getQueryCode());
        queryJson.put(QueryParams.DATASTORE.getParam(), ds);
        queryJson.put(QueryParams.COLLECTION.getParam(), collection);

        JSONObject payloadJson = new JSONObject();
        payloadJson.put(QueryParams.DATA.getParam(), new JSONArray(records));
        payloadJson.put(QueryParams.TYPE.getParam(), RecordType.AUTO);

        queryJson.put(QueryParams.PAYLOAD.getParam(), payloadJson);
        return this;
    }

    /**
     * Creates a query for inserting a collection of records of the specified type
     * @param ds name of datastore
     * @param collection name of collection
     * @param records a collection of records that are to be inserted
     * @param recordType the @link {@link RecordType} indicating the format engine to use for data
     * @return the generated query in JSON format
     */
    public Query insertQuery(final String ds, final String collection, List<Record> records, final RecordType recordType) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.INSERT.getQueryCode());
        queryJson.put(QueryParams.DATASTORE.getParam(), ds);
        queryJson.put(QueryParams.COLLECTION.getParam(), collection);

        JSONObject payloadJson = new JSONObject();
        payloadJson.put(QueryParams.TYPE.getParam(), recordType.getTypeCode());
        payloadJson.put(QueryParams.DATA.getParam(), new JSONArray(Collections2.transform(records, new Function<Record, JSONObject>() {
            public JSONObject apply(Record record) {
                return record.asJson();
            }
        })));

        queryJson.put(QueryParams.PAYLOAD.getParam(), payloadJson);
        return this;
    }

    /**
     * Creates a query for inserting a collection of records of the specified type
     * @param ds name of datastore
     * @param collection name of collection
     * @param records a collection of records that are to be inserted
     * @param recordType the @link {@link RecordType} indicating the format engine to use for data
     * @return the generated query in JSON format
     */
    public Query insertQueryUninferred(final String ds, final String collection, List<Object> records, final RecordType recordType) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.INSERT.getQueryCode());
        queryJson.put(QueryParams.DATASTORE.getParam(), ds);
        queryJson.put(QueryParams.COLLECTION.getParam(), collection);

        JSONObject payloadJson = new JSONObject();
        payloadJson.put(QueryParams.TYPE.getParam(), recordType.getTypeCode());
        payloadJson.put(QueryParams.DATA.getParam(), new JSONArray(records));

        queryJson.put(QueryParams.PAYLOAD.getParam(), payloadJson);
        return this;
    }

    /**
     * Creates an SQL query
     * <code>
     *     {
     *         "q":"SQL",
     *         "ds":"ds-name",
     *         "sql":"sql"
     *     }
     * </code>
     * @param ds name of datastore
     * @param sql the sql query to run
     * @return the populated sql query
     */
    public Query sql(final String ds, final String sql) {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.SQL.getQueryCode());
        queryJson.put(QueryParams.DATASTORE.getParam(), ds);
        queryJson.put(QueryParams.SQL.getParam(), sql);
        return this;
    }

    public Query listDsQuery() {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.LIST_DS.getQueryCode());
        return this;
    }

    public Query listCollectionsQuery() {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.LIST_COLLECTIONS.getQueryCode());
        return this;
    }

    public Query listCollections(final String ds) {
        queryJson.put(QueryParams.DATASTORE.getParam(), ds);
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.LIST_COLLECTIONS.getQueryCode());
        return this;
    }

    /* Internal queries */

    public Query softCommitSuccessQuery() {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.SOFT_COMMIT_SUCCESS.getQueryCode());
        return this;
    }

    public Query commitQuery() {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.COMMIT.getQueryCode());
        return this;
    }

    public Query commitSuccessQuery() {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.COMMIT_SUCCESS.getQueryCode());
        return this;
    }

    public Query rollbackQuery() {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.ROLLBACK.getQueryCode());
        return this;
    }

    public Query rollbackSuccessQuery() {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.ROLLBACK_SUCCESS.getQueryCode());
        return this;
    }

    public Query responseQuery() {
        queryJson.put(QueryParams.QUERY.getParam(), QueryType.QUERY_RESPONSE.getQueryCode());
        return this;
    }
}
