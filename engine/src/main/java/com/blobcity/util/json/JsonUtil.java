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

package com.blobcity.util.json;

import com.google.common.base.Preconditions;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author akshaydewan
 * @author sanketsarang
 */
public class JsonUtil {

    public static JSONObject finalJson;
    
    public static boolean isAck(final String json) {
        try {
            JSONObject jsonObject = new JSONObject(json);
            String ack = jsonObject.optString("ack");
            return ack.equals("1");
        } catch (JSONException e) {
            return false;
        }
    }
    
    public static boolean isAck(final JSONObject jsonObject) {
        return "1".equals(jsonObject.optString("ack"));
    }

    /**
     * Checks if a homogeneous JSONArray contains the specified element
     * @param <T> template type for the elements in the JSONArray.
     * @param jsonArray the json array on which the contains check is to be done
     * @param element the element to search in the array
     * @return <code>true</code> if the element is found; <code>false</code> otherwise
     */
    public static <T> boolean contains(final JSONArray jsonArray, T element) {
        Preconditions.checkNotNull(jsonArray);
        Preconditions.checkNotNull(element);

        for (int i = 0; i < jsonArray.length(); i++) {
            T t = (T) jsonArray.get(i);
            if (element.equals(t)) {
                return true;
            }
        }

        return false;
    }
    
    public static JSONObject getHierarchicalJson(final Object originalJson, String keyPrefix) {
        finalJson = new JSONObject();
        readKeys((JSONObject)originalJson, keyPrefix);
        return finalJson;
    }

    public static JSONObject getHierarchicalJsonFromArray(final Object originalJsonArray, String keyPrefix) {
        finalJson = new JSONObject();
        readKeysFromArray((JSONArray)originalJsonArray, keyPrefix);
        return finalJson;
    }
    
    public static void readKeysFromArray(JSONArray jsonArray, String keyPrefix) {
        for (int i = 0; i < jsonArray.length(); i++) {
            Object jsonObj = jsonArray.get(i);
            if (jsonObj instanceof JSONObject) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                for (Object key : jsonObject.keySet()) {
                    Object value = jsonObject.get(key.toString());
                    if (value instanceof JSONObject) {
                        readKeysAndAppend(jsonObject.getJSONObject(key.toString()), keyPrefix + key.toString() + ".");
                    } else {
                        finalJson.append(keyPrefix + key.toString(), value);
                    }
                }
            } else if (jsonObj instanceof JSONArray) {
                readKeysFromArray((JSONArray) jsonObj, keyPrefix);
            }
            else {
                finalJson.append(keyPrefix, jsonObj);
            }
        }
    }
    
    public static void readKeys(JSONObject jsonObject, String keyPrefix) {
        for (Object key : jsonObject.keySet()) {
            Object value = jsonObject.get(key.toString());

            if (value instanceof JSONObject) {
                readKeys(jsonObject.getJSONObject(key.toString()), keyPrefix + key + ".");
            } else if (value instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) value;
                if (jsonArray.length() > 0) {
                    Object jsonObj = jsonArray.get(0);
                    if ((jsonObj instanceof JSONObject) || (jsonObj instanceof JSONArray)) {
                        readKeysFromArray(jsonObject.getJSONArray(key.toString()), keyPrefix + key + ".");
                    } else {
                        readKeysFromArray(jsonObject.getJSONArray(key.toString()), keyPrefix + key);
                    }
                }
            } else {
                finalJson.put(keyPrefix + key, value);
            }
        }
    }
    
    public static void readKeysAndAppend(JSONObject jsonObject, String keyPrefix) {
        for (Object key : jsonObject.keySet()) {
            Object value = jsonObject.get(key.toString());

            if (value instanceof JSONObject) {
                readKeys(jsonObject.getJSONObject(key.toString()), keyPrefix + key + ".");
            }
            else if(value instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray)value;
                readKeysFromArray(jsonObject.getJSONArray(key.toString()), keyPrefix + key + ".");
            }
            else {
                finalJson.append(keyPrefix + key, value);
            }
        }
    }
}
