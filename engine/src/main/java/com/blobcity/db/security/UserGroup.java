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

package com.blobcity.db.security;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.json.JSONObject;

/**
 * This class stores information about user group
 * 
 * @author sanketsarang
 */
public class UserGroup {
    private String name;
    private String owner;
    // dont change it, since the name of the column in database is stored in this way only
    private String createdat;
    private String users;

    public UserGroup(String name, String owner) {
        this.name = name;
        this.owner = owner;
        this.createdat = new Date().toString();
        this.users = "";
    }
    
    public UserGroup(String name){
        this.name = name;
        this.owner = "root";
        this.createdat = new Date().toString();
        this.users = "";
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getCreatedAt() {
        return createdat;
    }

    public void setCreatedAt(String createdAt) {
        this.createdat = createdAt;
    }

    public String getUsers() {
        return users;
    }

    public void setUsers(String users) {
        this.users = users;
    }
    
    @Override
    public String toString(){
        return new Gson().toJson(this);
        //return "UserGroup{" + "name=" + name + ",owner=" + owner + ",createdat=" + createdat + ",users=" + users + "}";
    }
    
    public JSONObject toJSON(){
        JSONObject json = new JSONObject();
        json.put("name", name);
        json.put("owner", owner);
        json.put("createdat", createdat);
        json.put("users", users);
        return json;
    }
    
    public List<String> getUsersasList(){
        List<String> allusers = new ArrayList<>();
        for(String user: this.users.split(",")){
            try{
                if(!user.isEmpty()) allusers.add(user);
            } catch(NullPointerException ex){
                // don't add user to the list
            }
        }
        return allusers;
    }
            
}
