/*
 * Copyright 2015 aervits.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.storm;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author aervits
 */
public class JsonScheme implements Scheme {
    
    private static final Logger LOG = Logger.getLogger(JsonScheme.class.getName());

    public static final String JSON_ID = "id";
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    public static final String EMAIL = "email";
    public static final String ADDRESS = "address";
    public static final String CITY = "city";
    public static final String STATE = "state";
    public static final String ZIP = "zipcode";
    public static final String TEXT = "text";
    public static final String URL = "url";
    
    private static final long servialVersionUID = -2990121166902741545L;

    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String jsonEvent = new String(bytes, "UTF-8");
            System.out.println(jsonEvent);
            JSONObject jobj = new JSONObject(jsonEvent);
            String id = jobj.getString("id");
            JSONObject person = jobj.getJSONObject("person");
            String firstName = person.getString("first_name");
            String lastName = person.getString("last_name");
            String email = person.getString("email");
            JSONObject location = person.getJSONObject("location");
            String address = location.getString("address");
            String city = location.getString("city");
            String state = location.getString("state");
            String zip = location.getString("zipcode");

            String text = person.getString("text");
            String url = person.getString("url");

            return new Values(cleanup(id), cleanup(firstName),
                    cleanup(lastName), cleanup(email), cleanup(address), cleanup(city), cleanup(state), cleanup(zip), cleanup(text), cleanup(url));
        } catch (UnsupportedEncodingException | JSONException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(JSON_ID,
                FIRST_NAME,
                LAST_NAME,
                EMAIL,
                ADDRESS,
                CITY,
                STATE,
                ZIP,
                TEXT,
                URL);
    }

    private String cleanup(String str) {
        if (str != null) {
            return str.trim().replace("\n", "").replace("\t", "");
        } else {
            return str;
        }
    }
}
