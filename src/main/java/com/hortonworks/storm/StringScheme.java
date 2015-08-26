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

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 *
 * @author aervits
 */
public class StringScheme implements Scheme {

        public static final String JSON  = "json";
        
	private static final long serialVersionUID = -2990121166902741545L;

	private static final Logger LOG = Logger.getLogger(StringScheme.class);
	
	@Override
	public List<Object> deserialize(byte[] bytes)
        {
		try 
                {
        		String json = new String(bytes, "UTF-8");
			return new Values(cleanup(json));
		} 
                catch (UnsupportedEncodingException e) 
                {
                    LOG.error(e);
                    throw new RuntimeException(e);
		}
		
	}
        
	@Override
	public Fields getOutputFields()
        {
            return new Fields(JSON);
		
	}
        
        private String cleanup(String str)
        {
            if (str != null)
            {
                return str.trim().replace("\n", "").replace("\t", "");
            } 
            else
            {
                return str;
            }
            
        }
}


