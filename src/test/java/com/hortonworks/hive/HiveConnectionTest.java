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
package com.hortonworks.hive;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author aervits
 */
public class HiveConnectionTest {
    
    public HiveConnectionTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of getHiveConnection method, of class HiveConnection.
     */

    @Test
    public void testGetHiveConnection() {
        System.out.println("getHiveConnection");
        String urlString = "thrift://sandbox.hortonworks.com:9083";
        String dbName = "default";
        String tblName = "testConnection";
        ArrayList<String>partitionVals = new ArrayList();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                
        partitionVals.add(dateFormat.format(new Date()));
        
        HiveDump hiveDump = new HiveDump(urlString, dbName, tblName);
        
    }
    
}
