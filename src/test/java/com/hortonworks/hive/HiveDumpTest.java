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

import backtype.storm.Testing;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Date;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

/**
 *
 * @author aervits
 */
public class HiveDumpTest {
    private final String urlString = "thrift://sandbox.hortonworks.com:9083";
    private final String dbName = "default";
    private final String tblName = "jsonstreaming";
    private final ArrayList<String> partitionVals = new ArrayList<>();
    private HiveDump instance;
    
    public HiveDumpTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        instance = new HiveDump(urlString, dbName, tblName);
        partitionVals.add(new Date().toString());
    }
    
    @After
    public void tearDown() {
        partitionVals.clear();
        instance.close();
    }

    /**
     * Test of persistRecord method, of class HiveDump.
     */
    @Test
    public void testPersistRecord() {
        System.out.println("persistRecord");
        
        String json = "{\"id\":\"51d24c1f-e9eb-41b2-ae7a-5e9168f68a5f\",\"person\":{\"first_name\":\"Anthony\",\"last_name\":\"Tucker\",\"email\":\"atucker0@stanford.edu\",\"location\":{\"address\":\"5153 8th Park\",\"city\":\"Buffalo\",\"state\":\"New York\",\"zipcode\":\"14215\"},\"text\":\"sem praesent id massa id nisl venenatis\",\"url\":\"https://timesonline.co.uk/ultrices/phasellus/id/sapien.xml?orci=non\\u0026nullam=mattis\\u0026molestie=pulvinar\\u0026nibh=nulla\\u0026in=pede\\u0026lectus=ullamcorper\\u0026pellentesque=augue\\u0026at=a\\u0026nulla=suscipit\\u0026suspendisse=nulla\\u0026potenti=elit\\u0026cras=ac\\u0026in=nulla\\u0026purus=sed\\u0026eu=vel\\u0026magna=enim\\u0026vulputate=sit\\u0026luctus=amet\\u0026cum=nunc\\u0026sociis=viverra\"}}";
        MkTupleParam param = new MkTupleParam();
        param.setStream("test-stream");
        param.setComponent("test-component");
        param.setFields("json");
        Tuple tuple1 = Testing.testTuple(new Values(json), param);
        Testing.multiseteq(new Values(json), tuple1.getValues());
        Tuple tuple2 = Testing.testTuple(new Values(json), param);
        Testing.multiseteq(new Values(json), tuple2.getValues());
        ArrayList<Tuple> tuples = new ArrayList<>();
        
        tuples.add(tuple1);
        tuples.add(tuple2);
        
        instance.persistRecord(tuples);
    }

    /**
     * Test of close method, of class HiveDump.
     */
    @Test
    public void testClose() {
        System.out.println("close");
        
        instance = new HiveDump(urlString, dbName, tblName);
        instance.close();
    }
    
}
