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

import backtype.storm.tuple.Tuple;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.SerializationError;
import org.apache.hive.hcatalog.streaming.StrictJsonWriter;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

/**
 *
 * @author aervits
 */

public class HiveDump {

//  drop table if exists jsonstreaming;
//  create table jsonstreaming (id string, json string) 
//	partitioned by (dt string) 
//	clustered by (id) into 5 buckets 
//	stored as orc;
    
//  drop table if exists jsonstreaming;
//  create table jsonstreaming (
//  id string, 
//  person struct<email:string, first_name:string, last_name:string, `location`:struct<address:string, city:string, state:string, zipcode:string>, text:string, url:string>) 
//      partitioned by (state string) 
//      clustered by (id) into 5 buckets 
//      stored as orc;
    
    private static final Logger LOG = Logger.getLogger(HiveDump.class.getName());

    //private static final String serdeClass = "org.apache.hive.hcatalog.data.JsonSerDe";
    private final String database;
    private final String table;
    private final String urlString;
    private final ArrayList<String> partitionVals = new ArrayList<>();
    private final HiveEndPoint hiveEP;
    private final StreamingConnection connection;
    private final String [] fieldNames;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm");
    
    public HiveDump(String urlString, String database, String table) {
        this.fieldNames = new String []{"json"};
        this.urlString = urlString;
        this.database = database;
        this.table = table;
        
        // CHANGE THIS TO U.S. STATE
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        partitionVals.add(dateFormat.format(new Date()));

        this.hiveEP = new HiveEndPoint(urlString, database, table, partitionVals);
        this.connection = HiveConnection.getHiveConnection(hiveEP);
    }

    public void persistRecord(List<Tuple> tuples) {
        TransactionBatch txnBatch = null;
        
        for(Tuple tuple : tuples) {
            try {
                StrictJsonWriter writer = new StrictJsonWriter(hiveEP);
                txnBatch = connection.fetchTransactionBatch(tuples.size(), writer);
                txnBatch.beginNextTransaction();
    
                byte [] record = tuple.getStringByField("json").getBytes();
                txnBatch.write(record);
                
            } catch (SerializationError ex) {
                LOG.log(Level.SEVERE, null, ex);
            } catch (StreamingException | InterruptedException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }    
            try {
                txnBatch.commit();
            } catch (StreamingException | InterruptedException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }    
        }    
    }
    
    public void close() {
        try {
            connection.close();
        } catch (Exception ex) {
            LOG.log(Level.WARNING, "error closing connection to Hive");
        }
    }
}

