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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hive.hcatalog.streaming.ConnectionError;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.ImpersonationFailed;
import org.apache.hive.hcatalog.streaming.InvalidPartition;
import org.apache.hive.hcatalog.streaming.InvalidTable;
import org.apache.hive.hcatalog.streaming.PartitionCreationFailed;
import org.apache.hive.hcatalog.streaming.StreamingConnection;

/**
 *
 * @author aervits
 */

//  Assuming the table below exists
//  drop table if exists testconnection;
//  create table testconnection (id int, json string) 
//	partitioned by (dt timestamp)
//	clustered by (id) into 5 buckets 
//	stored as orc;

public class HiveConnection {

    private static StreamingConnection connection = null;
    private static final Logger LOG = Logger.getLogger(HiveConnection.class.getName());

    public static StreamingConnection getHiveConnection(HiveEndPoint hiveEP) {
        
        try {
            
            connection = hiveEP.newConnection(true);
               return connection;
        } catch (ConnectionError | InvalidPartition | InvalidTable | PartitionCreationFailed | ImpersonationFailed | InterruptedException ex) {
            LOG.log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage());
        }    
    }
}
