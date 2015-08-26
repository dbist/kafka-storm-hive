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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.hortonworks.storm.bolt.HivePersistBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.starter.bolt.PrinterBolt;

/**
 *
 * @author aervits
 */
public class JsonEventProcessingTopology {

    private static final String KAFKA_SPOUT_ID = "kafkaSpout";
    private static final String LOG_JSON_BOLT_ID = "logJsonEventBolt";
    private static final String HIVE_PERSIST_BOLT_ID = "hdfsBolt";
    private static final String PRINTER_BOLT_ID = "printerBolt";

//    public JsonEventProcessingTopology(String configFileLocation) throws Exception 
//    {
//        super(configFileLocation);
//    }
    private SpoutConfig constructKafkaSpoutConf() {

        BrokerHosts hosts = new ZkHosts("sandbox.hortonworks.com:2181");
        String topic = "json";
        String zkRoot = "/json_event_spout";
        String consumerGroupId = "StormSpout";

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

        spoutConfig.scheme = new SchemeAsMultiScheme(new JsonScheme());

        return spoutConfig;
    }

    public void configureKafkaSpout(TopologyBuilder builder) {
        KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
        int spoutCount = 1;
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
    }

    public void configureLogJsonEventBolt(TopologyBuilder builder) {
        LogJsonEventsBolt logBolt = new LogJsonEventsBolt();
        builder.setBolt(LOG_JSON_BOLT_ID, logBolt).globalGrouping(KAFKA_SPOUT_ID);
    }
    
    public void configurePrinterBolt(TopologyBuilder builder) {
        PrinterBolt printerBolt = new PrinterBolt();
        builder.setBolt(PRINTER_BOLT_ID, printerBolt).globalGrouping(KAFKA_SPOUT_ID);
    }
        public void configureHivePersistBolt(TopologyBuilder builder) {
        HivePersistBolt hivePersistBolt = new HivePersistBolt("thrift://sandbox.hortonworks.com:9083", "default", "jsonstreaming");
        builder.setBolt(HIVE_PERSIST_BOLT_ID, hivePersistBolt).globalGrouping(KAFKA_SPOUT_ID);
    }

    private void buildAndSubmit(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        configureKafkaSpout(builder);
        configurePrinterBolt(builder);
//        configureLogJsonEventBolt(builder);
//        configureHivePersistBolt(builder);

        Config config = new Config();
        config.setDebug(true);

//        if (args != null && args.length > 0) {
//            config.setMaxTaskParallelism(3);
//
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("kafka", config, builder.createTopology());
//
//            Thread.sleep(20000);
//
//            cluster.shutdown();
//        } else {
        config.setNumWorkers(3);

        StormSubmitter.submitTopology("json-event-processor",
                config, builder.createTopology());
//        }
    }

    public static void main(String[] args) throws Exception {
        String configFileLocation = "json_event_topology.properties";
        JsonEventProcessingTopology jsonTopology
                = new JsonEventProcessingTopology();

        jsonTopology.buildAndSubmit(args);
    }

}
