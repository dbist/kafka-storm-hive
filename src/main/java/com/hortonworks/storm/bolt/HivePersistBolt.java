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
package com.hortonworks.storm.bolt;

import backtype.storm.Config;
import storm.starter.utils.TupleUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.hortonworks.hive.HiveDump;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

/**
 *
 * @author artem
 */
public class HivePersistBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(HivePersistBolt.class.getName());

    private static final long serialVersionUID = 1L;
    private final String database;
    private final String urlString;
    private HiveDump hiveDump = null;
    private final String tableName;
    private static final int emitFrequencyInSeconds = 30;
    private static final CopyOnWriteArrayList<Tuple> tuples = new CopyOnWriteArrayList<>();
    private OutputCollector outputCollector;

    public HivePersistBolt(String urlString, String database, String tableName) {
        this.database = database;
        this.urlString = urlString;
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        hiveDump = new HiveDump(urlString, database, tableName);
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        if(TupleUtils.isTick(tuple)) {
            LOG.info("received Tick, printing counts");
            hiveDump.persistRecord(tuples);
            tuples.clear();
        } else {
            tuples.add(tuple);
            outputCollector.ack(tuple);
        }
        
        String json = tuple.getStringByField("json");
        outputCollector.emit(new Values(json));
        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {
        hiveDump.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
    
}
