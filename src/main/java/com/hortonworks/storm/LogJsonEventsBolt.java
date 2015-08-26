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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 *
 * @author aervits
 */
public class LogJsonEventsBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(LogJsonEventsBolt.class.getName());

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {

    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {

    }

    @Override
    public void execute(Tuple tuple) {
        StringBuilder sb = new StringBuilder();
        sb.append(tuple.getStringByField(JsonScheme.JSON_ID))
                .append(",")
                .append(tuple.getStringByField(JsonScheme.FIRST_NAME))
                .append(",")
                .append(tuple.getValueByField(JsonScheme.LAST_NAME))
                .append(",")
                .append(tuple.getStringByField(JsonScheme.EMAIL))
                .append(",")
                .append(tuple.getStringByField(JsonScheme.ADDRESS))
                .append(",")
                .append(tuple.getStringByField(JsonScheme.CITY))
                .append(",")
                .append(tuple.getStringByField(JsonScheme.STATE))
                .append(",")
                .append(tuple.getStringByField(JsonScheme.ZIP))
                .append(",")
                .append(tuple.getStringByField(JsonScheme.TEXT))
                .append(",")
                .append(tuple.getStringByField(JsonScheme.URL));
        LOG.info(sb.toString());
               
    }

}
