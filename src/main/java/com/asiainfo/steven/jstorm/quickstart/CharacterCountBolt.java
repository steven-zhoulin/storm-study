package com.asiainfo.steven.jstorm.quickstart;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.HashMap;

public class CharacterCountBolt extends BaseRichBolt {

    private OutputCollector collector;

    // 存储单词和对应的计数
    private Map<String, Long> counts = null; // 注：不可序列化对象需在prepare中实例化

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {

        String c = tuple.getStringByField("char");

        Long count = this.counts.get(c);

        if (count == null) {
            count = 0L;
        }

        count++;
        this.counts.put(c, count);
        this.collector.emit(new Values(c, count));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("char", "count"));
    }

}