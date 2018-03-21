package com.asiainfo.steven.jstorm.quickstart;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout implements IRichSpout {

    private static final String XXX = "ABCDEFGHIJKLMN";
    private SpoutOutputCollector collector;
    private Random random;

    private String randomSentence(int len) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < len; i++) {
            sb.append(XXX.charAt(random.nextInt(len)));
        }

        return sb.toString();

    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        this.collector.emit(new Values(randomSentence(8)));
        Utils.sleep(1000);
    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
