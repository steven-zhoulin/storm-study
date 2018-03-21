package com.asiainfo.steven.jstorm.quickstart;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class App {

    private static final String RANDOM_SENTENCE_SPOUT_ID = "ramdom-sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String CHARACTER_COUNT_BOLT_ID = "character-count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "character-count-topology";

    private static void localModeRunning(Config config, TopologyBuilder builder) {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.sleep(60000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

    private static void productModeRunning(Config config, TopologyBuilder builder) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        // 实例化spout和bolt
        RandomSentenceSpout spout = new RandomSentenceSpout();
        SplitBolt splitBolt = new SplitBolt();
        CharacterCountBolt countBolt = new CharacterCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(RANDOM_SENTENCE_SPOUT_ID, spout, 2);
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4).shuffleGrouping(RANDOM_SENTENCE_SPOUT_ID);
        builder.setBolt(CHARACTER_COUNT_BOLT_ID, countBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("char"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(CHARACTER_COUNT_BOLT_ID);


        Config config = new Config();
        config.setDebug(false);
        // config.setNumWorkers(4);
        productModeRunning(config, builder);
    }

}
