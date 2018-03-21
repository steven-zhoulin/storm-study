package com.asiainfo.steven.storm.quickstart;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 实现单词计数topology
 */
public class App {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    private static void localModeRunning(Config config, TopologyBuilder builder) {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

    private static void productModeRunning(Config config, TopologyBuilder builder) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        // 实例化spout和bolt
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        // TopologyBuilder提供流式风格的API来定义topology组件之间的数据流
        TopologyBuilder builder = new TopologyBuilder();

        // 设置两个Executeor(线程)，默认一个
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);

        // SentenceSpout -> SplitSentenceBolt
        // shuffleGrouping方法告诉Storm要将SentenceSpout发射的tuple随机均匀的分发给SplitSentenceBolt的实例
        // SplitSentenceBolt单词分割器设置4个Task，2个Executeor(线程)
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);

        // SplitSentenceBolt -> WordCountBolt
        // fieldsGrouping将含有特定数据的tuple路由到特殊的bolt实例中
        // 这里fieldsGrouping()方法保证所有“word”字段相同的tuple会被路由到同一个WordCountBolt实例中

        // WordCountBolt单词计数器设置4个Executeor(线程)
        builder.setBolt(COUNT_BOLT_ID, countBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));

        // WordCountBolt -> ReportBolt
        // globalGrouping是把WordCountBolt发射的所有tuple路由到唯一的ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        // config.setNumWorkers(2); // 设置worker数量
        config.setDebug(false);

        // localModeRunning(config, builder);
        productModeRunning(config, builder);

    }
}