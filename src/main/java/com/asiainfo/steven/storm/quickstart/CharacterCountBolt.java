package com.asiainfo.steven.storm.quickstart;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 订阅 SplitSentenceBolt的输出流，实现单词计数，并发送当前计数给下一个bolt
 */
public class CharacterCountBolt extends BaseRichBolt {

    private OutputCollector collector;

    // 存储子母和对应的计数
    private HashMap<String, Long> counts = null; // 注：不可序列化对象需在prepare中实例化

    /**
     * 大部分实例变量通常是在prepare()中进行实例化，这个设计模式是由topology的部署方式决定的
     * 因为在部署拓扑时,组件spout和bolt是在网络上发送的序列化的实例变量。
     * 如果spout或bolt有任何non-serializable实例变量在序列化之前被实例化(例如,在构造函数中创建)
     * 会抛出NotSerializableException并且拓扑将无法发布。
     * 本例中因为HashMap 是可序列化的,所以可以安全地在构造函数中实例化。
     * 但是，通常情况下最好是在构造函数中对基本数据类型和可序列化的对象进行复制和实例化
     * 而在prepare()方法中对不可序列化的对象进行实例化。
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    /**
     * 在execute()方法中,我们查找的收到的单词的计数(如果不存在，初始化为0)
     * 然后增加计数并存储,发出一个新的词和当前计数组成的二元组。
     * 发射计数作为流允许拓扑的其他bolt订阅和执行额外的处理。
     */
    public void execute(Tuple tuple) {

        String ch = tuple.getStringByField("char");
        Long count = this.counts.get(ch);

        if (count == null) {
            count = 0L;
        }

        count++;
        this.counts.put(ch, count);
        this.collector.emit(new Values(ch, count));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        // 声明一个输出流，其中tuple包括了单词和对应的计数，向后发射其他bolt可以订阅这个数据流进一步处理
        declarer.declare(new Fields("char", "count"));

    }

}