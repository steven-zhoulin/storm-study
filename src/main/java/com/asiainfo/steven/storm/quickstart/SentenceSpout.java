package com.asiainfo.steven.storm.quickstart;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * BaseRichSpout是ISpout接口和IComponent接口的简单实现
 */
public class SentenceSpout extends BaseRichSpout {

    private int index = 0;
    private SpoutOutputCollector collector;

    private String[] sentences = {
        "my name is soul",
        "im a boy",
        "i have a dog",
        "my dog has fleas",
        "my girl friend is beautiful"
    };

    /**
     * 在Spout组件初始化时被调用。
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * nextTuple()方法是任何Spout实现的核心。
     * Storm调用这个方法，向输出的collector发出tuple。
     * 在这里,我们只是发出当前索引的句子，并增加该索引准备发射下一个句子。
     */
    public void nextTuple() {

        this.collector.emit(new Values(sentences[index++]));

        if (index >= sentences.length) {
            index = 0;
        }

        Utils.sleep(1000);

    }

    /**
     * 所有Storm的组件(spout和bolt)都必须实现这个接口
     * 用于告诉Storm流组件将会发出那些数据流，每个流的tuple将包含的字段
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 告诉组件发出数据流包含sentence字段
        declarer.declare(new Fields("sentence"));
    }

}