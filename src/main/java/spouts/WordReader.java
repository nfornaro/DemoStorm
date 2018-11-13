package spouts;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;

import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordReader implements IRichSpout {
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;

    public boolean isDistributed() {
        return false;
    }

    public void ack(Object msgId) {
        System.out.println("OK:"+msgId);
    }

    public void close() {}

    public void fail(Object msgId) {
        System.out.println("FAIL:"+msgId);
    }

    public void nextTuple() {
        if(completed){
            try {
                Thread.sleep(1000);
                return;
            } catch (Exception ex) {}
        }

        String str;
        BufferedReader reader = new BufferedReader(fileReader);

        try{
            while((str = reader.readLine()) != null){
                this.collector.emit(new Values(str), str);
            }
        } catch(Exception e){
            throw new RuntimeException("Error reading tuple",e); }finally{
            completed = true;
        }
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.context = context;
            this.fileReader = new FileReader(conf.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
        }

        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public void deactivate(){}

    public void activate(){}

    public Map<String,Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}
