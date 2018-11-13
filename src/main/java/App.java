import spouts.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;

public class App {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader());

        builder.setBolt("word-normalizer", new WordNormalizer())
            .shuffleGrouping("word-reader");

        builder.setBolt("word-counter", new WordCounter(), 5)
            .shuffleGrouping("word-normalizer");
            // .fieldsGrouping("word-normalizer", new Fields("word"));

        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.put(Config.TOPOLOGY_DEBUG, false);
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf,builder.createTopology());

        try {
            Thread.sleep(5000);
        } catch (Exception ex) {}

        cluster.shutdown();
    }
}
