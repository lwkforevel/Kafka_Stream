package kafka_stream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	final Serde<String> stringSerde = Serdes.String();
    	final Serde<Long> longSerde = Serdes.Long();
    	 
    	// Construct a `KStream` from the input topic "streams-plaintext-input", where message values
    	// represent lines of text (for the sake of this example, we ignore whatever may be stored
    	// in the message keys).
    	Properties props = new Properties();
    	props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
    	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");   
    	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    	props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    	final StreamsBuilder builder = new StreamsBuilder();
    	
    	KStream<String, String> source = builder.stream(
    	      "streams-plaintext-input",
    	      Consumed.with(stringSerde, stringSerde)
    	    );
    	
    	KTable<String, Long> wordCounts = source
    	    // Split each text line, by whitespace, into words.
    	    .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
    	 
    	    // Group the text words as message keys
    	    .groupBy((key, value) -> value)
    	 
    	    // Count the occurrences of each word (message key).
    	    .count();
    	 
    	// Store the running counts as a changelog stream to the output topic.
    	KStream<String, Long> stream = wordCounts.toStream();
    	stream.to("streams-wordcount-output",Produced.with(stringSerde, longSerde));
    	
    	Topology topology = builder.build();
  //  	source.to("streams-pipe-output");
    	
    	final CountDownLatch latch = new CountDownLatch(1);
    	 
    	final KafkaStreams streams = new KafkaStreams(topology, props);
    	// attach shutdown handler to catch control-c
    	Runtime runtime = Runtime.getRuntime();
    	runtime.addShutdownHook(new Thread("streams-shutdown-hook") {
    	    @Override
    	    public void run() {
    	        streams.close();
    	        latch.countDown();
    	    }
    	});
    	try {
    	    streams.start();
    	    latch.await();
    	} catch (Throwable e) {
    	    System.exit(1);
    	}
    	streams.start();
    	System.exit(0);    	
    	System.out.println(topology.describe());
      }
}
