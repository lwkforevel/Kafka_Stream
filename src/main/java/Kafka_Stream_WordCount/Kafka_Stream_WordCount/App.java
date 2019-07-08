package Kafka_Stream_WordCount.Kafka_Stream_WordCount;

import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
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
    	
    	
        final StreamsBuilder builder = new StreamsBuilder();
    	
    	KStream<String, String> textLines = builder.stream(
    	      "streams-plaintext-input",
    	      Consumed.with(stringSerde, stringSerde)
    	    );
    	 
    	KTable<String, Long> wordCounts = textLines
    	    // Split each text line, by whitespace, into words.
    	    .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
    	 
    	    // Group the text words as message keys
    	    .groupBy((key, value) -> value)
    	 
    	    // Count the occurrences of each word (message key).
    	    .count();
    	 
    	// Store the running counts as a changelog stream to the output topic.
    	wordCounts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

      }    
}
