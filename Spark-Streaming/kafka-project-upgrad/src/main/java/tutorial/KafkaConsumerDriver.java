package tutorial;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.spark.streaming.Durations;

import scala.Tuple2;

public class KafkaConsumerDriver {

	public static void main(String[] args) {

		//Persisting the input arguments into the variable
		String brokers = args[0]; //"34.206.133.62:9092";
		String groupId = args[1]; //"test2";
		String topics = args[2]; //"stockData";

		//Set a unique name for the application
		SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkProject"); //.setMaster("local[*]");
		//Create context with a 5 seconds batch interval. Defining a micro batch
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.minutes(1));
		jssc.sparkContext().setLogLevel("WARN");

		//Split the topic if multiple values are passed
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

		//Define a new Hashmap for holding the Kafka information
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer .class);

		// Create direct kafka stream with brokers and topics/
		// LocationStrategy with prefer consistent allows partitions to be distributed consistently to the spark executors.
		// CosumerStrategy allows to subscribe to the kafka topics.
		// JavaInputDStream is a continuous input stream associated to the source.
		JavaInputDStream<ConsumerRecord<String, JsonNode>> messages = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

		// JavaDStream is an internal stream object for processed data.
		//Sliding window 5min && window 10min
		// Mapping the incoming JSON from kafka stream to the object.
		JavaDStream<Data> lines = messages.map(new Function<ConsumerRecord<String, JsonNode>, Data>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			// overriding the default call method
			@Override
			public Data call(ConsumerRecord<String, JsonNode> record) throws JsonProcessingException {
				ObjectMapper objectMapper = new ObjectMapper();

				return objectMapper.treeToValue(record.value(), Data.class);
			}
		});

		// Iterating over the processed object.
		// RDD is created to store the records
		JavaPairDStream<String, Tuple2<String, Integer>> pd = lines.filter(line -> (line.getSymbol().equalsIgnoreCase("BTC") || 
				line.getSymbol().equalsIgnoreCase("ETH") ||
				line.getSymbol().equalsIgnoreCase("LTC") ||
				line.getSymbol().equalsIgnoreCase("XRP")))
				.mapToPair(line -> new Tuple2<>(line.getSymbol(), 
						line.getPriceData().getClose() 
						+ "#" + line.getPriceData().getOpen() 
						+ "#" + java.lang.Math.abs(line.getPriceData().getVolume())))
				.mapValues(value -> new Tuple2<>(value,1)) 
				.reduceByKeyAndWindow(new Function2<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>> () {
					/**
					 *
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String,Integer> call(Tuple2<String,Integer> t1, Tuple2<String,Integer> t2) {
						String[] tup1 = t1._1.split("#");
						String[] tup2 = t2._1.split("#");
						float closing = Float.parseFloat(tup1[0]) + Float.parseFloat(tup2[0]);
						float opening = Float.parseFloat(tup1[1]) + Float.parseFloat(tup2[1]);
						float volume = Float.parseFloat(tup1[2]) + Float.parseFloat(tup2[2]);
						int count = t1._2 + t2._2;
						return new Tuple2<>( closing + "#" + opening + "#" + volume, count);
					}
				}, Durations.minutes(10), Durations.minutes(5));


		pd.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<String, Integer> >>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			int iter = 1;

			@Override
			public void call(JavaPairRDD<String, Tuple2<String, Integer>> rdd)
					throws Exception {
				System.out.println("*********************** Iteration " + iter + " *********************** ");
				Map<String, Float> mapProfit = new HashMap<String,Float>();
				Map<String, Float> mapVolume = new HashMap<String,Float>();
				List<Tuple2<String, Tuple2<String, Integer>>> list = rdd.collect();
				for (Tuple2<String, Tuple2<String, Integer>> record : list) {
					String key = record._1;
					String[] values = record._2()._1.split("#");
					Float closing = Float.parseFloat(values[0]);
					Float opening = Float.parseFloat(values[1]);
					Float volume = Float.parseFloat(values[2]);
					Integer count = record._2()._2;
					
					//Problem 1
					System.out.println("Problem 1: Average closing price for cryptocurrency " + key + " is : " + closing/count);
					
					mapProfit.put(key, (closing/count) - (opening/count));
					mapVolume.put(key, (volume/count));
				}
				
				//Problem 2
				float maxValueInProfit=(Collections.max(mapProfit.values()));  // This will return max value in the Hashmap
				for (Entry<String, Float> entry : mapProfit.entrySet()) {  // Iterate through hashmap
					if (entry.getValue()==maxValueInProfit) {
						System.out.println("Problem 2: Maximum profit " + maxValueInProfit + " is for the cryptocurrency " + entry.getKey());     // Print the key with max value
					}
				}
				
				//Problem 3
				float maxValueInVolume=(Collections.max(mapVolume.values()));  // This will return max value in the Hashmap
				for (Entry<String, Float> entry : mapVolume.entrySet()) {  // Iterate through hashmap
					if (entry.getValue()==maxValueInVolume) {
						System.out.println("Problem 3: Maximum volume " + maxValueInVolume + " is for the cryptocurrency " + entry.getKey());     // Print the key with max value
					}
				}
				iter++;
				System.out.println("*********************************************************** ");
			}
		});

		// Start the streaming computation
		jssc.start();
		// / Add Await Termination to respond to Ctrl+C and gracefully close Spark Streams
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
