package com.capstone;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.parser.JSONParser;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Tuple2;
/**
 * Main Class to provide solution for 
 * Final Capstone project
 * @author Shubhra
 *
 */
public class CreditCardFraudAnalysis {

	private static final int ScoreThreshold = 200;
	private static final double SpeedThreshold = 0.25;
	private static final String tableCardLookup = "card_lookup";
	private static final String tableCardTransactions = "card_transactions";

	static DecimalFormat df = new DecimalFormat("#.00");

	public static void main(String[] args) throws InterruptedException {


		if (args.length < 3) {
			System.out.println(
					"Usage: java -jar <Jar File with Dependencies> <Kafka Broker IP and Port> <Kafka Topic Name> <HBase Server IP>");
			return;
		}


		String kafkaBroker = args[0];
		String kafkaTopic = args[1];
		String HBaseServerIP = args[2];

		SparkConf sparkConf = new SparkConf().setAppName("CreditCardCFraudAnalysis").setMaster("local[*]");
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		jsc.sparkContext().setLogLevel("ERROR");

		String uniqueGroupId = UUID.randomUUID().toString();

		Set<String> topicName = Collections.singleton(kafkaTopic);
		Map<String, Object> kafkaParams = new HashMap<String, Object>();

		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupId);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

		JavaInputDStream<ConsumerRecord<String, String>> sourceStream = KafkaUtils.createDirectStream(jsc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topicName, kafkaParams));

		 // Converting Kafka POS transaction JSON object into the POJO
		JavaPairDStream<String, POSTransaction> posData = sourceStream.map(record -> record.value())
				.mapToPair(new PairFunction<String, String, POSTransaction>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, POSTransaction> call(String x) throws Exception {

						JSONParser jsonParser = new JSONParser();
						Gson gson = new Gson();
						Object obj = jsonParser.parse(x);
						POSTransaction posData = gson.fromJson(obj.toString(), POSTransaction.class);
						return new Tuple2<String, POSTransaction>(posData.getCardId(), posData);
					}
				});

		// Validate each of the POS transaction whether its Genuine or Fraud
		// Update the Lookup
		posData.foreachRDD(r -> {
			r.collect();
			r.foreach(rec -> {
				POSTransaction poxTxn = rec._2;
				ValidatePOSTransaction(poxTxn, HBaseServerIP);

			});
		});

		jsc.start();
		jsc.awaitTermination();

	}

	/*
	 * Method to perform following course of actions:
	 * 1. Lookup data for the card_id of the POStransaction
	 * 2. Validate the the transaction against the three rules and decide POS Transaction is Genuine or Fraud
	 * 3. If Transaction is Genuine, then update the Lookup record with with transaction_dt and postalcode
	 * 4. Insert POS Transaction into card_transactions hbase table
	 */
	static void ValidatePOSTransaction(POSTransaction posTransaction, String HBaseServerIP) {

		try {

			Table lookUpTable = HBaseDAO.GetHBaseTable(HBaseServerIP, tableCardLookup);
			Result result = HBaseDAO.getLookupDataRow(posTransaction.getCardId(), lookUpTable);

			if (result != null) {
				//Retrieve lookup row
				byte[] scoreByte = result.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("score"));
				byte[] uclByte = result.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("UCL"));
				byte[] postCodeByte = result.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("postcode"));
				byte[] transactionDtByte = result.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("transaction_dt"));

				double lookupScore = Double.parseDouble(Bytes.toString(scoreByte));
				double lookupUCL = Double.parseDouble(Bytes.toString(uclByte));
				String lookupPostCode = Bytes.toString(postCodeByte);
				String lookupTxnDt = Bytes.toString(transactionDtByte);

				String transactionDt = posTransaction.getTransactionDate();

				/*
				 * Get the difference of  POS transaction date time and lookup date time, in seconds
				 */
				Date lookupDt = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss", Locale.ENGLISH).parse(lookupTxnDt);
				Date posTxnDt = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss", Locale.ENGLISH).parse(transactionDt);
				Double diffTime = (double) (Math.abs(posTxnDt.getTime() - lookupDt.getTime()) / 1000);

				DistanceUtility distanceUtility = new DistanceUtility();
				Double dist = distanceUtility.getDistanceViaZipCode(lookupPostCode, posTransaction.getPostCode());
				Double speed = dist / diffTime;
				System.out.println("CardId = " + posTransaction.getCardId());
				System.out.println(" Lookup Data:" + " score " + lookupScore + " UCL " + df.format(lookupUCL) + " post code "
						+ lookupPostCode + " transaction date " + lookupTxnDt);

				//Validate the transaction
				double amount = Double.parseDouble(posTransaction.getAmount());
				boolean txnStatus = true;
				if (amount > lookupUCL) {
					txnStatus = false;
					System.out.println("UCL check failed");
				}
				if (lookupScore < ScoreThreshold) {
					txnStatus = false;
					System.out.println("Score threshold failed");
				}
				if (speed > SpeedThreshold) {
					txnStatus = false;
					System.out.println("Speed threshold failed");
				}

				if (txnStatus) {

					posTransaction.setStatus("GENUINE");
					// POS transaction is genuine hence update postcode and transaction_dt
					Put rec = new Put(Bytes.toBytes(posTransaction.getCardId()));
					rec.addColumn(Bytes.toBytes("lookup"), Bytes.toBytes("postcode"),
							Bytes.toBytes(posTransaction.getPostCode()));
					rec.addColumn(Bytes.toBytes("lookup"), Bytes.toBytes("transaction_dt"),
							Bytes.toBytes(posTransaction.getTransactionDate()));
					lookUpTable.put(rec);

				} else {
					posTransaction.setStatus("FRAUD");
				}

				/*
				 * Insert the posTransaction into card_transactions HBase table
				 */
				Table cardTransactionTable = HBaseDAO.GetHBaseTable(HBaseServerIP, tableCardTransactions);

				/*
				 * Column family:"cardtransactions"
				 * HBase rowkey: member_id~transaction_dt~amount; 
				 */

				String rowKeyDelim = "~";

				byte[] rowKey = Bytes.toBytes((posTransaction.getMemberId() + rowKeyDelim
						+ posTransaction.getTransactionDate() + rowKeyDelim + posTransaction.getAmount()));

				Put rec = new Put(rowKey);

				rec.addColumn(Bytes.toBytes("cardtransactions"), Bytes.toBytes("card_id"),
						Bytes.toBytes(posTransaction.getCardId()));

				rec.addColumn(Bytes.toBytes("cardtransactions"), Bytes.toBytes("postcode"),
						Bytes.toBytes(posTransaction.getPostCode()));

				rec.addColumn(Bytes.toBytes("cardtransactions"), Bytes.toBytes("pos_id"),
						Bytes.toBytes(posTransaction.getPOSId()));

				rec.addColumn(Bytes.toBytes("cardtransactions"), Bytes.toBytes("status"),
						Bytes.toBytes(posTransaction.getStatus()));

				cardTransactionTable.put(rec);

				System.out.println(posTransaction.toString());

			} else {
				System.out.println("No record found in lookup for card_id" + posTransaction.getCardId());
			}
		} catch (Exception ex) {

			System.out.println("Error while validating the transaction: " + ex.getMessage());
		}

	}

}
