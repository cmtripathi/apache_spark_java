/**
 * 
 */
package cmt.gl.spark;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * @author c.tripathi
 *
 */
public class ReadDBService {

	private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		String master = "local[*]";
		readDB(master);
		readLog("/Users/c.tripathi/gr/logs/apache_logs.log", master);
	}

	/**
	 * 
	 * This reads Person table from MySQL DB.
	 * 
	 * @param master
	 */
	public static void readDB(String master) {
		SparkSession spark = SparkSession.builder().appName("gl.spark").master(master).getOrCreate();
		Properties properties = new Properties();
		properties.setProperty("user", "root");
		properties.setProperty("password", "password");
		properties.put("driver", "com.mysql.cj.jdbc.Driver");
		Dataset<Row> df = spark.read().jdbc("jdbc:mysql://localhost:3306/spark_poc?useSSL=false", "Person", properties);
		df.show();
	}

	/**
	 * 
	 * This reads the log/text file from the given location.
	 * 
	 * @param logFile - The file location.
	 * @param master
	 */
	public static void readLog(String logFile, String master) {
		SparkConf conf = new SparkConf().setAppName("Log Analyzer").setMaster(master);
		conf.set("spark.driver.allowMultipleContexts", "true");
		sc = new JavaSparkContext(conf);

		JavaRDD<String> logLines = sc.textFile(logFile);

		// Convert the text log lines to ApacheAccessLog objects and cache them
		// since multiple transformations and actions will be called on that data.
		JavaRDD<ApacheAccessLog> accessLogs = logLines.map(ApacheAccessLog::parseFromLogLine).cache();

		// Calculate statistics based on the content size.
		// Note how the contentSizes are cached as well since multiple actions
		// are called on that RDD.
		JavaRDD<Long> contentSizes = accessLogs.map(ApacheAccessLog::getContentSize).cache();
		System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
				contentSizes.reduce(SUM_REDUCER) / contentSizes.count(), contentSizes.min(Comparator.naturalOrder()),
				contentSizes.max(Comparator.naturalOrder())));

		// Compute Response Code to Count.
		List<Tuple2<Integer, Long>> responseCodeToCount = accessLogs
				.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L)).reduceByKey(SUM_REDUCER).take(100);
		System.out.println(String.format("Response code counts: %s", responseCodeToCount));

		// Top Endpoints.
		List<Tuple2<String, Long>> topEndpoints = accessLogs.mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
				.reduceByKey(SUM_REDUCER).top(10, new ValueComparator<>(Comparator.<Long>naturalOrder()));
		System.out.println(String.format("Top Endpoints: %s", topEndpoints));

		// Stop the Spark Context before exiting.
		sc.stop();
	}

	private static class ValueComparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable {
		private static final long serialVersionUID = -1337985154601673142L;
		private Comparator<V> comparator;

		public ValueComparator(Comparator<V> comparator) {
			this.comparator = comparator;
		}

		@Override
		public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
			return comparator.compare(o1._2(), o2._2());
		}
	}

}
