import java.util
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, BlockingQueue}
import java.util.{Date, Properties}

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.httpclient.BasicClient

import scala.util.Random
;
/**
 * Created by shashwat on 2/7/16.
 */
object TwitterKafkaProducer {
  def main(args: Array[String]) {
    // Defining command line arguments
    val consumerKey: String = args(0)
    val consumerSecret: String = args(1)
    val token: String = args(2)
    val secret: String = args(3)

    val twitter_topic = "tweets"
    val brokers = "localhost:9092"
    val rnd = new Random()

    // Defining kafka connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "SimplePartitioner");
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Create an appropriately sized blocking queue
    val queue: BlockingQueue[String] = new LinkedBlockingQueue[String](10000);
    val endpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint();
    // add some track terms
    endpoint.trackTerms(new util.ArrayList(util.Arrays.asList("a","b")))

    val auth: Authentication = new OAuth1(consumerKey, consumerSecret, token, secret);

    // Create a new BasicClient. By default gzip is enabled.
    val client: BasicClient = new ClientBuilder()
      .name("TestingKafkaTwitterStream")
      .hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue))
      .build()

    // Establish a connection
    client.connect()

    while(!client.isDone){
      val msg: String = queue.poll(5, TimeUnit.SECONDS)
      if(msg == null) println("Did not receive a message in 5 seconds")
      else {
        // Sending messages to kafka
        val ip = "127.0.0." + rnd.nextInt(255);
        val data = new KeyedMessage[String, String](twitter_topic, ip, msg);
        producer.send(data);
      }
    }

    client.stop()
    // Print some stats
    println("The client read "+client.getStatsTracker().getNumMessages()+" messages!\n");

    producer.close();

  }
}
