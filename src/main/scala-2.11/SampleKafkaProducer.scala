import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer
import scala.util.Random
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import java.util.Date

/**
 * Created by shashwat on 2/7/16.
 */
object SampleKafkaProducer {

  def main (args: Array[String]){
    // Defining command line arguments
    val events = args(0).toInt
    val topic = args(1)
    val brokers = args(2)
    val rnd = new Random()

    // Defining kafka connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "SimplePartitioner");
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Sending messages to kafka
    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime();
      val ip = "127.0.0." + rnd.nextInt(255);
      val msg = "At: " + runtime + "," + "EventNum: " + nEvents + ", from IP: " + ip;
      val data = new KeyedMessage[String, String](topic, ip, msg);
      producer.send(data);
    }

    producer.close();
  }

}