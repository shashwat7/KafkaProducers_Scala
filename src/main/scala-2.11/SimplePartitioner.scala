import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

/**
 * Created by shashwat on 2/7/16.
 */
class SimplePartitioner(props: VerifiableProperties) extends Partitioner{

   def partition(key: Any, a_numPartitions: Int): Int = {
    val partition: Int = 0
    val keyS: String = key.asInstanceOf[String]
    val offset: Int = keyS.lastIndexOf('.')
    if(offset > 0) keyS.substring(offset+1).toInt % a_numPartitions
    else 0
  }
}