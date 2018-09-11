import com.ailbb.act.$;
import com.ailbb.act.entity.$KafkaConnConfiguration;
import com.ailbb.act.kafka.$Kafka;
import org.junit.Test;

/**
 * Created by Wz on 9/10/2018.
 */
public class KafkaTest {
    @Test
    public void test() throws Exception {
//        Assert.assertEquals("", KafkaTest.init());
    }

    public static $Kafka init() throws Exception {
        $KafkaConnConfiguration kafkaConnConfiguration = new $KafkaConnConfiguration();

        kafkaConnConfiguration.setIp("slave08");

        return $.kafka.init(kafkaConnConfiguration);
    }
}
