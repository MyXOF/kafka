package myxof.git.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerPartition implements Partitioner {
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerPartition.class);

	public KafkaProducerPartition(VerifiableProperties props) {
	}

	@Override
	public int partition(Object key, int numPartition) {
		return convertDeviceNameToPartiton((String) key, numPartition);
	}

	private int convertDeviceNameToPartiton(String device, int numPartition) {
		try {
			int partition = Integer.parseInt(device) % numPartition;
			return partition;
		} catch (Exception e) {
			logger.error(String.format("KafkaProducerPartition: failed to parse string %s to int",device),e);
		}
		return 0;
		
	}

}