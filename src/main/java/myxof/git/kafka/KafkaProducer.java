package myxof.git.kafka;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaProducer {
	private final String CONF_FILE_PATH = "conf/kafka-producer.properties";
	private Producer<String, String> producer;
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

	public KafkaProducer() throws IOException {
		init();
	}
	
	private void init() throws IOException{
		this.producer = new Producer<String, String>(new ProducerConfig(readProperties()));	
	}

	private Properties readProperties() throws IOException {
		Properties props = new Properties();
		try (InputStream inPropertiesStream = new BufferedInputStream(new FileInputStream(CONF_FILE_PATH));){			
			props.load(inPropertiesStream);
			inPropertiesStream.close();
		} catch (FileNotFoundException e) {
			logger.error("KafkaProducer config file: "+CONF_FILE_PATH+" does not exists",e);
			throw e;
		} catch (IOException e) {
			logger.error("KafkaProducer fails to load configuration in: "+CONF_FILE_PATH,e);
			throw e;
		}
		logger.debug("KafkaProducer read config file finish");
		return props;
	}

	/**
	 * send message to server
	 * @param topic
	 * @param key decide which partition to send
	 * @param message
	 * @throws IOException 
	 */
	public void sendMessage(String topic, String key, String message) throws IOException {
		if(producer == null){
			logger.warn("KafkaProducer: producer is null before sending message");
			init();
		}
		producer.send(new KeyedMessage<String, String>(topic, key, message));
		logger.debug("KafkaProducer: prodecer send message "+message);
	}

	public void closeProducer() {
		if(producer == null) return;
		producer.close();
		logger.debug("KafkaProducer: producer close");
	}
}
