package myxof.git.kafka;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.ConsumerMetadataRequest;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.network.BlockingChannel;

public class KafkaSimpleConsumer {
	private SimpleConsumer consumer;
	private List<String> replicaBrokers;
	private List<String> cluster;
	private int port;
	private int soTimeout;
	private int bufferSize;
	private int fetchSize;
	
	private String CONF_FILE_PATH = "conf/kafka-simple-consumer.properties";
	
	private String myGroupName;
	private String myClientId;
	private String myClientName;
	
	private int correlationId = 0;
	private long readOffset = 0;
	
	private final int MAX_RETRY_FIND_NEW_LEADER = 3;
	private final int MAX_RETRY_FETCH_MESSAGE = 5;

	private static final Logger logger = LoggerFactory.getLogger(KafkaSimpleConsumer.class);
	
	public KafkaSimpleConsumer(String groupName,String clientId) throws IOException{
		this.myGroupName = groupName;
		this.myClientId = clientId;
		this.myClientName = "";
		replicaBrokers = new ArrayList<String>();
		cluster = new ArrayList<String>();
		readSimpleConsumerConfig();
	}
	
	private void readSimpleConsumerConfig() throws IOException{
		Properties props = new Properties();
		try (InputStream in = new BufferedInputStream (new FileInputStream(CONF_FILE_PATH))){	
			props.load(in);
			String brokerList = props.getProperty("simple_consumer_brokerList");
			String[] brokers = brokerList.trim().split(",");
			for(String broker : brokers){
				cluster.add(broker);
			}
			
			port = Integer.parseInt(props.getProperty("simple_consumer_port"));
			soTimeout = Integer.parseInt(props.getProperty("simple_consumer_soTimeout"));
			bufferSize = Integer.parseInt( props.getProperty("simple_consumer_bufferSize"));
			fetchSize = Integer.parseInt(props.getProperty("simple_consumer_fetchSize"));
		} catch (FileNotFoundException e) {
			logger.error("KafkaSimpleConsumer: config file: "+CONF_FILE_PATH+" does not exists",e);
			throw e;
		} catch (IOException e) {
			logger.error("KafkaSimpleConsumer: fails to load configuration in: "+CONF_FILE_PATH,e);
			throw e;
		}
		logger.debug("KafkaSimpleConsumer read config finish");
	}
	
	public long getReadOffset(){
		return readOffset;
	}
	
	/**
	 * fetch message set from server
	 * @param topic 
	 * @param partition
	 * @return FetchResponse contains message set
	 */
	public FetchResponse consumeTopic(String topic,int partition){		
		consumer = createTargetConsumer(topic, partition);
        String leadBroker = consumer.host();
        try {
			readOffset = getCurrentOffset(topic, partition,leadBroker);
		} catch (IOException e) {
			return null;
		}
        int numErrors = 0;
        while(true){
        	FetchResponse fetchResponse = fetchMessage(readOffset, leadBroker, topic, partition);
        	if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, partition);
                logger.warn("KafkaSimpleConsumer Error fetching data from the Broker:" + consumer.host() + " Reason: " + code);
                if (numErrors > MAX_RETRY_FETCH_MESSAGE) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer,topic, partition, kafka.api.OffsetRequest.LatestTime(), myClientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, topic, partition, port);
                continue;
            }
            numErrors = 0;
 
            return fetchResponse;
        }
		return null;
        
	}
	
	private SimpleConsumer createTargetConsumer(String topic,int partition){
        PartitionMetadata metadata = findLeader(cluster, port, topic, partition);
        if (metadata == null) {
        	logger.error("KafkaSimpleConsumer: Can't find metadata for Topic and Partition. Exiting");
            return null;
        }
        if (metadata.leader() == null) {
        	logger.error("KafkaSimpleConsumer: Can't find Leader for Topic and Partition. Exiting");
            return null;
        }
        String leadBroker = metadata.leader().host();
        myClientName = "Client_" + topic + "_" + partition;
        
        return (new SimpleConsumer(leadBroker, port, soTimeout, bufferSize, myClientName));
	}
	
	private long getCurrentOffset(String topic,int partition,String host) throws IOException{
        long readOffsetFromKafka = 0;
        TopicAndPartition partitionFetch = new TopicAndPartition(topic,partition);
        try {        	
			readOffsetFromKafka = fetchOffsetFromKafka(partitionFetch, createChannel(host));
        	if(readOffsetFromKafka < 0){
				readOffsetFromKafka = getLastOffset(consumer,topic, partition, kafka.api.OffsetRequest.EarliestTime(), myClientName);
			}
		} catch (IOException e) {
			logger.error("KafkaSimpleConsumer: failed to get offset from server",e);
			throw e;
		}
        
        return readOffsetFromKafka;
	}
	
	private FetchResponse fetchMessage(long readOffset,String leadBroker,String topic,int partition){
        if (consumer == null) {
            consumer = new SimpleConsumer(leadBroker, port, soTimeout, bufferSize, myClientName);
        }
        FetchRequest req = new FetchRequestBuilder()
                .clientId(myClientId)
                .addFetch(topic, partition, readOffset, fetchSize) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                .build();
        return (consumer.fetch(req));
	}
	
    private String findNewLeader(String oldLeader, String topic, int partition, int port) {
        for (int i = 0; i < MAX_RETRY_FIND_NEW_LEADER; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
            }
        }
        logger.error("KafkaSimpleConsumer: Unable to find new leader after Broker failure. Exiting");
		return null;
    }
 
    private PartitionMetadata findLeader(List<String> seedBrokers, int port, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : seedBrokers) {
            SimpleConsumer consumerLeader = null;
            try {
            	consumerLeader = new SimpleConsumer(seed, port, soTimeout, bufferSize, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumerLeader.send(req);
                logger.debug("KafkaSimpleConsumer: send find lead reqest to kafka server");
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
                consumerLeader.close();
            } catch (Exception e) {
            	logger.error("KafkaSimpleConsumer:  Error communicating with Broker [" + seed + ":" + port+ "] to find Leader for [" + topic + ", " + partition + "]",e);
            } finally {
                if (consumerLeader != null) consumerLeader.close();
            }
        }
        if (returnMetaData != null) {
            replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
    
    private long getLastOffset(SimpleConsumer consumerSimple, String topic, int partition,long whichTime, String clientName) {
    	TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumerSimple.getOffsetsBefore(request);
	
		if (response.hasError()) {
			logger.error("KafkaSimpleConsumer: Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
    
    private BlockingChannel createChannel(String leadBroker){
    	BlockingChannel channel = new BlockingChannel(leadBroker, port,
		        BlockingChannel.UseDefaultBufferSize(),
		        BlockingChannel.UseDefaultBufferSize(),
		        1000 /* read timeout in millis */);
		channel.connect();
		channel.send(new ConsumerMetadataRequest(myGroupName, ConsumerMetadataRequest.CurrentVersion(), correlationId++, myClientId));
		ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());
    
		if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
		    Broker offsetManager = metadataResponse.coordinator();
		    // if the coordinator is different, from the above channel's host then reconnect
		    channel.disconnect();
		    channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
		                                  BlockingChannel.UseDefaultBufferSize(),
		                                  BlockingChannel.UseDefaultBufferSize(),
		                                  1000 /* read timeout in millis */);
		    channel.connect();
		} else {
		    // retry (after backoff)
		}
		logger.debug("KafkaSimpleConsumer: create and connect channel");
		return channel;
    }

    
    /**
     * @param partitionCommit contains partition and topic to commit to
     * @param offset current offset which has been consumed
     * @param commitMetadata commit string
     */
    public void commitOffsetToKafka(TopicAndPartition partitionCommit,long offset,String commitMetadata){
    	if(consumer == null){
    		logger.warn("tt_dbcore_nrsync KafkaSimpleConsumer: consumer is null");
    		createTargetConsumer(partitionCommit.topic(), partitionCommit.partition());
    	}
    	commitOffsetToKafka(partitionCommit, offset, commitMetadata, createChannel(consumer.host()));
    }
    
    private void commitOffsetToKafka(TopicAndPartition partitionCommit,long offset,String commitMetadata,BlockingChannel channel){    	
		long now = System.currentTimeMillis();
		Map<TopicAndPartition, OffsetAndMetadata> offsets = new LinkedHashMap<TopicAndPartition, OffsetAndMetadata>();
		offsets.put(partitionCommit, new OffsetAndMetadata(offset, commitMetadata, now));
		OffsetCommitRequest commitRequest = new OffsetCommitRequest(
              myGroupName,
              offsets,
              correlationId++,
              myClientId,
              (short) 1 /* version */); // version 1 and above commit to Kafka, version 0 commits to ZooKeeper
		channel.send(commitRequest.underlying());
		logger.debug("KafkaSimpleConsumer: send commit request to kafka server by channel");
		OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());
		if (commitResponse.hasError()) {
			commitResponse.errors().values();
			for (Object partitionErrorCode: commitResponse.errors().values()) {
                if ((Short) partitionErrorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
                    // You must reduce the size of the metadata if you wish to retry
                } else if ((Short) partitionErrorCode == ErrorMapping.NotCoordinatorForConsumerCode() || (Short) partitionErrorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                  channel.disconnect();
                  logger.debug("KafkaSimpleConsumer: close channel from kafka server");
                  // Go to step 1 (offset manager has moved) and then retry the commit to the new offset manager
                } else {
                    // log and retry the commit
                }
            }
			logger.warn("KafkaSimpleConsumer: commit offset: " + offset + "failed!");
		}
		else{
			logger.debug("KafkaSimpleConsumer: commit offset: " + offset + "successs!");
		}
		channel.disconnect();
	    logger.debug("KafkaSimpleConsumer: close channel from kafka server");
    }
    
    private long fetchOffsetFromKafka(TopicAndPartition partitionFetch,BlockingChannel channel) throws IOException{
		List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        partitions.add(partitionFetch);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                myGroupName,
                partitions,
                (short) 1 /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                correlationId,
                myClientId);
        channel.send(fetchRequest.underlying());
		OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer());
		OffsetMetadataAndError result = fetchResponse.offsets().get(partitionFetch);
		short offsetFetchErrorCode = result.error();
		if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
		    channel.disconnect();
		    logger.debug("KafkaSimpleConsumer: close channel from kafka server");
		    // Go to step 1 and retry the offset fetch
		} else if (offsetFetchErrorCode == ErrorMapping.OffsetsLoadInProgressCode()) {
		    // retry the offset fetch (after backoff)
			channel.disconnect();
			logger.debug("KafkaSimpleConsumer: close channel from kafka server");
		} else {
		    long retrievedOffset = result.offset();
		    String retrievedMetadata = result.metadata();
		    logger.debug("KafkaSimpleConsumer: offset fetch is "+retrievedOffset+" Metadata is "+retrievedMetadata);
		    channel.disconnect();
		    logger.debug("KafkaSimpleConsumer: close channel from kafka server");
		    return retrievedOffset;
		}
		channel.disconnect();
		logger.debug("KafkaSimpleConsumer: close channel from kafka server");
		return 0;
    }
    
    public void closeSimpleConsumer(){
    	if(consumer == null) return;
    	consumer.close();
    	logger.debug("KafkaSimpleConsumer: close consumer from kafka server");
    }

}
