# kafka
It simply shows how to write code using kafka API

Before using Kafka API, you should carefully read offcial documentation(http://kafka.apache.org/documentation.html).

For Kafka producer, you only need to concern the function below
	/**
	 * send message to server
	 * @param topic
	 * @param key decide which partition to send
	 * @param message
	 * @throws IOException 
	 */
	public void sendMessage(String topic, String key, String message)

For Kafka consumer, you need to manage lots of complex things.
	/**
	 * fetch message set from server
	 * @param topic 
	 * @param partition
	 * @return FetchResponse contains message set
	 */
	public FetchResponse consumeTopic(String topic,int partition)
	This method helpes to get message from server

 	/**
     * @param partitionCommit contains partition and topic to commit to
     * @param offset current offset which has been consumed
     * @param commitMetadata commit string
     */
    public void commitOffsetToKafka(TopicAndPartition partitionCommit,long offset,String commitMetadata)
    This method helps you commit offset which you have consumed to kafka server. Next time, you will fetch message from the offset 
    
    
    
    
For config you can modify at /conf and see http://kafka.apache.org/documentation.html#configuration
    
    
    
