<?php

/**
 * Serialize events and send them to Kafka
 *
 * This appender can be configured by changing the following attributes:
 * 
 * - remoteHost         - Sets the Kafka Broker IP address - defaults to localhost
 * - port               - Sets the port of the Kafka Broker.
 * - topic              - Sets the Kafka topic to publish messages to
 * - partition          - Sets the Kafka parition to publish to - defaults to 0
 *
 * Parameters are {@link $remoteHost}, {@link $port}, {@link $topic},
 * {@link $partition}.
 *
 * An example:
 * 
 * {@example ../../examples/php/appender_kafka.php 19}
 * 
 * {@example ../../examples/resources/appender_kafka.properties 18}
 * 
 * @package log4php
 * @subpackage appenders
 */ 
class LoggerAppenderKafka extends LoggerAppender
{
	/**
	 * Target host. On how to define remote hostaname see
	 * {@link PHP_MANUAL#fsockopen}
	 * @var string
	 */
	private $_remoteHost = 'localhost';

	/**
	 * @var integer the port the Kafka broker is running on
	 */
	private $_port = 9092;

    /**
     * @var string the topic you wish to publish messages on
     */
    private $_topic = null;

    /**
     * Kafka is a partitioned system so not all servers have the complete data set.
     * Instead recall that topics are split into a pre-defined number of partitions, P,
     * and each partition is replicated with some replication factor, N.
     * Topic partitions themselves are just ordered "commit logs" numbered 0, 1, ..., P.
     *
     * At the moment, we have no way of passing in the partition number.  So, we will just
     * set it to a constant.
     *
     * @var integer
     */
    private $_partition = 0xFFFFFFFF;

	/**
	 * @var mixed socket connection resource
	 * @access private
	 */
	private $_sp = false;

    /**
     * Kafka Batching
     *
     * Our apis encourage batching small things together for efficiency. We have found this is a very significant performance win.
     * Both our API to send messages and our API to fetch messages always work with a sequence of messages not a
     * single message to encourage this. A clever client can make use of this and support an "asynchronous" mode
     * in which it batches together messages sent individually and sends them in larger clumps.
     * We go even further with this and allow the batching across multiple topics and partitions,
     * so a produce request may contain data to append to many partitions and a fetch request may pull data from many partitions all at once.
     */
    const KAFKA_PRODUCE_REQUEST_ID = 0;

    /**
	 * 1 byte "magic" identifier to allow format changes
	 *
	 * @var integer
	 */
	const KAFKA_CURRENT_MAGIC_VALUE = 0;

	
	/** @var bool indicates if this appender should run in dry mode
     * This enables us to test most of the functionality without using the actual Kafka calls
     */
	private $_dryRun = false;
	
	/**
	 * create a socket connection using defined parameters
     * // Done
	 */
	public function activateOptions() {
		if(!$this->_dryRun)
        {
            $errstr = '';
            $errno = null;
            if (!is_resource($this->_sp)) {
		    	$this->_sp = stream_socket_client('tcp://' . $this->_remoteHost . ':' . $this->_port, $errno, $errstr);
		    }
		    if (!is_resource($this->_sp)) {
			    throw new LoggerException('Cannot connect to Kafka: ' .$this->getRemoteHost().":".$this->getPort().": $errstr ($errno)");
		    }
		}

        $this->closed = false;
	}

    // Done
	public function close() {
		if($this->closed != true) {
            if (!$this->_dryRun and is_resource($this->_sp)) {
                fclose($this->_sp);
            }
			$this->closed = true;
		}
	}

	public function reset() {
		$this->close();
	}


	public function append(LoggerLoggingEvent $event)
    {
		if(( is_resource($this->_sp) || $this->_dryRun) && $this->layout !== null)
        {
            $buffer = $this->layout->format($event);
            $encodedBuffer = self::encode_message($buffer);
            $messageBody = pack('N', strlen($encodedBuffer)) . $encodedBuffer;

            // encode messages as <LEN: int><MESSAGE_BYTES>
            // create the request as <REQUEST_SIZE: int> <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER_SIZE: int> <BUFFER: bytes>
            $data = pack('n', self::KAFKA_PRODUCE_REQUEST_ID) .
                pack('n', strlen($this->_topic)) . $this->_topic .
                pack('N', $this->_partition) .
                pack('N', strlen($messageBody)) . $messageBody;

		    $toSend = pack('N', strlen($data)) . $data;

            if(!$this->_dryRun)
            {
                fwrite($this->_sp, $toSend);
            } else {
                echo "DRY MODE OF SOCKET APPENDER: ".$toSend;
            }
		}
	}

    private static function encode_message($msg)
    {
		// <MAGIC_BYTE: 1 byte> <CRC32: 4 bytes bigendian> <PAYLOAD: N bytes>
		return pack('CN', self::KAFKA_CURRENT_MAGIC_VALUE, crc32($msg))
			 . $msg;
	}

    public function __destruct() {
       $this->close();
   	}

    /**
	 * When serializing, close the socket and save the connection parameters
	 * so it can connect again
	 *
	 * @return array Properties to save
	 */
	public function __sleep() {
		$this->close();
		return array('request_key', 'host', 'port');
	}

	/**
	 * Restore parameters on unserialize
	 *
	 * @return void
	 */
	public function __wakeup() {

	}

    public function setDry($dry) {
		$this->_dryRun = $dry;
	}

    public function getRemoteHost() {
		return $this->_remoteHost;
	}

    /**
	 * @param string
	 */
	public function setRemoteHost($hostname) {
		$this->_remoteHost = $hostname;
	}

    /**
	 * @return integer
	 */
	public function getPort() {
		return $this->_port;
	}

    /**
	 * @param integer $port Sets the target port
	 */
	public function setPort($port) {
		if($port > 0 and $port < 65535) {
		    $this->setPositiveInteger('port', $port);
        }
	}

    /**
     * @param string $topic
     */
    public function setTopic($topic)
    {
        $this->_topic = $topic;
    }

    /**
     * @return string
     */
    public function getTopic()
    {
        return $this->_topic;
    }

    /**
     * @param int $partition
     */
    public function setPartition($partition)
    {
        $this->_partition = $partition;
    }

    /**
     * @return int
     */
    public function getPartition()
    {
        return $this->_partition;
    }
}
