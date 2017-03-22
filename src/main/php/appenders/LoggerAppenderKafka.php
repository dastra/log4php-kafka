<?php
declare(strict_types = 1);
namespace amoma\log4phpKafka\appenders;

use amoma\log4phpKafka\exceptions\KafkaProducerException;
use amoma\log4phpKafka\exceptions\KafkaTimedOutException;
use amoma\log4phpKafka\exceptions\LoggerAppenderKafkaException;
use RdKafka\{Producer, ProducerTopic, Conf, TopicConf};
use LoggerAppender;
use LoggerLoggingEvent;
use Throwable;

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
     * The topic you wish to publish messages on
     * @var string
     */
    protected $_topic = 'default';

    /**
     * Kafka server version
     * @var string
     */
    protected $_kafkaVersion = '0.8.2.2';

    /**
     * List of brokers (ex: 192.168.33.31:9092,192.168.33.31)
     * @var string
     */
    protected $_brokers = "localhost:9092";

    /**
     * Kafka is a partitioned system so not all servers have the complete data set.
     * Instead recall that topics are split into a pre-defined number of partitions, P,
     * and each partition is replicated with some replication factor, N.
     * Topic partitions themselves are just ordered "commit logs" numbered 0, 1, ..., P.
     *
     * @var int
     */
    protected $_partition = RD_KAFKA_PARTITION_UA;

    /**
     * Connection resource
     * @var ProducerTopic
     */
    protected $_producerTopic = null;

    public function activateOptions()
    {
        if (!$this->_producerTopic instanceof ProducerTopic) {
            $this->createProducerTopic();
        }
    }

    public function append(LoggerLoggingEvent $event)
    {
        if (!$this->_producerTopic instanceof ProducerTopic) {
            $this->createProducerTopic();
        }

        try {
            $this->_producerTopic->produce($this->_partition, 0, $event->getMessage());
        } catch (Throwable $exception) {
            throw new LoggerAppenderKafkaException("Error: " . $exception->getMessage());
        }
    }

    private function createProducerTopic()
    {
        $conf = new Conf();
        $conf->setErrorCb(function ($kafka, $err, $reason) {
            $this->_callbackWrite($kafka, $err, $reason);
        });
        $conf->set('broker.version.fallback', $this->_kafkaVersion);
        $conf->set('queue.buffering.max.ms', '1000');

        $confTopic = new TopicConf();

        $topicProducer = new Producer($conf);
        $topicProducer->addBrokers($this->_brokers);

        $this->_producerTopic = $topicProducer->newTopic($this->_topic, $confTopic);
    }

    /**
     * @param $kafka
     * @param $err
     * @param $reason
     * @throws KafkaProducerException
     * @throws KafkaTimedOutException
     */
    private function _callbackWrite($kafka, $err, $reason)
    {
        $error = rd_kafka_err2str($err);

        if (strpos($error, 'Message timed out') !== false || strpos($error, 'Connection timed out') !== false) {
            throw new KafkaTimedOutException(sprintf('Kafka Producer Exception: %1$s', $error));
        } else {
            if (strpos($error, 'Broker transport failure') !== false) {
                throw new KafkaProducerException(sprintf('Kafka Producer Exception: %1$s', $reason));
            }
        }

        throw new KafkaProducerException(sprintf('Kafka Producer Exception: %1$s', $reason));
    }

    public function close()
    {
        $this->_producerTopic = null;
    }

    public function reset()
    {
        $this->close();
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * When serializing, close the connection and save the parameters
     * so it can connect again
     *
     * @return array Properties to save
     */
    public function __sleep(): array
    {
        $this->close();

        $data = [
            'topic' => $this->_topic,
            'kafkaVersion' => $this->_kafkaVersion,
            'brokers' => $this->_brokers,
            'partition' => $this->_partition
        ];

        return $data;
    }

    /**
     * Restore parameters on unserialize
     * @param array $data
     * @return void
     */
    public function __wakeup(array $data)
    {
        $this->_topic = $data['topic'];
        $this->_kafkaVersion = $data['kafkaVersion'];
        $this->_brokers = $data['brokers'];
        $this->_partition = $data['partition'];

        $this->createProducerTopic();
    }

    /**
     * @param string $topic
     */
    public function setTopic(string $topic)
    {
        $this->_topic = $topic;
    }

    /**
     * @return string
     */
    public function getTopic(): string
    {
        return $this->_topic;
    }

    /**
     * @param string $kafkaVersion
     */
    public function setKafkaVersion(string $kafkaVersion)
    {
        $this->_kafkaVersion = $kafkaVersion;
    }

    /**
     * @return string
     */
    public function getKafkaVersion(): string
    {
        return $this->_kafkaVersion;
    }

    /**
     * @param array $brokers
     */
    public function setBrokers(array $brokers)
    {
        $this->_brokers = implode(',', $brokers);
    }

    /**
     * @return array
     */
    public function getBrokers(): array
    {
        return explode(',', $this->_brokers);
    }

    /**
     * @param int $partition
     */
    public function setPartition(int $partition)
    {
        $this->_partition = $partition;
    }

    /**
     * @return int
     */
    public function getPartition(): int
    {
        return $this->_partition;
    }
}
