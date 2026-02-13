<?php

namespace Rapide\LaravelQueueKafka\Queue;

use ErrorException;
use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\Log;
use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
use Rapide\LaravelQueueKafka\Queue\Jobs\KafkaJob;
use RdKafka\Consumer;
use RdKafka\Producer;

class KafkaQueue extends Queue implements QueueContract
{
    protected string $defaultQueue;
    protected ?int $sleepOnError = null;
    protected $config;

    private ?string $correlationId = null;
    private ?Producer $_producer = null;
    private ?Consumer $_consumer = null;
    private array $topics = [];
    private array $queues = [];

    public function __construct(array $config)
    {
        $this->defaultQueue = $config['queue'];
        if (@$config['sleep_on_error']) {
            $this->sleepOnError = $config['sleep_on_error'];
        }
        $this->config = $config;
    }

    /**
     * Get the size of the queue.
     *
     * @param null|string $queue
     *
     * @return int
     */
    public function size($queue = null): int
    {
        //Since Kafka is an infinite queue we can't count the size of the queue.
        return 1;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param string $job
     * @param mixed $data
     * @param string $queue
     *
     * @return bool
     */
    public function push($job, $data = '', $queue = null): bool
    {
        return $this->pushRaw($this->createPayload($job, $queue, $data), $queue, []);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param mixed $payload
     * @param ?string $queue
     * @param array $options
     *
     * @throws QueueKafkaException
     *
     * @return ?string
     */
    public function pushRaw($payload, $queue = null, array $options = []): ?string
    {
        try {
            $topic = $this->getTopic($queue);

            $pushRawCorrelationId = $this->getCorrelationId();

            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $pushRawCorrelationId);

            return $pushRawCorrelationId;
        } catch (ErrorException $exception) {
            $this->reportConnectionError('pushRaw', $exception);
            return null;
        }
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param \DateTime|int $delay
     * @param string $job
     * @param mixed $data
     * @param string $queue
     *
     * @throws QueueKafkaException
     *
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        //Later is not sup
        throw new QueueKafkaException('Later not yet implemented');
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string|null $queue
     *
     * @throws QueueKafkaException
     *
     * @return KafkaJob|null
     */
    public function pop($queue = null): ?KafkaJob
    {
        try {
            $queue = $this->getQueueName($queue);
            if (!array_key_exists($queue, $this->queues)) {
                $this->queues[$queue] = $this->getConsumer()->newQueue();
                $topicConf = new \RdKafka\TopicConf();
                $topicConf->set('auto.offset.reset', 'largest');

                $this->topics[$queue] = $this->getConsumer()->newTopic($queue, $topicConf);
                $this->topics[$queue]->consumeQueueStart(0, RD_KAFKA_OFFSET_STORED, $this->queues[$queue]);
            }

            $message = $this->queues[$queue]->consume(1000);

            if ($message === null) {
                return null;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    return new KafkaJob(
                        $this->container, $this, $message,
                        $this->connectionName, $queue ?: $this->defaultQueue, $this->topics[$queue]
                    );
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    return null;
                default:
                    throw new QueueKafkaException($message->errstr(), $message->err);
            }
        } catch (\RdKafka\Exception $exception) {
            throw new QueueKafkaException('Could not pop from the queue', 0, $exception);
        }
    }

    /**
     * @param null|string $queue
     *
     * @return string
     */
    private function getQueueName(?string $queue = null): string
    {
        return $queue ?: $this->defaultQueue;
    }

    /**
     * Return a Kafka Topic based on the name
     *
     * @param null|string $queue
     *
     * @return \RdKafka\ProducerTopic
     */
    private function getTopic(?string $queue = null): \RdKafka\ProducerTopic
    {
        return $this->getProducer()->newTopic($this->getQueueName($queue));
    }

    /**
     * Sets the correlation id for a message to be published.
     *
     * @param string $id
     */
    public function setCorrelationId(string $id): void
    {
        $this->correlationId = $id;
    }

    /**
     * Retrieves the correlation id, or a unique id.
     *
     * @return string
     */
    public function getCorrelationId(): string
    {
        return $this->correlationId ?: uniqid('', true);
    }

    /**
     * Create a payload array from the given job and data.
     *
     * @param  string $job
     * @param  ?string $queue
     * @param  mixed $data
     *
     * @return array
     */
    protected function createPayloadArray($job, $queue = null, $data = ''): array
    {
        return array_merge(parent::createPayloadArray($job, $queue, $data), [
            'id' => $this->getCorrelationId(),
            'attempts' => 0,
        ]);
    }

    /**
     * @param string $action
     * @param Exception $e
     *
     * @throws QueueKafkaException
     */
    protected function reportConnectionError(string $action, Exception $e): void
    {
        Log::error('Kafka error while attempting ' . $action . ': ' . $e->getMessage());

        // If it's set to false, throw an error rather than waiting
        if (!$this->sleepOnError) {
            throw new QueueKafkaException('Error writing data to the connection with Kafka');
        }

        // Sleep so that we don't flood the log file
        sleep($this->sleepOnError);
    }

    /**
     * @return Consumer
     */
    private function getConsumer(): Consumer
    {
        if (!$this->_consumer) {
            /** @var \RdKafka\TopicConf $topicConf */
            $topicConf = App::makeWith('queue.kafka.topic_conf', []);
            $topicConf->set('auto.offset.reset', 'largest');

            /** @var \RdKafka\Conf $conf */
            $consumerConf = App::makeWith('queue.kafka.conf', []);
            $consumerConf->set('bootstrap.servers', $this->config['bootstrap_servers']);
            if (true === $this->config['sasl_enable']) {
                $consumerConf->set('sasl.mechanisms', 'PLAIN');
                $consumerConf->set('sasl.username', $this->config['sasl_plain_username']);
                $consumerConf->set('sasl.password', $this->config['sasl_plain_password']);
                $consumerConf->set('ssl.ca.location', $this->config['ssl_ca_location']);
            }
            $consumerConf->set('group.id', $this->config['consumer_group_id']);
            $consumerConf->set('metadata.broker.list', $this->config['brokers']);
            $consumerConf->set('enable.auto.commit', $this->config['auto_commit']);
            $consumerConf->setDefaultTopicConf($topicConf);

            /** @var \RdKafka\KafkaConsumer $consumer */
            $this->_consumer = $this->container->makeWith('queue.kafka.consumer', ['conf' => $consumerConf]);
        }
        return $this->_consumer;
    }

    private function getProducer(): Producer
    {
        if (!$this->_producer) {
            /** @var \RdKafka\Conf $producerConf */
            $producerConf = App::makeWith('queue.kafka.conf', []);
            $producerConf->set('bootstrap.servers', $this->config['bootstrap_servers']);
            $producerConf->set('metadata.broker.list', $this->config['brokers']);
            if (true === $this->config['sasl_enable']) {
                $producerConf->set('sasl.mechanisms', 'PLAIN');
                $producerConf->set('sasl.username', $this->config['sasl_plain_username']);
                $producerConf->set('sasl.password', $this->config['sasl_plain_password']);
                $producerConf->set('ssl.ca.location', $this->config['ssl_ca_location']);
            }
            /** @var \RdKafka\Producer $producer */
            $this->_producer = new \RdKafka\Producer($producerConf);
        }
        return $this->_producer;
    }
}
