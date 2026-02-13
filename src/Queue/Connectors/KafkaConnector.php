<?php

namespace Rapide\LaravelQueueKafka\Queue\Connectors;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Rapide\LaravelQueueKafka\Queue\KafkaQueue;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\TopicConf;
use Illuminate\Support\Arr;

class KafkaConnector implements ConnectorInterface
{
    private Container $container;

    /**
     * KafkaConnector constructor.
     *
     * @param Container $container
     */
    public function __construct(Container $container)
    {
        $this->container = $container;
    }

    /**
     * Establish a queue connection.
     *
     * @param array $config
     *
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config): Queue
    {
        /** @var \RdKafka\Conf $conf */
        $conf = $this->container->makeWith('queue.kafka.conf', []);
        $conf->set('bootstrap.servers', $config['bootstrap_servers']);
        $conf->set('metadata.broker.list', $config['brokers']);
        /** @var \RdKafka\Producer $producer */
        $producer = new \RdKafka\Producer($conf);

        /** @var \RdKafka\TopicConf $topicConf */
        $topicConf = $this->container->makeWith('queue.kafka.topic_conf', []);
        $topicConf->set('auto.offset.reset', 'largest');

        /** @var \RdKafka\Conf $conf */
        $conf = $this->container->makeWith('queue.kafka.conf', []);
        $conf->set('bootstrap.servers', $config['bootstrap_servers']);
        if (true === $config['sasl_enable']) {
            $conf->set('sasl.mechanisms', 'PLAIN');
            $conf->set('sasl.username', $config['sasl_plain_username']);
            $conf->set('sasl.password', $config['sasl_plain_password']);
            $conf->set('ssl.ca.location', $config['ssl_ca_location']);
        }
        $conf->set('group.id', $config['consumer_group_id']);
        $conf->set('metadata.broker.list', $config['brokers']);
        $conf->set('enable.auto.commit', $config['auto_commit']);
        $conf->setDefaultTopicConf($topicConf);

        /** @var \RdKafka\KafkaConsumer $consumer */
        $consumer = $this->container->makeWith('queue.kafka.consumer', ['conf' => $conf]);

        return new KafkaQueue(
            $producer,
            $consumer,
            $config
        );
    }
}
