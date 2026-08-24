<?php

namespace Rapide\LaravelQueueKafka\Queue\Connectors;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Rapide\LaravelQueueKafka\Queue\KafkaQueue;

class KafkaConnector implements ConnectorInterface
{
    private Container $container;

    /**
     * KafkaConnector constructor.
     */
    public function __construct(Container $container)
    {
        $this->container = $container;
    }

    /**
     * Establish a queue connection.
     */
    public function connect(array $config): Queue
    {
        return new KafkaQueue($config);
    }
}
