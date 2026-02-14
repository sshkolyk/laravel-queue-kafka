<?php

namespace Rapide\LaravelQueueKafka;

use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use Rapide\LaravelQueueKafka\Queue\Connectors\KafkaConnector;

class LaravelQueueKafkaServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register(): void
    {
        $config_path = file_exists(config_path('queue-kafka.php')) ?
            config_path('queue-kafka.php') :
            __DIR__.'/../config/queue-kafka.php';
        $this->mergeConfigFrom($config_path, 'queue.connections.kafka');

        $this->registerDependencies();
    }

    /**
     * Register the application's event listeners.
     */
    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/queue-kafka.php' => config_path('queue-kafka.php'),
        ], 'queue-kafka-config');
        /** @var QueueManager $queue */
        $queue = $this->app['queue'];
        $connector = new KafkaConnector($this->app);

        $queue->addConnector('kafka', function () use ($connector) {
            return $connector;
        });
    }

    /**
     * Register adapter dependencies in the container.
     */
    protected function registerDependencies(): void
    {
        $this->app->bind('queue.kafka.topic_conf', function () {
            return new \RdKafka\TopicConf;
        });

        $this->app->bind('queue.kafka.producer', function () {
            return new \RdKafka\Producer;
        });

        $this->app->bind('queue.kafka.conf', function () {
            return new \RdKafka\Conf;
        });

        $this->app->bind('queue.kafka.consumer', function ($app, $parameters) {
            return new \RdKafka\Consumer($parameters['conf']);
        });

        $this->app->bind('queue.kafka.kafka_consumer', function ($app, $parameters) {
            return new \RdKafka\KafkaConsumer($parameters['conf']);
        });
    }

    /**
     * Get the services provided by the provider.
     *
     * @return array
     */
    public function provides(): array
    {
        return [
            'queue.kafka.topic_conf',
            'queue.kafka.producer',
            'queue.kafka.kafka_consumer',
            'queue.kafka.consumer',
            'queue.kafka.conf',
        ];
    }
}
