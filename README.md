# Kafka Queue Driver for Laravel 10–13

[![Latest Stable Version](https://img.shields.io/packagist/v/sshkolyk/laravel-queue-kafka.svg?style=flat-square)](https://packagist.org/packages/sshkolyk/laravel-queue-kafka)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)](LICENSE)

A Laravel queue driver backed by Apache Kafka, with support for Laravel 10–13, php-rdkafka 6.x, and librdkafka 2.x.

[View the repository on GitHub](https://github.com/sshkolyk/laravel-queue-kafka)

## Improvements over upstream

- Supports PHP 8.2+ and Laravel 10–13.
- Jobs can define their Kafka producer key through `HasKafkaKey` instead of relying on payload correlation IDs.
- Parallel workers can consume explicitly configured Kafka partitions instead of being limited to partition 0.
- Consumer partitions, producer partitioning, auto-commit, timeouts, and SASL protocol and mechanism are configurable.
- Offset reset is configurable and defaults to `earliest`, preventing new consumer groups from skipping existing jobs as upstream's hard-coded `largest` policy did.
- SASL/SSL authentication works for both producers and consumers, including PLAIN and SCRAM mechanisms.
- Queue size is reported as consumer lag using partition watermarks and committed offsets.
- Producer writes are flushed before returning and retried once with a fresh producer after a failure.
- Kafka clients and queue connectors are created lazily to avoid stale shared instances in long-lived workers such as Laravel Octane.
- Jobs use Laravel's standard execution path without database-specific deadlock detection that slowed Kafka consumers.
- Expanded automated tests cover queue lag, producer retries, auto-commit normalization, partition EOF handling, custom keys, and job IDs.

## Limitations

- Delayed dispatch through `Queue::later()` is not supported.
- Automatic job retries and `queue:work --tries` are not supported; failed jobs must be handled and requeued by the application.
- Laravel's `queue:clear` command is not supported. Kafka records may be shared by multiple consumer groups, so clearing a Laravel queue cannot safely delete them. Advancing a consumer group's committed offset is also unsafe while workers are active: a worker may later commit an older offset or finish a job fetched before the reset. Stop the group's consumers and manage its offsets explicitly with Kafka tooling instead.

## Installation

This package requires PHP 8.2+, librdkafka 2.x, and the php-rdkafka 6.x extension.

Install librdkafka using your operating system's package manager instead of building the development branch from source:

```bash
# Debian or Ubuntu
sudo apt update
sudo apt install librdkafka-dev

# Fedora, RHEL, or CentOS
sudo dnf install librdkafka-devel

# Alpine Linux
apk add --no-cache librdkafka-dev

# macOS
brew install librdkafka
```

If your distribution provides an older librdkafka release, use the packages from the [official Confluent repositories](https://github.com/confluentinc/librdkafka#installing-prebuilt-packages).

Install and enable the [php-rdkafka](https://github.com/arnaud-lb/php-rdkafka) extension:

```bash
pecl install rdkafka
```

Add `extension=rdkafka.so` to `php.ini` if PECL does not enable it automatically, then verify the installation:

```bash
php --ri rdkafka
```

Install the Laravel package and optionally publish its configuration:

```bash
composer require sshkolyk/laravel-queue-kafka
php artisan vendor:publish --tag=queue-kafka-config
```

Configure the queue connection and Kafka brokers in `.env`:

```dotenv
QUEUE_CONNECTION=kafka
KAFKA_BROKERS=localhost:9092
```

## Running workers

Run a worker using the configured consumer group:

```bash
php artisan queue:work kafka
```

Override the consumer group for a worker when needed:

```bash
KAFKA_CONSUMER_GROUP_ID=group2 php artisan queue:work kafka --sleep=3
```

For parallel processing, run one worker for each Kafka partition:

```bash
KAFKA_CONSUMER_PARTITION=0 php artisan queue:work kafka
KAFKA_CONSUMER_PARTITION=1 php artisan queue:work kafka
```

## Usage

Use Laravel's standard queue API to dispatch and process jobs. Kafka-specific behavior is configured through the connection settings documented below. See the [Laravel queue documentation](https://laravel.com/docs/queues) for general usage.

### Ordering jobs by key

Kafka guarantees ordering only within a partition. By default, each job receives a random producer key and may be routed to any partition. To keep related jobs on the same partition, implement `Rapide\LaravelQueueKafka\Contracts\HasKafkaKey` on the job:

```php
use Illuminate\Contracts\Queue\ShouldQueue;
use Rapide\LaravelQueueKafka\Contracts\HasKafkaKey;

class ProcessOrder implements ShouldQueue, HasKafkaKey
{
    public function __construct(private readonly int $orderId) {}

    public function kafkaKey(): string
    {
        return (string) $this->orderId;
    }
}
```

With the default `murmur2_random` partitioner, jobs with the same non-empty key are routed to the same partition. Run exactly one worker for each partition, as shown in [Running workers](#running-workers), to preserve processing order for that key. The `random` partitioner does not provide this guarantee.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_QUEUE` | `default` | Kafka topic used as the default Laravel queue. |
| `KAFKA_CONSUMER_GROUP_ID` | `laravel_queue` | Kafka consumer group ID. |
| `KAFKA_CONSUMER_PARTITION` | `0` | Partition consumed by this worker. |
| `KAFKA_PRODUCER_PARTITIONER` | `murmur2_random` | Partitioner used when producing jobs. |
| `KAFKA_STOP_CONSUME_ON_EMPTY` | `false` | Stop the low-level partition consumer after an empty result or partition EOF. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated bootstrap broker addresses. |
| `KAFKA_ERROR_SLEEP` | `5` | Seconds to wait after a connection error; set to `false` to throw immediately. |
| `KAFKA_SASL_ENABLE` | `false` | Enable SASL authentication for producers and consumers. |
| `KAFKA_SASL_SECURITY_PROTOCOL` | `SASL_SSL` | Security protocol: `SSL`, `PLAINTEXT`, `SASL_PLAINTEXT`, or `SASL_SSL`. |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. |
| `KAFKA_SSL_CA_LOCATION` | empty | Path to the CA certificate file or directory used to verify brokers. |
| `KAFKA_SASL_PLAIN_USERNAME` | empty | SASL username. |
| `KAFKA_SASL_PLAIN_PASSWORD` | empty | SASL password. |
| `KAFKA_AUTO_COMMIT` | `true` | Enable librdkafka's periodic automatic offset commits. |
| `KAFKA_AUTO_COMMIT_INTERVAL_MS` | `5000` | Interval between automatic offset commits; the low-level consumer accepts values from `10`. |
| `KAFKA_AUTO_RESET` | `earliest` | Offset reset policy when no valid committed offset exists. |
| `KAFKA_TIMEOUT_MS` | `1000` | Timeout in milliseconds for Kafka operations. |

For near-immediate offset commits and more responsive `queue:monitor` output, use the minimum supported interval:

```bash
KAFKA_AUTO_COMMIT_INTERVAL_MS=10 php artisan queue:work
```

Commits remain asynchronous and interval-based; this does not make each job perform a synchronous Kafka commit.

### Producer partitioners

- `random`: distribute jobs randomly.
- `consistent`: use a CRC32 hash; null keys use a single partition.
- `consistent_random`: use a CRC32 hash; null keys are distributed randomly.
- `murmur2`: use the Java-compatible Murmur2 hash; null keys use a single partition.
- `murmur2_random`: use the Java-compatible Murmur2 hash; null keys are distributed randomly.
- `fnv1a`: use the FNV-1a hash; null keys use a single partition.
- `fnv1a_random`: use the FNV-1a hash; null keys are distributed randomly.

### Offset reset policies

- `earliest`: start at the earliest available offset.
- `latest`: start after the newest available offset. Existing jobs are skipped when no committed offset exists.
- `none`: fail when no valid committed offset exists.

## Compatibility

| PHP | Laravel | php-rdkafka | librdkafka |
| --- | --- | --- | --- |
| 8.2+ | 10–13 | 6.x | 2.x |

Laravel 12.x and 13.x have been tested directly.

## Testing

Run the tests with:

```bash
vendor/bin/phpunit
```

## Acknowledgements

This package is a maintained fork of [rapideinternet/laravel-queue-kafka](https://github.com/rapideinternet/laravel-queue-kafka).

## Contributing

Bug reports and pull requests are welcome. Include the affected package version and enough information to reproduce the problem.
