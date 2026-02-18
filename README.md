Kafka Queue driver for Laravel 12+ and rdkafka 2.x
======================
[![Latest Stable Version](https://poser.pugx.org/rapide/laravel-queue-kafka/v/stable?format=flat-square)](https://packagist.org/packages/rapide/laravel-queue-kafka)
[![Build Status](https://travis-ci.org/rapideinternet/laravel-queue-kafka.svg?branch=master)](https://travis-ci.org/rapideinternet/laravel-queue-kafka)
[![Total Downloads](https://poser.pugx.org/rapide/laravel-queue-kafka/downloads?format=flat-square)](https://packagist.org/packages/rapide/laravel-queue-kafka)
[![StyleCI](https://styleci.io/repos/99249783/shield)](https://styleci.io/repos/99249783)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)](LICENSE)

#### Installation

1. Install [librdkafka c library](https://github.com/edenhill/librdkafka)

    ```bash
    $ cd /tmp
    $ mkdir librdkafka
    $ cd librdkafka
    $ git clone https://github.com/edenhill/librdkafka.git .
    $ ./configure
    $ make
    $ make install
    ```
2. Install the [php-rdkafka](https://github.com/arnaud-lb/php-rdkafka) PECL extension

    ```bash
    $ pecl install rdkafka
    ```
    
3. a. Add the following to your php.ini file to enable the php-rdkafka extension
    `extension=rdkafka.so`
    
   b. Check if rdkafka is installed  
   __Note:__ If you want to run this on php-fpm restart your php-fpm first.
   ```bash    
       php -i | grep rdkafka
   ```
   Your output should look something like this
   
       rdkafka
       rdkafka support => enabled
       librdkafka version (runtime) => 2.12.1
       librdkafka version (build) => 2.12.1.255
 
4. Install this package via composer using:
    ```bash  
	    composer require rapide/laravel-queue-kafka
    ```
5. If you are using Lumen, put this in `bootstrap/app.php`:
    ```php
        $app->register(Rapide\LaravelQueueKafka\LumenQueueKafkaServiceProvider::class);
    ```
6. You can also publish queue-kafka.php config:
    ```bash
        php artisan vendor:publish --tag queue-kafka-config
    ```
7. Add these properties to `.env` with proper values:
     
		QUEUE_DRIVER=kafka
     
8. If you want to run a worker for a specific consumer group
    ```bash
        export KAFKA_CONSUMER_GROUP_ID="group2" && php artisan queue:work --sleep=3
    ```
9. For run parallel in N partitions invoke N workers with:
    ```bash
        KAFKA_CONSUMER_PARTITION=0 php artisan queue:work
        KAFKA_CONSUMER_PARTITION=1 php artisan queue:work
        ...
    ```
10. <span style="color: red">--tries not working</span> with this driver. Make sure you catch all exceptions and enqueue again in your job if needed in your job<br>
Queue::later() also not working

#### Usage

Once you completed the configuration you can use Laravel Queue API. If you used other queue drivers you do not need to change anything else. If you do not know how to use Queue API, please refer to the official Laravel documentation: http://laravel.com/docs/queues

#### Supported environment variables
`KAFKA_QUEUE` - default queue(topic) name

`KAFKA_CONSUMER_GROUP_ID` - kafka consumer group, default = 'laravel_queue'

`KAFKA_CONSUMER_PARTITION` - kafka partition for consume, default = 0

`KAFKA_PRODUCER_PARTITIONER` - Producer partitioner algorithm, default = 'murmur2_random'

###### Can be:
1. random - random distribution, consistent - CRC32 hash of key (Empty and NULL keys are mapped to single partition),
2. consistent_random - CRC32 hash of key (Empty and NULL keys are randomly partitioned),
3. murmur2 - Java Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition),
4. murmur2_random - Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned.
        This is functionally equivalent to the default partitioner in the Java Producer.),
5. fnv1a - FNV-1a hash of key (NULL keys are mapped to single partition),
6. fnv1a_random - FNV-1a hash of key (NULL keys are randomly partitioned).

`KAFKA_BROKERS` - Comma-separated list of Kafka broker addresses the client will initially connect to, default = localhost:9092

`KAFKA_ERROR_SLEEP` - Determine the number of seconds to sleep if there's an error communicating with kafka or false|null, default = 5

`KAFKA_SASL_ENABLE` - Enable SASL authentication. if false other SASL config does not matter, default = false

`KAFKA_SASL_SECURITY_PROTOCOL` - One of SSL, PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, default = SASL_SSL

`KAFKA_SASL_MECHANISM` - One of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, default = SCRAM-SHA-512

`KAFKA_SSL_CA_LOCATION` - File or directory path to CA certificate(s) for verifying the broker's key, default empty

`KAFKA_SASL_PLAIN_USERNAME` - no default

`KAFKA_SASL_PLAIN_PASSWORD` - no default

`KAFKA_AUTO_COMMIT` - The property enable.auto.commit is set to true by default, and Kafka commits the current offset back to the Kafka broker at a specified interval, default = true. <span style="color:red">In most cases you don't need to touch this</span>

`KAFKA_AUTO_RESET` - What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):

1. earliest: automatically reset the offset to the earliest offset
2. latest: automatically reset the offset to the latest offset
3. none: throw exception to the consumer if no previous offset is found for the consumer's group
4. anything else: throw exception to the consumer,
default = 'earliest'. If you change this <span style="color:red">first producer messages may be ignored by consumer</span>

`KAFKA_TIMEOUT_MS` - Timeout in ms for most operations, default = 1000


#### Testing

Run the tests with:

``` bash
vendor/bin/phpunit
```

#### Acknowledgement 

This library is inspired by [laravel-queue-rabbitmq](https://github.com/vyuldashev/laravel-queue-rabbitmq) by vyuldashev.
And the Kafka implementations by [Superbalist](https://github.com/Superbalist/php-pubsub-kafka) be sure to check those out. 

#### Contribution

You can contribute to this package by discovering bugs and opening issues. Please, add to which version of package you create pull request or issue.

#### Supported versions of Laravel 

Tested on: [12.0]
