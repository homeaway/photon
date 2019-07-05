Photon [![Build Status][build-icon]][build-link]
==============================
                        
[build-icon]: https://travis-ci.org/homeaway/photon.svg?branch=master
[build-link]: https://travis-ci.org/homeaway/photon

An event sourcing framework built on top of Apache Cassandra allowing horizontally scalable throughput and native cross data-center
replication.

### Benefits of Photon
1. Exactly one delivery.
2. Horizontal Scalability.
3. Backed by Apache Cassandra.
4. Native cross data center replication.

## Getting Started
Once you have setup a Cassandra cluster then deploy the necessary schema located [here link](https://github.homeawaycorp.com/coe-data-tools/photon/tree/master/src/main/resources/cassandra).

Make sure you include the Photon dependency below in your code
```xml
<dependencies>
    <dependency>
        <groupId>com.homeaway.datatools.photon</groupId>
        <artifactId>photon</artifactId>
        <version>0.1.8</version>
    </dependency>
</dependencies>
```
In order to create a producer or consumer, you will first need to define the properties that will be used to
build the producer or consumer. Below an example of properties using the default Cassandra driver and the
default Json serializer.

### Producer
```java
Properties properties = new Properties();
properties.put(PHOTON_DRIVER_CLASS, "com.homeaway.datatools.photon.driver.CassandraPhotonDriver");
properties.put(PHOTON_SERIALIZER_CLASS, "com.homeaway.datatools.photon.serialization.JsonPhotonSerializer");
properties.put(SESSION_CONTACT_POINTS, "XX.XXX.X.XX,XX.XXX.XX.XXX,XX.XXX.XX.XXX");
properties.put(SESSION_USER_NAME,"{username}");
properties.put(SESSION_PASSWORD,"{password}");
properties.put(SESSION_KEYSPACE,"{keyspace}");

BeamProducer producer = Producers.newProducer(properties);
```

### Consumer
```java
Properties properties = new Properties();
properties.put(CONSUMER_TYPE, "SINGLE_REGION");
properties.put(PHOTON_DRIVER_CLASS, "com.homeaway.datatools.photon.driver.CassandraPhotonDriver");
properties.put(PHOTON_DESERIALIZER_CLASS, "com.homeaway.datatools.photon.serialization.JsonPhotonDeserializer");
properties.put(SESSION_CONTACT_POINTS, "XX.XXX.X.XX,XX.XXX.XX.XXX,XX.XXX.XX.XXX");
properties.put(SESSION_USER_NAME,"{username}");
properties.put(SESSION_PASSWORD,"{password}");
properties.put(SESSION_KEYSPACE,"{keyspace}");

ConsumerFactory factory = Consumers.newConsumerFactory(properties);
```

## Features

### Producer
##### Synchronous and Asynchronous Writes using NIO channels (no local threads)
###### Synchronous
```java
PayloadObject obj = new PayloadObject();
producer.writeMessageToBeam("stream.name.here", "message.key.here", obj);
```
###### Asynchronous
```java
PayloadObject obj = new PayloadObject();
BeamFuture future = producer.writeMessageToBeamAsync("stream.name.here", "message.key.here", obj);
if (future.get())
{
	//Successful write
}
```
##### Configurable Time To Live per Event
These examples will write events that will be automatically deleted in 30 days.
###### Synchronous
```java
Duration ttl = Duration.ofDays(30);
PayloadObject obj = new PayloadObject();
producer.writeMessageToBeam("stream.name.here", "message.key.here", obj, ttl);
```
###### Asynchronous
```java
Duration ttl = Duration.ofDays(30);
PayloadObject obj = new PayloadObject();
BeamFuture future = producer.writeMessageToBeamAsync("stream.name.here", "message.key.here", obj, ttl);
if (future.get())
{
	//Successful write
}
```
##### Future event creation.
These examples will write an event that will be received 5 days from now.
###### Synchronous
```java
Instant writeTime = Instant.now().plus(5, ChronoUnit.DAYS);
PayloadObject obj = new PayloadObject();
producer.writeMessageToBeam("stream.name.here", "message.key.here", obj, writeTime);
```
###### Asynchronous
```java
Instant writeTime = Instant.now().plus(5, ChronoUnit.DAYS);
PayloadObject obj = new PayloadObject();
BeamFuture future = producer.writeMessageToBeamAsync("stream.name.here", "message.key.here", obj, writeTime);
if (future.get())
{
	//Successful write
}
```
### Consumer
##### Standard consumer handles events as they are received with user defined event handler.
Below is an example of how to setup and standard consumer that will beginning reading from the last point where this client read to.
```java
PhotonConsumer consumer = consumerFactory.getPhotonConsumer();

consumer.putBeamForProcessing("ClientNameHere", "stream.name.here",
new PhotonMessageHandler() {

  @Override
  public void handleMessage(PhotonMessage message) {
  	//Perform some action base on the message received
  }

  @Override
  public void handleException(BeamException beamException) {
  	//Handle exception thrown by the message
  }

  @Override
  public void handleStaleMessage(PhotonMessage message) {
  	//If a message arrives here then it is because the it arrived out of order
  }
}, PhotonBeamReaderOffsetType.FROM_CURRENT);

try {
	consumer.start()
} catch (Exception e)
{

}
```
##### Events are processed in the order they are written and provides mechanism for detecting events that arrive late.
##### Configurable polling interval to suit different workloads and SLA demands
This example sets the polling interval to 500 milliseconds.
```java
PhotonConsumer consumer = consumerFactory.getPhotonConsumer();
consumer.setPollingInterval(500L);

try {
	consumer.start()
} catch (Exception e)
{

}
```
##### Configurable start offset so that you can begin consuming from an exact point in time
There are 3 offset options:

The example below would read from the beginning of a stream:
```java
PhotonConsumer consumer = consumerFactory.getPhotonConsumer();

consumer.putBeamForProcessing("ClientNameHere", "stream.name.here",
new PhotonMessageHandler() ..., PhotonBeamReaderOffsetType.FROM_BEGINNING);

try {
	consumer.start()
} catch (Exception e)
{

}
```
The example below will read from the last point where this client left off
```java
PhotonConsumer consumer = consumerFactory.getPhotonConsumer();

consumer.putBeamForProcessing("ClientNameHere", "stream.name.here",
new PhotonMessageHandler() ..., PhotonBeamReaderOffsetType.FROM_CURRENT);

try {
	consumer.start()
} catch (Exception e)
{

}
```
The example below will read all messages starting from 3 hours ago
```java
PhotonConsumer consumer = consumerFactory.getPhotonConsumer();

consumer.putBeamForProcessing("ClientNameHere", "stream.name.here",
new PhotonMessageHandler() ..., PhotonBeamReaderOffsetType.FROM_OFFSET,
Instant.now().minus(3, ChronoUnit.HOURS));

try {
	consumer.start()
} catch (Exception e)
{

}
```
The example below will read all messages starting from the Instant returned by the provided BiFunction
```java
PhotonConsumer consumer = consumerFactory.getPhotonConsumer();

consumer.putBeamForProcessing("ClientNameHere", "stream.name.here",
new PhotonMessageHandler() ..., PhotonBeamReaderOffsetType.FROM_OFFSET,
(clientName, beamName) -> {
	//Some logic here to determine determine where to beginning reading from and returns    	//and Instant
});

try {
	consumer.start()
} catch (Exception e)
{

}
```
##### Async consumer for processing events on multiple threads while still ensuring order with guaranteed delivery
The following example will create and Async processor that will map ```PhotonMessage``` to ```Widget``` and then execute the provided ```Consumer<Widget>``` on separate threads. The events will be processed in order by stream name and message key and there will be no limit to the number of events that can be container in memory at any given time (This can be dangerous in high throughput situations where there are memory constraints).
```java
AsyncPhotonConsumer<Widget> asyncConsumer = consumerFactory.getAsyncPhotonConsumer();

asyncConsumer.putBeamForProcessing("ClientNameHere", "stream.name.here",
	(photonMessage) -> {
    	Widget widget = new Widget();
        //Some logic to mape the PhotonMessage to a Widget
        return widget
    },
    (widget) -> {
    	//Some logic that is performed on the provided widget
    },
    PhotonBeamReaderOffsetType.FROM_CURRENT);
try {
	asyncConsumer.start()
} catch (Exception e)
{

}
```
##### Configurable memory utilization for Async consumer (limit the number of events that are kept in memory at any give time).
The following example is the same as about but it will limit the number of events that can be held in memory to 500.
```java
AsyncPhotonConsumer<Widget> asyncConsumer = consumerFactory.getAsyncPhotonConsumer(500);

asyncConsumer.putBeamForProcessing("ClientNameHere", "stream.name.here",
	(photonMessage) -> {
    	Widget widget = new Widget();
        //Some logic to mape the PhotonMessage to a Widget
        return widget
    },
    (widget) -> {
    	//Some logic that is performed on the provided widget
    },
    PhotonBeamReaderOffsetType.FROM_CURRENT);
try {
	asyncConsumer.start()
} catch (Exception e)
{

}
```