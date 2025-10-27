# Cluster Sharding sample

The KillrWeather sample illustrates how to use [Akka Cluster Sharding](http://doc.akka.io/libraries/akka-core/current/scala/typed/cluster-sharding.html) in Scala.
It also shows the basic usage of [Akka HTTP](https://doc.akka.io/libraries/akka-http/current/index.html).

To try this example locally, download the sources files with [akka-sample-sharding-scala.zip](https://doc.akka.io/libraries/akka-core/current//attachments/akka-sample-sharding-scala.zip).

The sample consists of two applications, each a separate maven submodule:

 * *killrweather* - A distributed Akka cluster that shards weather stations, each keeping a set of recorded 
    data points and allowing for local querying of the records. The cluster app has a HTTP endpoint for recording
    and querying the data per weather station. 

 * *killrweather-fog* - A client that periodically submits random weather measurements for a set of stations to a 
    running cluster.

Let's start going through the implementation of the cluster!
 
## KillrWeather

Open [KillrWeather.scala](killrweather/src/main/scala/sample/killrweather/KillrWeather.scala).
This program starts an `ActorSystem` which joins the cluster through configuration, and starts a `Guardian` actor for the system. 

### Guardian

The [Guardian.scala](killrweather/src/main/scala/sample/killrweather/Guardian.scala) bootstraps the application to shard 
`WeatherStation` actors across the cluster nodes.

Setting up sharding with the entity is done in [WeatherStation.scala](killrweather/src/main/scala/sample/killrweather/WeatherStation.scala#L34).
Keeping the setup logic together with the sharded actor and then calling it from the bootstrap logic of the application is a common pattern to structure sharded entities. 

### WeatherStation - sharded data by id
 
Each sharded `WeatherStation` actor has a set of recorded data points for a station identifier.

It receives that data stream from remote devices via the HTTP endpoint. Each `WeatherStation` and also respond to queries about it's recorded set of data such as:
 
 * current
 * averages 
 * high/low 

### Receiving edge device data by data type

The [WeatherHttpServer](killrweather/src/main/scala/sample/killrweather/WeatherHttpServer.scala) is started with 
 [WeatherRoutes](killrweather/src/main/scala/sample/killrweather/WeatherRoutes.scala)
 to receive and unmarshall data from remote devices by station ID to allow 
  querying. The HTTP port of each node is chosen from the port used for Akka Remoting plus 10000, so for a node running 		 querying. To interact with the sharded entities it uses the [`EntityRef` API](killrweather/src/main/scala/sample/killrweather/WeatherRoutes.scala#L26).
 
The HTTP port of each node is chosen from the port used for Akka Remoting plus 10000, so for a node running 
on port 2525 the HTTP port will be 12525.

### Configuration

This application is configured in [killrweather/src/main/resources/application.conf](killrweather/src/main/resources/application.conf)
Before running, first make sure the correct settings are set for your system, as described in the akka-sample-cluster tutorial.

## Fog Network

Open [Fog.scala](killrweather-fog/src/main/scala/sample/killrweather/fog/Fog.scala).

`Fog` is the program simulating many weather stations and their devices which read and report data to clusters.
The name refers to [Fog computing](https://en.wikipedia.org/wiki/Fog_computing) with edges - the remote weather station
nodes and their device edges.

This example starts simply with one actor per station and just reports one data type, temperature. In the wild, other devices would include:
pressure, precipitation, wind speed, wind direction, sky condition and dewpoint.
`Fog` starts the number of weather stations configured in [killrweather-fog/src/main/resources/application.conf](killrweather-fog/src/main/resources/application.conf) 
upon boot.

### Weather stations and devices

Each [WeatherStation](killrweather-fog/src/main/scala/sample/killrweather/fog/WeatherStation.scala) is run on a task to trigger scheduled data sampling.
These samples are timestamped and sent to the cluster over HTTP using [Akka HTTP](https://doc.akka.io/libraries/akka-http/current/index.html). 

## Akka HTTP example

Within KillrWeather are two simple sides to an HTTP equation.

**Client**

* [WeatherStation](killrweather-fog/src/main/scala/sample/killrweather/fog/WeatherStation.scala) - HTTP data marshall and send

**Server**

* [WeatherHttpServer](killrweather/src/main/scala/sample/killrweather/WeatherHttpServer.scala) - HTTP server
* [WeatherRoutes](killrweather/src/main/scala/sample/killrweather/WeatherRoutes.scala) - HTTP routes receiver which will unmarshall and pass on the data


Both parts of the application uses Spray-JSON for marshalling and unmarshalling objects to and from JSON.

## Running the samples

### The KillrWeather Cluster

There are two ways to run the cluster, the first is a convenience quick start.

#### A simple three node cluster in the same JVM

The simplest way to run this sample is to run this in a terminal, if not already started:
   
    sbt killrweather/run
   
This command starts three (the default) `KillrWeather` actor systems (a three node cluster) in the same JVM process. 

#### Dynamic WeatherServer ports

In the log snippet below, note the dynamic weather ports opened by each KillrWeather node's `WeatherServer` for weather stations to connect to. 
The number of ports are by default three, for the minimum three node cluster. You can start more cluster nodes, so these are dynamic to avoid bind errors. 

```
[2020-01-16 13:44:58,842] [INFO] [] [akka.actor.typed.ActorSystem] [KillrWeather-akka.actor.default-dispatcher-3] [] - WeatherServer online at http://127.0.0.1:12553/
[2020-01-16 13:44:58,842] [INFO] [] [akka.actor.typed.ActorSystem] [KillrWeather-akka.actor.default-dispatcher-19] [] - WeatherServer online at http://127.0.0.1:53937/
[2020-01-16 13:44:58,843] [INFO] [] [akka.actor.typed.ActorSystem] [KillrWeather-akka.actor.default-dispatcher-15] [] - WeatherServer online at http://127.0.0.1:12554/
```

#### A three node cluster in separate JVMs

It is more interesting to run them in separate processes. Stop the application and then open three terminal windows.
In the first terminal window, start the first seed node with the following command:

    sbt "killrweather/runMain sample.killrweather.KillrWeather 2553"

2553 corresponds to the port of the first seed-nodes element in the configuration. In the log output you see that the cluster node has been started and changed status to 'Up'.

You'll see a log message when a `WeatherStation` sends a message to record the current temperature, and for each of those you'll see a log message from the `WeatherRoutes` showing the action taken and the new average temperature.

In the second terminal window, start the second seed node with the following command:

    sbt "killrweather/runMain sample.killrweather.KillrWeather 2554"

2554 corresponds to the port of the second seed-nodes element in the configuration. In the log output you see that the cluster node has been started and joins the other seed node and becomes a member of the cluster. Its status changed to 'Up'. Switch over to the first terminal window and see in the log output that the member joined.

Some of the temperature aggregators that were originally on the `ActorSystem` on port 2553 will be migrated to the newly joined `ActorSystem` on port 2554. The migration is straightforward: the old actor is stopped and a fresh actor is started on the newly created `ActorSystem`. Notice this means the average is reset: if you want your state to be persisted you'll need to take care of this yourself. For this reason Cluster Sharding and Akka Persistence are such a popular combination.

Start another node in the third terminal window with the following command:

    sbt "killrweather/runMain sample.killrweather.KillrWeather 0"

Now you don't need to specify the port number, 0 means that it will use a random available port. It joins one of the configured seed nodes.
Look at the log output in the different terminal windows.

Start even more nodes in the same way, if you like.

#### Dynamic WeatherServer port

Each node's log will show its dynamic weather port opened for weather stations to connect to. 
```
[2020-01-16 13:44:58,842] [INFO] [] [akka.actor.typed.ActorSystem] [KillrWeather-akka.actor.default-dispatcher-3] [] - WeatherServer online at http://127.0.0.1:12553/
```


### Interacting with the HTTP endpoint manually

With the cluster running you can interact with the HTTP endpoint using raw HTTP, for example with `curl`.

Record data for station 62:

```
curl -XPOST http://localhost:12553/weather/62 -H "Content-Type: application/json" --data '{"eventTime": 1579106781, "dataType": "temperature", "value": 10.3}'
```

Query average temperature for station 62:

```
curl "http://localhost:12553/weather/62?type=temperature&function=average"
```

### The Fog Network
 
In a new terminal start the `Fog`, (see [Fog computing](https://en.wikipedia.org/wiki/Fog_computing))

Each simulated remote weather station will attempt to connect to one of the round-robin assigned ports for Fog networking over HTTP.

The fog application, when run without parameters, will expect the cluster to have been started without parameters as well so that the HTTP ports if has bound are predictable.
If you have started cluster nodes manually providing port numbers you will have to do the same with the fog app or else it will not be able to find the endpoints.   

For example:
 
    sbt "killrweather-fog/runMain sample.killrweather.fog.Fog 8081 8033 8056"
     
### Shutting down

Shut down one of the nodes by pressing 'ctrl-c' in one of the terminal windows. The other nodes will detect the failure after a while, which you can see in the log output in the other terminals.

---

The Akka family of projects is managed by teams at Lightbend with help from the community.

License
-------

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).
