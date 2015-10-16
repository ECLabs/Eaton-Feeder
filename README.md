# Eaton-Feeder
Application(s) for reading information from external feeds, initially the Indeed API for job descriptions
### Installation

You'll need to follow the instructions here for installing go: https://golang.org/doc/install

You will also need kafka servers running for the program to interact with.  You can get kafka quickly started by following the instructions here: http://kafka.apache.org/documentation.html#quickstart

Git is also required to pull down the code.

Once you have everything installed (with your GOPATH set), just run:

```sh
$ go get github.com/ECLabs/Eaton-Feeder
```

Here's an example script to run the program as a producer:

```
#!/bin/sh

KAFKA_TOPIC=myTopic
KAFKA_SERVERS=127.0.0.1:9092
INDEED_PUBLISHER_ID=123456789

export KAFKA_TOPIC
export KAFKA_SERVERS
export INDEED_PUBLISHER_ID
# a -1 interval disables the builtin polling capability and once the 
# program has finished pulling all results from the indeed API it
# will terminate.
$GOPATH/bin/Eaton-Feeder -produce=true -interval=-1
```

The program operates sightly differently when it's a consumer.  It will stay running indefinitely since the client needs to always be ready for new messages.  To ensure that it is always running, you can install monit and have a script similar to the following to keep try of the process id:

```
#!/bin/sh
AWS_ACCESS_KEY_ID=my_access_key
AWS_SECRET_ACCESS_KEY=my_secret_key
KAFKA_TOPIC=myTopic
KAFKA_SERVERS=127.0.0.1:9092

export AWS_SECRET_ACCESS_KEY
export AWS_ACCESS_KEY_ID
export KAFKA_SERVERS
export KAFKA_TOPIC

$GOPATH/bin/Eaton-Feeder -offset=newest -consume=true  & echo "$!" > eatonfeeder.pid

```