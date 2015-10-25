# Eaton-Feeder-Http
Basic http/websocket server for consuming logs generated by either the consumer or producer version of the Eaton-Feeder.
### Installation

You'll need to follow the instructions here for installing go: https://golang.org/doc/install

You will also need kafka servers running for the program to interact with.  You can get kafka quickly started by following the instructions here: http://kafka.apache.org/documentation.html#quickstart

Git is also required to pull down the code.

Once you have everything installed (with your GOPATH set), just run:

```sh
$ go get github.com/ECLabs/Eaton-Feeder/http
```

Here's an example script to run the program:

```
#!/bin/sh

# Override the following to change which topic/servers to consume messages from.
# The following are the default values:
#   KAFKA_LOGGER_TOPIC = logs
#       This is the topic that the Eaton-Feeder producer or consumer are sending logging messages to in JSON format.
#   KAFKA_SERVERS = 127.0.0.1:9092
#       This is the comma delmited listing of all kafka servers to send messages to.
KAFKA_SERVERS=127.0.0.1:9092
KAFKA_LOGGER_TOPIC=logs

export KAFKA_LOGGER_TOPIC
export KAFKA_SERVERS

# The application requires an 'assets' directory to be located
# in the current working directory and the go install should 
# by default pull in the assets directory in git.  
cd $GOPATH/src/github.com/ECLabs/Eaton-Feeder/http

$GOPATH/bin/http & echo "$!" > http.pid

# You should then be able to navigate to http://localhost:5000 and
# see incoming logs from a running Eaton-Feeder producer or consumer.
```