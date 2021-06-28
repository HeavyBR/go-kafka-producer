# go-kafka-producer

`go run main.go -topic topic_name -message "some cool message"`

# Setting up OS

We are using confluentic official go kafka package, so this package uses kafka *librdkafka* (C client) internally and because of that you will need to install some deps on your OS

For ubuntu users

`sudo apt-get install build-essential pkg-config git`

# Setting up kafka

`docker-compose up -d`

*kafdrop* will run on port `29000` and *kafka server* will run on `9092`

