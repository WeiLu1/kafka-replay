## Kafka Replay
This is a prototype service which listens to error topics in kafka which are used when messages fail to send to their
main topic and are re-routed. Then they will be persisted into a mongodb database.

Use the docker-compose file provided by either [kafka-failure](https://github.com/anojkunes/kafka-retry-service)
or [kafka-replay](https://github.com/WeiLu1/kafka-replay) to spin up the docker network.\
```bash
docker compose up -d
```
Run both the `kafka-replay` and `kafka-failure` services together.


## Dependencies
- Java 17
- Gradle
- Kafka
- MongoDB



## Endpoints
### kafka-replay

Service will be hosted on localhost:8090.\
All endpoints begin with: `internal/v1/messages/`

- Get message using ID\
`GET`: `internal/v1/messages/{id}`
  

- Get all messages from a topic\
`GET`: `internal/v1/messages/topic/{topic}`
  

- Get all messages\
`GET`: `internal/v1/messages/`


- Get all topics\
`GET`: `internal/v1/messages/topics`


- Retry sending a message using ID\
`POST`: `internal/v1/messages/{id}`
  

- Retry sending all messages in a topic\
`POST`: `internal/v1/messages/topic/{topic}`


- Delete a message\
`DELETE`: `internal/v1/messages/{id}`
  

- Delete all messages from a topic\
`DELETE`: `internal/v1/messages/topic/{topic}`
  

Responses will be of the form:
```json
{
    "id": "<UUID>",
    "topic": "<Topic_Name>_RETRY",
    "key": "<UUID",
    "payload": "{...}",
    "exceptionStacktrace": "..."
}
```

### kafka-failure

Service will be hosted on localhost:8080.\
All endpoints begin with: `kafka-failure/v1/internal`

- Send a message\
`POST`: `kafka-failure/v1/internal`
  
Request Body example:
```json
{
    "hello": "there"
}
```