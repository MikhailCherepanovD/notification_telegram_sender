# Notification telegram sender
Notification telegram sender is part of [Flight ticket tracking application](https://github.com/MikhailCherepanovD/notification_service).

##  Functionality
The server:

* Implements simple logic of a Telegram bot that saved chat_id in Redis key-value storage to retain chat_id after a container restart;

* Is a consumer of Apache Kafka, prepares and sends received messages in telegram.
