MVSC Content Server
====================

This is a set of two applications which are designed to transfer files between
two computers with minimal overhead.

Assumptions
------------

* Any computer on the local network is trusted. (`content-server.py`)
* RabbitMQ has a user "test" with password "test" that has global permissions.
This needs to be changed before it is sent out into the wild, but it serves
for testing purposes. (`fs-watcher.py`)



Content Server
----------------

The server runs on the computer that is producing the files.

`content-server.py`

This is a very dangerous script that allows anyone on the same network to access any
file available to the default user. This can be changed to restrict it to a
certain path, but for the sake of expediency I have omitted that feature.

It is, however, very efficient, transferring files directly over TCP. If the
performance ever becomes a problem, this component can trivially (as it is now)
be rewritten in C.

`fs-watcher.py`

This script is useful part of the server. It is invoked as such:

`fs-watcher.py directory_to_watch rabbitmq_host`

Whenever a file is created in `directory_to_watch`, it sends a message to the
rabbitmq server running on `rabbitmq_host`, which is retrieved by the client.

The client then downloads the file by using the service provided by
`content-server.py`.

Content Client
---------------

The client runs on the computer that is collecting data from any number of
content servers. The RabbitMQ server can run on the client, as it will in our
case, but it does not have to. It is invoked as such:

`content-client.py rabbitmq-host save-directory`
