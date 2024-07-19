![NATS](https://raw.githubusercontent.com/nats-io/nats.net.v1/main/documentation/large-logo.png)

# TLS Variations

This project is simply some example code to jump start setting up TLS in the client.
There are also unit tests and corresponding configuration files which may be of use.

## Using a Reverse Proxy

In a reverse proxy configuration, the client connects securely to the reverse proxy
and the proxy may connect securely or insecurely to the server.

If the proxy connects securely to the server, 
then there is nothing special required to do at all.

But most commonly, the proxy connects insecurely to the server. 
This is where server configuration comes into play. 
You will need to configure the server like so:

```
tls {}
allow_non_tls: true
```

Before this, the client would not connect 
because the server was not requiring tls for the proxy, 
but the client was configured as secure because it was connecting securely to the proxy. 
The client thought that this was a mismatch and would not connect, 
essentially failing fast instead of waiting for the server to reject the connection attempt.

The latest version of the client is able to recognize this server configuration 
and understands that it's okay to connect securely to the proxy regardless of the 
server configuration.

You just have to make sure you can properly connect securely to the proxy 
and that's where the code in this sample comes in.
