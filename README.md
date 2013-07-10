SolrRabbitMQConsumer
====================

Custom request handler for Apache SOLR for consuming documents from RabbitMQ.

Consumes XML documents from RabbitMQ and adds to local index core.

Solr Configuration
------------------

* Add SolrRabbitMQConsumer.jar and required RabbitMQ dependencies inside /lib folder for your Solr instance.
* Add custom request handler configuration to solrconfig.xml: 


```xml
<requestHandler name="solrRabbitMQConsumer" class="SolrRabbitMQConsumer">
  <lst name="queueConfig">
    <!-- Name of queue to consume documents from -->
    <str name="queueName">solr</str>
    <!-- URI specifying credentials, RabbitMQ server, port and vhost -->
    <str name="queueURI">amqp://solr:solr@127.0.0.1:5672/solr</str>
  </lst>
</requestHandler>
```

Document format
---------------

Currently, due to supporting a legacy (non-SOLR) indexing system, XML documents must be in the following format:

```xml
<doc>
  <f n="fieldname"><v>field value</v></f>
  <f n="fieldname"><v>field value</v></f>
  ...
</doc>
```
