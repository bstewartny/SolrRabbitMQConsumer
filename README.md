SolrRabbitMQConsumer
====================

Custom request handler for SOLR for consuming documents from RabbitMQ.

Consumes XML documents from RabbitMQ and add to local index core.

In solrconfig.xml:

<requestHandler name="solrRabbitMQConsumer" class="SolrRabbitMQConsumer">
  <lst name="queueConfig">
    <!-- Name of queue to consume documents from -->
    <str name="queueName">solr</str>
    <!-- URI specifying credentials, RabbitMQ server, port and vhost -->
    <str name="queueURI">amqp://solr:solr@127.0.0.1:5672/solr</str>
  </lst>
</requestHandler>
