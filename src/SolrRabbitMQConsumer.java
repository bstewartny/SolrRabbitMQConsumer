/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import com.rabbitmq.client.Channel;
import java.net.URL;
import java.util.HashMap;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean.Category;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 *
 * @author bstewart
 */
public class SolrRabbitMQConsumer implements SolrRequestHandler,SolrCoreAware
{
    RabbitMQDocumentConsumerTask worker=null;
    public String queueName="solr"; 
    public String queueURI="amqp://solr:solr@localhost:5672/solr";
    
    @Override
    public void inform(SolrCore sc) {
        if(worker==null)
        {
            worker=new RabbitMQDocumentConsumerTask(queueURI,queueName,sc);
            worker.start();
        }
    }

    private static class RabbitMQDocumentConsumerTask extends Thread
    {
        Connection conn=null;
        Channel channel=null;
        QueueingConsumer consumer=null;
        SolrCore core;
        String queueName;
        String queueURI;
        
        public RabbitMQDocumentConsumerTask(String queueURI,String queueName,SolrCore core)
        {
            this.core=core;
            this.queueURI=queueURI;
            this.queueName=queueName;
        }
        
        private Document bytesToXml(byte[] xml) {
            try
            {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                return builder.parse(new ByteArrayInputStream(xml));
            }
            catch(Exception e)
            {
                e.printStackTrace();
                return null;
            }
        }
        
        private boolean createConsumer()
        {
            ConnectionFactory factory = new ConnectionFactory();
            try
            {
                factory.setUri(queueURI);
            }
            catch(Exception k)
            {
                k.printStackTrace();
                return false;
            }
             
            try
            {
                conn= factory.newConnection();
            }
            catch(IOException e)
            {
                e.printStackTrace();
                return false;
            }
             
            if(conn!=null)
            {
                try
                {
                    channel=conn.createChannel();
                    consumer=new QueueingConsumer(channel);
                    channel.basicConsume(queueName, consumer);
                }
                catch(IOException e)
                {
                    e.printStackTrace();
                    return false;
                }
            }
            return consumer!=null;
        }
         
        private Document nextDocument() throws InterruptedException
        {
            QueueingConsumer.Delivery delivery=consumer.nextDelivery();
            return bytesToXml(delivery.getBody());
        }
        
        private String getStringFromXmlDoc(Document doc)
        {
            try
            {
                TransformerFactory tf = TransformerFactory.newInstance();
                Transformer transformer = tf.newTransformer();
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                StringWriter writer = new StringWriter();
                transformer.transform(new DOMSource(doc), new StreamResult(writer));
                return writer.getBuffer().toString();
            }
            catch(Exception e)
            {
                e.printStackTrace();
                return null;
            }
        }
        
        private SolrInputDocument transformXmlDocToSolrDoc(Document doc)
        {
            SolrInputDocument solrDoc=new SolrInputDocument();
            
            String xml=getStringFromXmlDoc(doc);
            
            if(xml!=null)
            {
                System.out.println(xml);
            }
            
            NodeList list=doc.getElementsByTagName("f");
            
            for(int i=0;i<list.getLength();i++)
            {
                Element element=(Element)list.item(i);
                
                String fieldName=element.getAttribute("n");
                String fieldValue=null;
                NodeList values=element.getElementsByTagName("v");
                
                if(values!=null && values.getLength()>0)
                {
                    Element value=(Element)values.item(0);
                    fieldValue=value.getTextContent();
                }
                
                if(fieldValue!=null && fieldValue.length()>0)
                {
                    solrDoc.addField(fieldName,fieldValue);
                }
            }
            
              
            return solrDoc;
        }
        
        private void addDocumentToCore(SolrInputDocument doc)
        {
            LocalSolrQueryRequest req=new LocalSolrQueryRequest(core,new HashMap<String,String[]>());

            AddUpdateCommand cmd = new AddUpdateCommand(req);
            cmd.solrDoc=doc;
            try
            {
                core.getUpdateHandler().addDoc(cmd);
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        
        @Override
        public void run()
        {
            if(createConsumer())
            {
                while(!this.isInterrupted())
                {
                    try
                    {
                        Document doc=nextDocument();
                        if(doc!=null)
                        {
                            addDocumentToCore(transformXmlDocToSolrDoc(doc));
                        }
                        else
                        {   
                            Thread.sleep(1000);
                        }
                    }
                    catch(InterruptedException e)
                    {
                        break;
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                        try
                        {
                            Thread.sleep(1000);
                        }
                        catch(InterruptedException ie)
                        {
                            return;
                        }
                    }
                }
            } 
        }
    }
    
    public SolrRabbitMQConsumer()
    {
    }
     
    @Override
    public void init(NamedList nl) {
        NamedList queueConfig=(NamedList)nl.get("queueConfig");
        if(queueConfig!=null)
        {
            this.queueName=(String)queueConfig.get("queueName");
            this.queueURI=(String)queueConfig.get("queueURI");
        }
    }
 
    @Override
    public void handleRequest(SolrQueryRequest sqr, SolrQueryResponse sqr1) {
    
    }

    @Override
    public String getName() {
        return "QueueIndexer";
    }

    @Override
    public String getVersion() {
        return "1.0";
    }

    @Override
    public String getDescription() {
        return "QueueIndexer";
    }

    @Override
    public Category getCategory() {
        return Category.OTHER;
    }

    @Override
    public String getSource() {
        return "QueueIndexer";
    }

    @Override
    public URL[] getDocs() {
        return null;
    }

    @Override
    public NamedList getStatistics() {
        return null;
    }
}
