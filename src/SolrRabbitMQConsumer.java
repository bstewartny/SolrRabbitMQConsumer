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
import java.util.Iterator;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
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
    String queueName="solr"; 
    String queueURI="amqp://solr:solr@localhost:5672/solr";
    
    
    public SolrRabbitMQConsumer()
    {
       
    }
     
    @Override
    public void init(NamedList nl) {
        System.out.println("SolrRabbitMQConsumer::init");
        NamedList queueConfig=(NamedList)nl.get("queueConfig");
        if(queueConfig!=null)
        {
            queueName=(String)queueConfig.get("queueName");
            queueURI=(String)queueConfig.get("queueURI");
        }
    }
    
    @Override
    public void inform(SolrCore sc) {
        System.out.println("SolrRabbitMQConsumer::inform");
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
        CloudSolrServer cloudServer=null;
        
        
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
        
        private void printParams(SolrParams params)
        {
            System.out.println("printParams");
            if(params!=null)
            {
                Iterator<String> it=params.getParameterNamesIterator();
                while(it.hasNext())
                {
                    String name=it.next();
                    String value=params.get(name);
                    System.out.println(name+"="+value);
                }
            }
        }
        
        private boolean isSolrCloud()
        {
            if(cloudServer!=null)
            {
                return true;    
            }
            
            CloudDescriptor cloud=core.getCoreDescriptor().getCloudDescriptor();
            
            if(cloud!=null)
            {
                System.out.println("core has CloudDescriptor=");
                System.out.println("getCollectionName="+cloud.getCollectionName());
                System.out.println("getNumShards="+cloud.getNumShards());
                System.out.println("getShardId="+cloud.getShardId());
                System.out.println("isLeader="+cloud.isLeader());
                
                SolrParams params=cloud.getParams();
                printParams(params);
                
                return true;
                //return cloud.getNumShards()>0;
                
            }
            else
            {
                System.out.println("No CloudDescriptor found...");
                return false;
            }
        }
        
        private void addDocumentToCloud(SolrInputDocument doc) throws SolrServerException,IOException
        {
            System.out.println("addDocumentToCloud");
            
            
             cloudServer.add(doc);
        
        }
        
        private void addDocumentToCore(SolrInputDocument doc)
        {
            System.out.println("addDocumentToCore");
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
        
        private String getZookeeperHost()
        {
            return "127.0.0.1:9983"; // TODO
        }
        
        private boolean createCloudServerClient()
        {
            String zkHost=getZookeeperHost();
            if(zkHost==null)
            {
                return false;
            }
            try
            {
                cloudServer=new CloudSolrServer(zkHost);
                cloudServer.setDefaultCollection(core.getName());
                
                return true;
            }
            catch(Exception e)
            {
                e.printStackTrace();
                return false;
            }
        
        }
        
        @Override
        public void run()
        {
            System.out.println("run");
            if(createConsumer())
            {
                boolean isSolrCloud=isSolrCloud();
                if(isSolrCloud())
                {
                    System.out.println("Running SOLR cloud...");
                    if(!createCloudServerClient())
                    {
                        System.out.println("Failed to create cloud server client");
                        return;
                    }
                    else
                    {
                        // we need to make sure cluster is fully up before we start adding documents
                        // AFAIK, there is no good trigger to know when it is availabie
                        // We can try to sleep for default zk timeout, and then try again to connect to cluster...
                        cloudServer.connect();
                        
                        while(!this.isInterrupted())
                        {
                           System.out.println("Check for live nodes in the cluster...");
                            try
                            {
                                try
                                {
                                    ZkStateReader zkReader=cloudServer.getZkStateReader();
                                    if(zkReader!=null)
                                    {
                                        ClusterState clusterState=zkReader.getClusterState();
                                        if(clusterState!=null)
                                        {
                                            
                                       

                                            Set<String> liveNodes=clusterState.getLiveNodes();
                                            if(liveNodes!=null && liveNodes.size()>0)
                                            {
                                                System.out.println("We see "+liveNodes.size()+" live nodes...");
                                                break;
                                            }
                                            else
                                            {
                                                System.out.println("No live nodes found");
                                                


                                            } 
                                        }
                                        else
                                        {
                                            System.out.println("No cluster state found");
                                        }
                                    }
                                    else
                                    {
                                        System.out.println("Failed to get zkStateReader");
                                    }
                                    Thread.sleep(1000);
                                }
                                catch(Exception e)
                                {
                                    e.printStackTrace();
                                    Thread.sleep(1000);
                                }
                            }
                            catch(InterruptedException e)
                            {
                                break;
                            }
                        }
                    }
                }
                
                while(!this.isInterrupted())
                {
                    try
                    {
                        Document doc=nextDocument();
                        if(doc!=null)
                        {
                            if(isSolrCloud)
                            {
                                addDocumentToCloud(transformXmlDocToSolrDoc(doc));
                            }
                            else
                            {
                                addDocumentToCore(transformXmlDocToSolrDoc(doc));
                            }
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
    
    
    @Override
    public void handleRequest(SolrQueryRequest sqr, SolrQueryResponse sqr1) {
    
    }

    @Override
    public String getName() {
        return "SolrRabbitMQConsumer";
    }

    @Override
    public String getVersion() {
        return "1.0";
    }

    @Override
    public String getDescription() {
        return "SolrRabbitMQConsumer";
    }

    @Override
    public Category getCategory() {
        return Category.OTHER;
    }

    @Override
    public String getSource() {
        return "SolrRabbitMQConsumer";
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
