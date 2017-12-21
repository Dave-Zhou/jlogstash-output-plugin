package com.tansun.jlogstash.outputs;

import com.tansun.jlogstash.annotation.Required;
import com.tansun.jlogstash.render.Formatter;
import com.tansun.jlogstash.utils.ConditionUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dave on 2017/12/21.
 *
 * @version 1.0 2017/12/21
 * autor zxd
 */
public class Elasticsearch6 extends BaseOutput {

  private static final Logger logger = LoggerFactory.getLogger(Elasticsearch6.class);

  @Required(required = true)
  public static String index;

  public static String indexTimezone = null;

  public static String documentId;

  public static String documentType = "logs";

  public static String cluster;

  @Required(required = true)
  public static List<String> hosts;

  private static boolean sniff = true;

  private static int bulkActions = 20000;

  private static int bulkSize = 15;

  private static int flushInterval = 5;//seconds

  private static int concurrentRequests = 1;

  private BulkProcessor bulkProcessor;

  private TransportClient esclient;

  private static int numberOfShards = 5;

  private static int numberOfReplicas = 1;

  private static String condition;

  private AtomicBoolean isClusterOn = new AtomicBoolean(true);

  private ExecutorService executor;

  public Elasticsearch6(Map config) {
    super(config);
  }

  public void prepare() {

    try {

      executor = Executors.newSingleThreadExecutor();

      this.initESClient();

    } catch (Exception e) {

      logger.error("elasticsearch6 initESClient faild", e);

      System.exit(1);
    }

  }


  private void initESClient() throws NumberFormatException,
      UnknownHostException {

    Builder builder = Settings.builder().put("client.transport.sniff", sniff);

    if (StringUtils.isNotBlank(cluster)) {

      builder.put("cluster.name", cluster);

    }

    Settings settings = builder.build();

    esclient = new PreBuiltTransportClient(settings);

    TransportAddress[] addresss = new TransportAddress[hosts.size()];

    for (int i = 0; i < hosts.size(); i++) {

      String host = hosts.get(i);

      String[] hp = host.split(":");

      String h = null, p = null;

      if (hp.length == 2) {

        h = hp[0];

        p = hp[1];

      } else if (hp.length == 1) {

        h = hp[0];

        p = "9300";

      }
      addresss[i] = new TransportAddress(InetAddress.getByName(h), Integer.parseInt(p));
    }

    esclient.addTransportAddresses(addresss);

    executor.submit(new ClusterMonitor(esclient));

    bulkProcessor = BulkProcessor
        .builder(esclient, new BulkProcessor.Listener() {

          @Override
          public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {

            List<DocWriteRequest> requests = arg1.requests();

            int toberetry = 0;

            int totalFailed = 0;

            for (BulkItemResponse item : arg2.getItems()) {

              if (item.isFailed()) {

                switch (item.getFailure().getStatus()) {

                  case TOO_MANY_REQUESTS:

                    if (totalFailed == 0) {

                      logger.error("too many request {}:{}", item.getIndex(),
                          item.getFailureMessage());

                    }

                    addFailedMsg(requests.get(item.getItemId()));

                    break;

                  case SERVICE_UNAVAILABLE:

                    if (toberetry == 0) {

                      logger.error("sevice unavaible cause {}:{}", item.getIndex(),
                          item.getFailureMessage());

                    }

                    addFailedMsg(requests.get(item.getItemId()));

                    break;

                  default:

                    if (totalFailed == 0) {

                      logger.error("data formate cause {}:{}:{}", item.getIndex(),
                          ((IndexRequest) requests.get(item.getItemId())).sourceAsMap(),
                          item.getFailureMessage());

                    }

                    break;

                }

                totalFailed++;

              }
            }

            if (totalFailed > 0) {

              logger.info(totalFailed + " doc failed, " + toberetry + " need to retry");

            } else {

              logger.debug("no failed docs");

            }

          }

          @Override
          public void afterBulk(long arg0, BulkRequest arg1,
              Throwable arg2) {

            logger.error("bulk got exception:", arg2);

            for (DocWriteRequest request : arg1.requests()) {

              addFailedMsg(request);

            }

          }

          @Override
          public void beforeBulk(long arg0, BulkRequest arg1) {

            logger.info("executionId: " + arg0);

            logger.info("numberOfActions: " + arg1.numberOfActions());
          }
        })
        .setBulkActions(bulkActions)
        .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
        .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
        .setConcurrentRequests(concurrentRequests).build();
  }

  public void emit(Map event) {

    //根据condition 判断是否执行grok
    if (!StringUtils.isEmpty(this.condition)) {

      if (!ConditionUtils.isTrue(event, this.condition)) {

        return;

      }
    }

    String _index = Formatter.format(event, index, indexTimezone);

    //判断一次index是否已经存在
    if (!indexExists(esclient, _index)) {

      int totalFields = 1000;
      esclient.admin().indices().prepareCreate(_index)
          .setSettings(Settings.builder()
              .put("index.number_of_shards", numberOfShards)
              .put("index.number_of_replicas", numberOfReplicas)
              .put("index.mapping.total_fields.limit", totalFields)
          ).get();

    }

    String _indexType = Formatter.format(event, documentType, indexTimezone);

    IndexRequest indexRequest;

    if (StringUtils.isBlank(documentId)) {

      indexRequest = new IndexRequest(_index, _indexType).source(event);

    } else {

      String _id = Formatter.format(event, documentId, indexTimezone);

      if (Formatter.isFormat(_id)) {

        indexRequest = new IndexRequest(_index, _indexType).source(event);

      } else {

        indexRequest = new IndexRequest(_index, _indexType, _id).source(event);

      }
    }

    this.bulkProcessor.add(indexRequest);

    checkNeedWait();

  }

  @Override
  public void sendFailedMsg(Object msg) {

    this.bulkProcessor.add((IndexRequest) msg);

    checkNeedWait();

  }

  public boolean indexExists(TransportClient esclient, String index) {
    IndicesExistsRequest request = new IndicesExistsRequest(index);
    IndicesExistsResponse response = esclient.admin().indices().exists(request).actionGet();
    if (response.isExists()) {
      return true;
    }
    return false;
  }


  @Override
  public void release() {
    if (bulkProcessor != null) {
      bulkProcessor.close();
    }
  }

  public void checkNeedWait() {
    while (!isClusterOn.get()) {//等待集群可用
      try {

        logger.warn("wait cluster avaliable...");

        Thread.sleep(1000);

      } catch (InterruptedException e) {

        logger.error("wait cluster exception", e);

      }
    }
  }

  class ClusterMonitor implements Runnable {

    private TransportClient transportClient;

    public ClusterMonitor(TransportClient client) {
      this.transportClient = client;
    }

    @Override
    public void run() {

      while (true) {

        try {

          logger.debug("getting es cluster health.");

          ActionFuture<ClusterHealthResponse> healthFuture = transportClient.admin().cluster()
              .health(Requests.clusterHealthRequest());

          ClusterHealthResponse healthResponse = healthFuture.get(5, TimeUnit.SECONDS);

          logger.debug("Get num of node:{}", healthResponse.getNumberOfNodes());

          logger.debug("Get cluster health:{} ", healthResponse.getStatus());

          isClusterOn.getAndSet(true);

        } catch (Throwable t) {

          if (t instanceof NoNodeAvailableException) {//集群不可用

            logger.error("the cluster no node avaliable.");

            isClusterOn.getAndSet(false);

          } else {

            isClusterOn.getAndSet(true);

          }
        }
        try {

          Thread.sleep(1000);

        } catch (InterruptedException ie) {

          logger.error("Thread sleep faild",ie);
        }
      }
    }
  }
}
