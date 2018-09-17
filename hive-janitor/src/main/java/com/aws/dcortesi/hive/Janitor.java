package com.aws.dcortesi.hive;

import com.amazonaws.services.sqs.AmazonSQSClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Janitor extends MetaStoreEventListener {
    private static final Logger log = LoggerFactory.getLogger(Janitor.class);
    private static String queueURL;
    private final AmazonSQS sqs = new AmazonSQSClient();


    public Janitor(Configuration config) {
        super(config);
        queueURL = config.get("janitor.queue_url");
        log.info(String.format("Janitor created - sending notifications to %s", queueURL));
    }

    @Override
    public void onInsert(InsertEvent insertEvent) throws MetaException {
        super.onInsert(insertEvent);
        log.info("Janitor: Insert called on " + insertEvent.getDb() + "." + insertEvent.getTable());
    }

    @Override
    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
        super.onAddPartition(partitionEvent);
        log.info("Janitor: AddPartition called on "  + partitionEvent.getTable());
    }

    @Override
    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
        super.onAlterTable(tableEvent);
        String msg = "Janitor: AlterTable called on " + tableEvent.toString() + " for old table " + tableEvent.getOldTable().getTableName() + " and new table " + tableEvent.getNewTable().getTableName();
        log.info(msg);
        notifySQS(tableEvent.getNewTable().getDbName(), tableEvent.getNewTable().getTableName());
    }

    public void notifySQS(String databaseName, String tableName) {
        String target = String.format("%s.%s", databaseName, tableName);
        log.info(String.format("Notifying SQS of table modification: %s", target));
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueURL)
                .withMessageBody(target)
                .withDelaySeconds(5);
        sqs.sendMessage(send_msg_request);
    }
}


