package com.aws.dcortesi.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sweeper extends MetaStorePreEventListener {
    private static final Logger log = LoggerFactory.getLogger(Sweeper.class);

    public Sweeper(Configuration config) {
        super(config);
        log.info("Janitor Sweeper preHook created");
    }

    public void onEvent(PreEventContext preEventContext) {
        String msg = "Janitor Sweeper got an event " + preEventContext.getEventType();
        log.info(msg);
    }
}