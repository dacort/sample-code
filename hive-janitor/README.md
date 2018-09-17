# Hive Janitor

Hive metastore listener that notifies SQS on table modification

## Overview

Implements both a pre event and event listener to illustrate utilizing even listeners to notify other systems of changes.

## Building

- Update `pom.xml` to match versions on your cluster
- Create the jar with `mvn package`
- Copy the jar into `/usr/lib/hive/lib` (or `$HIVE_HOME/lib` directory)

## Usage

1. Set the following properties either in Hive CLI (for testing) or by modifying `hive-site.xml`.

```xml
  <property>
    <name>hive.metastore.event.listeners</name>
    <value>com.aws.dcortesi.hive.Janitor</value>
  </property>

  <property>
    <name>hive.metastore.pre.event.listeners</name>
    <value>com.aws.dcortesi.hive.Sweeper</value>
  </property>

  <property>
    <name>janitor.queue_url</name>
    <value>https://sqs.<REGION>.amazonaws.com/<ACCOUNT_ID>/<QUEUE_NAME></value>
  </property>
```

OR

```sql
set hive.metastore.event.listeners=com.aws.dcortesi.hive.Janitor;
set hive.metastore.pre.event.listeners=com.aws.dcortesi.hive.Sweeper;
set janitor.queue_url=https://sqs.<REGION>.amazonaws.com/<ACCOUNT_ID>/<QUEUE_NAME>;
```

2. Restart Hive:

```shell
sudo initctl stop hive-hcatalog-server && sudo initctl start hive-hcatalog-server
```


3. Test with some sample data


```sql
CREATE TABLE s3test (id int)
STORED AS ORC
LOCATION 's3://<BUCKET/<PREFIX>/';
insert into table s3test values (3);
```

And you should see notifications show up in your SQS queue.


```xml
  <property>
    <name>hive.metastore.pre.event.listeners</name>
    <value>com.aws.dcortesi.hive.Sweeper</value>
  </property>
```


## Partitioned Table

```sql
CREATE TABLE s3testpart (id int) PARTITIONED BY (part int)
STORED AS ORC
LOCATION 's3://<BUCKET>/<PART_PREFIX>/';
```