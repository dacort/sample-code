# Hive Janitor

Hive metastore listener that notifies SQS on table modification

## Overview

Implements both a pre event and event listener to illustrate utilizing event listeners to notify other systems of changes.

In addition, implements a pre hook that fires before the query is compiled so
the original query can be inspected.

## Building

- Update `pom.xml` to match versions on your cluster
- Create the jar with `mvn package`
- Copy the jar into `/usr/lib/hive/lib` (or `$HIVE_HOME/lib` directory)

## Usage

1. Set the following properties in your `hive-site.xml` depending on which hook you want to implement..

```xml
  <property>
    <name>hive.exec.pre.hooks</name>
    <value>com.aws.dcortesi.hive.QueryValidatorHook</value>
  </property>

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