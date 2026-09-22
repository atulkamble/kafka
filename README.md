Here’s a **basic Apache Kafka hands-on tutorial for an EC2 instance running Amazon Linux 2023**. It’s suitable for classroom practice and covers installation, broker setup, topics, producers, consumers, and useful commands.

## Apache Kafka on EC2 — Amazon Linux 2023

### 1. Architecture

```text
Producer
   |
   | Send Messages
   v
+-----------------------+
|    Kafka Broker       |
|                       |
| Topic: mytopic        |
| +-------------------+ |
| | Partition 0       | |
| | msg1 msg2 msg3    | |
| +-------------------+ |
+-----------------------+
   |
   | Read Messages
   v
Consumer
```

Kafka terminology:

```text
Producer  → Sends messages
Broker    → Kafka server
Topic     → Logical message category
Partition → Splits topic data
Consumer  → Reads messages
Offset    → Position of a message
```

For this lab, we'll use **KRaft mode**, so ZooKeeper is not required.

---

# 2. Create EC2 Instance

Recommended lab configuration:

```text
OS: Amazon Linux 2023
Instance Type: t3.medium
Storage: 15–20 GB
```

Security Group:

```text
SSH       TCP 22     Your IP
Kafka     TCP 9092   Your IP / lab network
```

For a basic same-instance producer/consumer lab, only **SSH 22** needs to be exposed publicly. Do not expose `9092` to `0.0.0.0/0`.

Connect:

```bash
ssh -i key.pem ec2-user@<PUBLIC-IP>
```

---

# 3. Update Amazon Linux

```bash
sudo dnf update -y
```

Check OS:

```bash
cat /etc/os-release
```

---

# 4. Install Java

Kafka requires Java.

```bash
sudo dnf install java-17-amazon-corretto-headless -y
```

Verify:

```bash
java -version
```

Expected:

```text
openjdk version "17..."
```

---

# 5. Download Kafka

Go to home directory:

```bash
cd /home/ec2-user
```

Download a current Kafka binary from the official Apache Kafka downloads page. The exact current version can change, so use the version shown there rather than assuming an old release.

[Apache Kafka Downloads](https://kafka.apache.org/downloads?utm_source=chatgpt.com)

Example pattern after choosing a version:

```bash
wget https://downloads.apache.org/kafka/<VERSION>/kafka_<SCALA_VERSION>-<VERSION>.tgz
```

Extract:

```bash
tar -xzf kafka_*.tgz
```

Rename for easier practice:

```bash
mv kafka_* kafka
```

Enter directory:

```bash
cd kafka
```

Verify:

```bash
ls
```

You should see directories such as:

```text
bin
config
libs
licenses
site-docs
```

---

# 6. Configure Kafka in KRaft Mode

Kafka can operate without ZooKeeper using **KRaft**.

First generate a cluster ID:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```

Check:

```bash
echo $KAFKA_CLUSTER_ID
```

Format the Kafka storage using the KRaft configuration included with your downloaded Kafka release. The exact config path can differ between Kafka releases, so first inspect:

```bash
find config -name "*server*.properties" -o -name "*kraft*.properties"
```

Then format using the appropriate server properties file, for example:

```bash
bin/kafka-storage.sh format \
-t $KAFKA_CLUSTER_ID \
-c config/server.properties
```

---

# 7. Start Kafka Broker

Start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Keep this terminal running.

Kafka should start listening on its configured listener, commonly:

```text
localhost:9092
```

---

# 8. Start Kafka in Background

Instead of keeping the terminal open:

```bash
bin/kafka-server-start.sh -daemon config/server.properties
```

Check process:

```bash
ps -ef | grep kafka
```

Check port:

```bash
sudo ss -lntp | grep 9092
```

---

# 9. Create Your First Topic

Create topic:

```bash
bin/kafka-topics.sh \
--create \
--topic mytopic \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1
```

Expected:

```text
Created topic mytopic.
```

---

# 10. List Topics

```bash
bin/kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

Expected:

```text
mytopic
```

---

# 11. Describe Topic

```bash
bin/kafka-topics.sh \
--describe \
--topic mytopic \
--bootstrap-server localhost:9092
```

Conceptually:

```text
mytopic
   |
   +---- Partition 0
            |
            +---- Leader: Broker 1
```

Because we have only **one Kafka broker**, replication factor is `1`.

---

# 12. Start Kafka Producer

```bash
bin/kafka-console-producer.sh \
--topic mytopic \
--bootstrap-server localhost:9092
```

Now enter messages:

```text
>Hello Kafka
>Hello AWS
>Hello Cloud
>Kafka running on EC2
```

Flow:

```text
Producer
   |
   | Hello Kafka
   v
Kafka Broker
   |
   v
mytopic
```

---

# 13. Start Kafka Consumer

Open another SSH terminal.

```bash
cd /home/ec2-user/kafka
```

Run:

```bash
bin/kafka-console-consumer.sh \
--topic mytopic \
--bootstrap-server localhost:9092 \
--from-beginning
```

Output:

```text
Hello Kafka
Hello AWS
Hello Cloud
Kafka running on EC2
```

Complete flow:

```text
Terminal 1
Producer
   |
   v
+----------------------+
| Kafka Broker         |
|                      |
| Topic: mytopic       |
| Partition: 0         |
|                      |
| Hello Kafka          |
| Hello AWS            |
| Hello Cloud          |
+----------------------+
   |
   v
Consumer
Terminal 2
```

---

# 14. Create Topic with Multiple Partitions

```bash
bin/kafka-topics.sh \
--create \
--topic orders \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

Describe:

```bash
bin/kafka-topics.sh \
--describe \
--topic orders \
--bootstrap-server localhost:9092
```

Architecture:

```text
                orders
                   |
        +----------+----------+
        |          |          |
        v          v          v
   Partition 0 Partition 1 Partition 2
```

Partitions provide Kafka with a mechanism for **parallelism and scalability**.

---

# 15. Consumer Groups

Start consumer:

```bash
bin/kafka-console-consumer.sh \
--topic orders \
--bootstrap-server localhost:9092 \
--group order-group
```

Check groups:

```bash
bin/kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--list
```

Describe group:

```bash
bin/kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--describe \
--group order-group
```

Important relationship:

```text
Topic: orders

Partition 0 ──→ Consumer 1
Partition 1 ──→ Consumer 2
Partition 2 ──→ Consumer 3

        Consumer Group
         order-group
```

Within the same consumer group, a partition is consumed by only one consumer at a time.

---

# 16. Delete Topic

```bash
bin/kafka-topics.sh \
--delete \
--topic mytopic \
--bootstrap-server localhost:9092
```

Verify:

```bash
bin/kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

---

# 17. Stop Kafka

```bash
bin/kafka-server-stop.sh
```

Check:

```bash
ps -ef | grep kafka
```

---

## Commands to Remember

```bash
# Start Kafka
bin/kafka-server-start.sh -daemon config/server.properties

# Create topic
bin/kafka-topics.sh --create \
--topic mytopic \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1

# List topics
bin/kafka-topics.sh --list \
--bootstrap-server localhost:9092

# Describe topic
bin/kafka-topics.sh --describe \
--topic mytopic \
--bootstrap-server localhost:9092

# Producer
bin/kafka-console-producer.sh \
--topic mytopic \
--bootstrap-server localhost:9092

# Consumer
bin/kafka-console-consumer.sh \
--topic mytopic \
--bootstrap-server localhost:9092 \
--from-beginning

# Consumer groups
bin/kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--list

# Delete topic
bin/kafka-topics.sh --delete \
--topic mytopic \
--bootstrap-server localhost:9092

# Stop Kafka
bin/kafka-server-stop.sh
```

### Points to remember

**Kafka = Distributed Event Streaming Platform**

```text
Producer → Broker → Topic → Partition → Consumer
```

A **producer** publishes records, a **broker** stores and serves them, a **topic** groups related records, a **partition** is an ordered log within a topic, and a **consumer** reads records. An **offset** identifies a record's position within a partition, while a **consumer group** lets consumers share partition processing.

For production, you would normally use multiple brokers, replication, private networking, authentication/encryption, monitoring, and durable storage rather than this single-node EC2 classroom setup.
