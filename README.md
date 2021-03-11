# Epik knowledge graph gateway job

## How to submit domain-triple-files----> log files transformation job to spark on yarn.

Follow the instructions of [nebula-java](https://github.com/vesoft-inc/nebula-java) to build and install nebula-java client.

```bash
${SPARK_HOME}/bin/spark-submit --class com.epik.kbgateway.DomainKnowledge2LogFile --master yarn --deploy-mode cluster --driver-memory 256M --driver-java-options "-Dspark.testing.memory=536870912" --executor-memory 6g  --num-executors 4 --executor-cores 2 /root/epik-kbgateway-job.jar -f /cn_dbpedia_input/baike_triples.txt -d cn_dbpedia -t /epik_log_output
```

```bash
${SPARK_HOME}/bin/spark-submit --class com.epik.kbgateway.LogFileReplayer --master yarn --deploy-mode cluster --driver-memory 256M --driver-java-options "-Dspark.testing.memory=536870912" --executor-memory 4g  --num-executors 4 --executor-cores 2 /root/epik-kbgateway-job.jar -d cn_dbpedia -h localhost:55020,localhost:55022,localhost:9669 -b 100 -i /epik_log_output -q /ngql_dump -s 1 -t 4000
```


# How to restore cn_dbpedia Nebula Graph database?

1. Install docker and docker-compose, make sure the following cmd works.
```bash
$ docker version
$ docker-compose help
```

2. Clone nebula docker compose git repo, and start up nebula graph database cluster with docker-compose.

```bash
$ git clone https://github.com/vesoft-inc/nebula-docker-compose.git
$ cd nebula-docker-compose/
$ docker-compose up -d
```

Check to see whether nebula graph cluster start up successfully through `docker ps` , make sure the output show something like this:

```bash
# docker ps
CONTAINER ID   IMAGE                               COMMAND                  CREATED        STATUS                PORTS                                                                                                  NAMES
ca439dc5e07e   vesoft/nebula-console:v2-nightly    "/bin/sh"                38 hours ago   Up 38 hours                                                                                                                  nostalgic_heyrovsky
5f1a4681882a   vesoft/nebula-console:v2-nightly    "/bin/sh"                2 days ago     Up 2 days                                                                                                                    fervent_hoover
20b16354febf   vesoft/nebula-console:v2-nightly    "/bin/sh"                3 days ago     Up 3 days                                                                                                                    gallant_mayer
ed103e81797c   vesoft/nebula-console:v2-nightly    "/bin/sh"                3 days ago     Up 3 days                                                                                                                    fervent_visvesvaraya
0317bf1d1bf6   vesoft/nebula-graphd:v2-nightly     "/usr/local/nebula/b…"   3 days ago     Up 2 days (healthy)   0.0.0.0:55020->9669/tcp, 0.0.0.0:49317->19669/tcp, 0.0.0.0:49315->19670/tcp                            nebula-docker-composegit_graphd1_1
e4dfacfd87a9   vesoft/nebula-graphd:v2-nightly     "/usr/local/nebula/b…"   3 days ago     Up 2 days (healthy)   0.0.0.0:55022->9669/tcp, 0.0.0.0:49320->19669/tcp, 0.0.0.0:49319->19670/tcp                            nebula-docker-composegit_graphd2_1
4193243df37b   vesoft/nebula-storaged:v2-nightly   "./bin/nebula-storag…"   3 days ago     Up 2 days (healthy)   9777-9778/tcp, 9780/tcp, 0.0.0.0:49318->9779/tcp, 0.0.0.0:49316->19779/tcp, 0.0.0.0:49313->19780/tcp   nebula-docker-composegit_storaged2_1
fa681b1da10b   vesoft/nebula-storaged:v2-nightly   "./bin/nebula-storag…"   3 days ago     Up 2 days (healthy)   9777-9778/tcp, 9780/tcp, 0.0.0.0:49323->9779/tcp, 0.0.0.0:49322->19779/tcp, 0.0.0.0:49321->19780/tcp   nebula-docker-composegit_storaged0_1
e80dd421c455   vesoft/nebula-storaged:v2-nightly   "./bin/nebula-storag…"   3 days ago     Up 2 days (healthy)   9777-9778/tcp, 9780/tcp, 0.0.0.0:49314->9779/tcp, 0.0.0.0:49312->19779/tcp, 0.0.0.0:49311->19780/tcp   nebula-docker-composegit_storaged1_1
7db545994257   vesoft/nebula-graphd:v2-nightly     "/usr/local/nebula/b…"   3 days ago     Up 2 days (healthy)   0.0.0.0:9669->9669/tcp, 0.0.0.0:49309->19669/tcp, 0.0.0.0:49306->19670/tcp                             nebula-docker-composegit_graphd_1
bcb3ecfc1ac9   vesoft/nebula-metad:v2-nightly      "./bin/nebula-metad …"   3 days ago     Up 2 days (healthy)   9560/tcp, 0.0.0.0:49302->9559/tcp, 0.0.0.0:49301->19559/tcp, 0.0.0.0:49300->19560/tcp                  nebula-docker-composegit_metad1_1
3913c7a64c72   vesoft/nebula-metad:v2-nightly      "./bin/nebula-metad …"   3 days ago     Up 2 days (healthy)   9560/tcp, 0.0.0.0:49308->9559/tcp, 0.0.0.0:49305->19559/tcp, 0.0.0.0:49303->19560/tcp                  nebula-docker-composegit_metad2_1
478942d3bc4c   vesoft/nebula-metad:v2-nightly      "./bin/nebula-metad …"   3 days ago     Up 2 days (healthy)   9560/tcp, 0.0.0.0:49310->9559/tcp, 0.0.0.0:49307->19559/tcp, 0.0.0.0:49304->19560/tcp                  nebula-docker-composegit_metad0_1
```

3. Run nebula-console docker image to restore schema and data, which is dump as ngql.

Run nebula-console container first:

```bash
docker run --rm -ti --network nebula-docker-composegit_nebula-net -v /root/schema:/schema --entrypoint=/bin/sh vesoft/nebula-console:v2-nightly
-v /root/data/ngql_output:/ngql_output
```

In nebula-console docker container, run the following to restore graph database schema(must before data):

```bash
# nebula-console -u user -p password --address=graphd --port=9669 -f /schema/cn_dbpedia_schema.ngql
```

cn_dbpedia_schema.nql's content:

partition_num should be >=512, to make LogReplay easy.
```sql
CREATE SPACE IF NOT EXISTS cn_dbpedia(partition_num=512, replica_factor=3, vid_type=INT64);
:sleep 5
USE cn_dbpedia;
CREATE TAG IF NOT EXISTS domain_entity(domain_predicate string);
CREATE EDGE IF NOT EXISTS predicate(domain_predicate string);
```

and then, restore data, _this may take hours_.

```bash
for file in $(ls /ngql_output/part-* | sort)
do
    echo "Replaying ${file}..."
    nebula-console -u user -p password --address=graphd --port=9669 -f $file;
    echo "Replay ${file} Done."
done
```


4. Query data back using ngql.

```sql
GO 1 STEPS FROM hash("书籍") OVER predicate BIDIRECT YIELD predicate.domain_predicate;

GET SUBGRAPH 1 STEPS FROM hash("书籍") OUT predicate;
```