<a href="https://www.blobcity.com"><img src="https://blobcity.com/blobcity-logo.png" height="60"/></a>

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://github.com/blobcity/db/blob/master/LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)
[![GitHub issues](https://img.shields.io/github/issues/blobcity/db.svg)](https://github.com/blobcity/db/issues)
[![Generic badge](https://img.shields.io/badge/docs-read-blue.svg)](https://docs.db.blobcity.com)
[![Ask Me Anything !](https://img.shields.io/badge/Ask%20me-anything-1abc9c.svg)](mailto:support@blobcity.com)
[![Slack](https://slack.blobcity.com/badge.svg)](https://slack.blobcity.com)

# Description

BlobCity DB is an All-in-One Database. It offers support for natively storing 17 different formats of data, including JSON, XML, CSV, PDF, Word, Excel, Log, GIS, Image amongst others. It run two full feature storage engines. One that stores data in memory and the other that stores data on disk. In-memory storage offers sheer performance for real-time analytics, while the disk storage make BlobCity an excellent alternative for DataLakes.

# Supported Data Formats
**Push data in any of these 17 formats:** JSON, XML, CSV, SQL, Plaintext, PDF, Excel, Word, RTF, ZIP, Log, Powerpoint, syslog, audio files, video files, image files, GIS

# Multi-Model Example
<img src="http://blobcity.com/assets/img/JSON-XML.gif"/>

**JSON Record**
```JSON
{"col1": 1, "col2": 2}
```

**XML Record**
```XML
<col1>3</col1></col2>4</col2>
```
**Auto created table schema and data**

| col1 | col2 |
|:----:|:----:|
|   1  |   2  |
|   3  |   4  |

Push variety of data into a single collection within BlobCity, and get back a standardised response.

```shell
user$ nc localhost 10113
username>root
password>9a371c6445
You are now inside the BlobCity DB console
Type 'help' for assistance and 'exit' to quit
blobcity>create-ds test
Datastore successfully created
blobcity>create-collection test.test
Collection successfully created

blobcity>insert into test.test JSON
In insert mode. Type 1 JSON per line and press enter to insert
{"col1": 1, "col2": 2}
Inserted
exit
Exited insert mode

blobcity>insert into test.test XML
In insert mode. Type 1 XML per line and press enter to insert
<col1>3</col1><col2>4</col2>
Inserted
exit
Exited insert mode

blobcity>sql test: select * from test.test
{"p":[{"_id":"5cb30531-dde1-493c-9c67-86b5f4dce36c","col2":2,"col1":1},{"_id":"57f653e3-de68-4591-9563-af9ad66af56b","col2":4,"col1":3}],"time(ms)":2,"ack":"1","rows":2}

blobcity>sql test: select col1 from test.test
{"p":[{"col1":"1"},{"col1":"3"}],"time(ms)":18,"ack":"1","rows":2}

blobcity>set-column-type test.test col1 integer 
Column type successfully updated in schema

blobcity>sql test: select SUM(col1) from test.test
{"p":[{"SUM(col1)":4}],"time(ms)":27,"ack":"1","rows":1}
```

The above example shows inserting both JSON and XML recoreds into the same collection. The DB seamlessly creates columns and merges the columns to allow querying of both records using SQL. 

# Features
* **Full SQL:** Run SQL queries over REST, ODBC & JDBC connectivity
* **DataLake:** On-disk storage engine optimised for DataLake scale with low latency query response
* **DML Support:** Designed like a DataLake, but works like a database. Full support for `UPDATE` & `DELETE` queries
* **Realtime:** HIgh speed in-memory storage optimised for real-time analytics
* **17 Data Formats:** Stores 17 formats of data such as JSON, XML, PDF, Excel, Word amongst others for collective analytics
* **ACID:** Full ACID compliant transactions on individual records
* **Stored Procedures:** Run Java & Scala code within the database for complex operations on data without moving the data out of the database
* **Fine-grained Access Control:** Control data access across users and departments, with column level control on user access
* **On-Cloud:** Fully managed virtually infinte scale, multi-tenant cloud with unlimited free storae and pay only for what you analyse

# Get Started
<a href="http://www.blobcity.com"><img src="https://www.blobcity.com/assets/img/blobcity-favicon.png" height="50"/></a>
&nbsp;&nbsp;
<a href="https://docs.blobcity.com/docs/getting-started"><img src="https://www.blobcity.com/assets/img/docker.png" height="50"/></a>
&nbsp;&nbsp;
<a href="https://docs.blobcity.com/docs/blobcity-on-aws-marketplace"><img src="https://blobcity.com/assets/img/aws.png" height="50"/></a>
&nbsp;&nbsp;
<a href="https://docs.blobcity.com/docs/blobcity-on-digital-ocean-marketplace"><img src="https://www.blobcity.com/assets/img/do.png" height="50"/></a>

### On BlobCity Cloud
Store unlimited data for free and pay only for what you analyse. Delivers ultra high speed analytics over multi-tenant infrastructure, starting at $10/month. 

[Start Now](https://blobcity.com/getstarted.html)

### Using Docker
`docker run -i -p 10111:10111 -p 10113:10113 blobcity/db`

Once container is started, open a telnet connection on port `10113` to connect to over network CLI. 
```shell
nc localhost 10113
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
username>root
password>xxxxx
You are now inside the BlobCity DB console
Type 'help' for assistance and 'exit' to quit
blobcity>
```

A random auto-generated password is placed at `/mnt/data/root-pass.txt`. This file can be found within the container. It can be fetched from within the container, or by mounting this folder to an external mount point. 

`docker run -i -v /my-folder:/mnt/data -p 10111:10111 -p 10113:10113 blobcity/db`

The password file can now be found at `/my-folder/root-pass.txt` on your computer.

### Using Binary Distribution
*Supported only on Unix & MacOS distributions*

Download latest tar.gz archive from the [releases](https://github.com/blobcity/db/releases).

Decompress the download file, then run `blobcity.sh` from inside the `bin` folder.

```shell
user>tar -xvf blobcity-db-x.x.x.tar.gz
user>cd blobcity-db-x.x.x/bin
user>sh ./blobcity.sh
```

`JAVA_HOME` must be set to a JDK / JRE version 8 or higher for the DB to be booted. 

The database will create a folder called `data` at `blobcity-db-x.x.x/data`. The randomly generated `root` user password can be found inside a text file at `blobcity-db-x.x.x/data/root-pass.txt`. 

Use this password to connect to the CLI console to start using the DB. It is recommended that the `data` folder be stored at a difference location than the boot volume, and the volume be XFS formatted. 

The location of the data folder can be set by editing the `blobcity.sh` file and uncommenting the following line and setting a folder path of your choice.

```sh
#export BLOBCITY_DATA=
```

Look at some of the [best practices](https://docs.blobcity.com/docs/disk-storage-performance-considerations) for optimal disk storage performance. 

# Acceleration
BlobCity is a winner of Economic Times Power of Ideas (Season 2), is funded by CIIE IIM-Ahmedabad and is a graduate from NetApp Excellerator (Cohort #2). 

<a href="https://ciie.co"><img src="https://www.blobcity.com/assets/img/ciie-logo.png" height="60"/></a>
&nbsp;&nbsp;
<a href="https://startup.netapp.in"><img src="https://www.blobcity.com/assets/img/netapp-excellerator.png" height="60"/></a>

# Docs

[https://docs.db.blobcity.com](https://docs.db.blobcity.com)

# Contribute

[Join our Slack community](https://slack.blobcity.com) and request to become a contributor. We encourage your contributions :)

# Authors
BlobCity DB was created by [Sanket Sarang](https://www.linkedin.com/in/sanketsarang/) along with notable contributions from [Akshay Dewan](https://www.linkedin.com/in/akshay-dewan-0a972b21) and [Karun Japhet](https://www.linkedin.com/in/karunjaphet), amongst others. BlobCity DB is sponsored by [BlobCity, Inc.](https://www.blobcity.com).
   
# License

GNU Affero General Public License v3.0

# Kraken
```
                       ___
                    .-'   `'.
                   /         \
                   |         ;
                   |         |           ___.--,
          _.._     |0) ~ (0) |    _.---'`__.-( (_.
   __.--'`_.. '.__.\    '--. \_.-' ,.--'`     `""`
  ( ,.--'`   ',__ /./;   ;, '.__.'`    __
  _`) )  .---.__.' / |   |\   \__..--""  """--.,_
 `---' .'.''-._.-'`_./  /\ '.  \ _.-~~~````~~~-._`-.__.'
       | |  .'R_.-' |  |  \K \  '.               `~---`
        \K\/ .'     \  \   '. '-._)
         \/ /        \  \    `=.__`~-.
         / /\         `) )    /E/ `"".`\
   , _.-'.'\ \        /A/    ( (     /N/
    `--~`   ) )    .-'.'      '.'.  | (
           (/`    ( (`          ) )  '-;
            `      '-;         (-'
```
Kraken was our internal project code name until open sourcing. You may still find some mentions of it in the code docs.
