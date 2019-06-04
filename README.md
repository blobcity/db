<a href="https://www.blobcity.com"><img src="https://blobcity.com/blobcity-logo.png" height="60"/></a>

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://github.com/blobcity/db/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/blobcity/db.svg?branch=master)](https://travis-ci.org/blobcity/db)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)
[![GitHub issues](https://img.shields.io/github/issues/blobcity/db.svg)](https://github.com/blobcity/db/issues)
[![Generic badge](https://img.shields.io/badge/docs-read-blue.svg)](https://docs.blobcity.com)
[![Ask Me Anything !](https://img.shields.io/badge/Ask%20me-anything-1abc9c.svg)](mailto:support@blobcity.com)
[![Slack](https://slack.blobcity.com/badge.svg)](https://slack.blobcity.com)

# Description

BlobCity DB is an All-in-One Database. It offers support for natively storing 17 different formats of data, including JSON, XML, CSV, PDF, Word, Excel, Log, GIS, Image amongst others. It run two full feature storage engines. One that stores data in memory and the other that stores data on disk. In-memeory storage offers sheer performance for real-time analytics, while the disk storage make BlobCity an excellent alternative for DataLakes.

<a href="https://www.gartner.com/doc/3288923"><img src="https://blobcity.com/assets/img/Gartner-CoolVendor-2016.jpg" height="80"/></a>
<a href="https://hub.docker.com/_/blobcity-db"><img src="https://blobcity.com/assets/img/Docker_Container_white_icon%202@2x.png" height="80"/></a>

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
```
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

# Acceleration
BlobCity is a winner of Economic Times Power of Ideas (Season 2), is funded by CIIE IIM-Ahmedabad and is a graduate from NetApp Excellerator (Cohort #2). 

<a href="https://ciie.co"><img src="https://www.blobcity.com/assets/img/ciie-logo.png" height="60"/></a>
&nbsp;&nbsp;
<a href="https://startup.netapp.in"><img src="https://www.blobcity.com/assets/img/netapp-excellerator.png" height="60"/></a>

# Docs

[https://docs.blobcity.com](https://docs.blobcity.com)

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
