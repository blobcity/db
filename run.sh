#!/bin/bash
#just run the database
cd distribution/target
tar -xvf blobcity-db-1.4.2.tar.gz
cd blobcity-db-1.4.2/bin
./blobcity.sh
