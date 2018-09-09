#Copyright (C) 2018  BlobCity, Inc

#This program is free software: you can redistribute it and/or modify
#it under the terms of the GNU Affero General Public License as published
#by the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.

#This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU Affero General Public License for more details.

#You should have received a copy of the GNU Affero General Public License
#along with this program.  If not, see <http://www.gnu.org/licenses/>.

FROM blobcity/java8-alpine

ENV DB_VER=1.7-alpha

RUN cd / && mkdir data

ADD distribution/target/blobcity-db-$DB_VER.tar.gz /opt/

#RUN cp /opt/blobcity-db-$DB_VER/libraries/tableausdk-linux64-10200.17.0223.1918/lib64/tableausdk/* /usr/lib/

ENV BLOBCITY_DATA=/data/

WORKDIR /opt/blobcity-db-$DB_VER

CMD ["bin/blobcity.sh"]

EXPOSE 10111
EXPOSE 10113
