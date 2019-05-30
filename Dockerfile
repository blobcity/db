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

#FROM blobcity/java8-alpine

FROM blobcity/java8-ubuntu

ENV DB_VER=1.7.8-alpha

RUN cd / && mkdir data

ADD distribution/target/blobcity-db-$DB_VER.tar.gz /opt/

RUN apt-get update
RUN apt-get install dmidecode

COPY resources/* /resources/

ENV BLOBCITY_DATA=/data/

WORKDIR /opt/blobcity-db-$DB_VER

CMD ["bin/blobcity.sh"]

EXPOSE 10111
EXPOSE 10113
