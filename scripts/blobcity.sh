#!/usr/bin/env bash

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



# Set the BlobCity specific environment and add all required libraries to the classpath

# The Java implementation to use. This is required.
#export JAVA_HOME=

VERSION="1.7.10-alpha"
JAVA=""
if [ "$JAVA_HOME" != "" ]; then
  JAVA=$JAVA_HOME/bin/java
  echo "JAVA_HOME $JAVA_HOME"
else
  echo "JAVA_HOME must be set."
  exit 1
fi

# Use to specify data storage location. Default location is BLOBCITY_HOME/data/
#export BLOBCITY_DATA=

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR_UP="/../"

export BLOBCITY_HOME=$DIR$DIR_UP

echo "BLOBCITY_HOME $BLOBCITY_HOME"

#location of tableau library. Required if Tableau integration features are to be used.
export LD_LIBRARY_PATH=$BLOBCITY_HOME/libraries/tableausdk-linux64-10300.17.0728.2252/lib64/tableausdk
echo "Using TableauSDK from: $LD_LIBRARY_PATH"

export TAB_SDK_LOGDIR=$BLOBCITY_HOME/logs/
echo "TableauSDK log directory : $TAB_SDK_LOGDIR"

# Set the classpath.

if [ "$CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
else
  CLASSPATH=$JAVA_HOME/lib/tools.jar
fi

# so that filenames w/ spaces are handled correctly in loops below
IFS=

for f in $BLOBCITY_HOME/modules/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done

for f in $BLOBCITY_HOME/modules/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done

echo "CLASSPATH=$CLASSPATH"

# restore ordinary behavior
unset IFS

#All Done. Continue now
exec "$JAVA" -classpath "$CLASSPATH" -Xmx128G -jar "$BLOBCITY_HOME/modules/launcher-"$VERSION".jar" "$@"