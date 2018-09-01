#
# Build and run C/C++ samples.
#
# Requires g++ 5.0 or later.

LIBROOT = /usr/lib64
RELLIBROOT = ../../../lib64
FLAGS  = -I$(LIBROOT)/../include -I$(RELLIBROOT)/../include
CFLAGS = $(FLAGS) -std=c99
CXXFLAGS = $(FLAGS) -std=c++0x
LDFLAGS = -Wl,-rpath,$(LIBROOT)/tableausdk:$(RELLIBROOT)/tableausdk

LIBS = -L$(LIBROOT)/tableausdk -L$(RELLIBROOT)/tableausdk -lTableauCommon -lTableauExtract -lTableauServer

usage :
	@echo
	@echo "usage: make [target]"
	@echo
	@echo "Top-level targets:"
	@echo "  usage (default)        Generate this message"
	@echo "  clean                  Clean up all the generated files"
	@echo "  build-c                Build the C sample"
	@echo "  build-cpp              Build the C++ sample"
	@echo "  build-both             Build the C and C++ samples"
	@echo "  run-c                  Build the C sample and then display its usage information"
	@echo "  run-cpp                Build the C++ sample and then display its usage information"
	@echo "  run-c-buildExtract     Build the C sample and then use it to create or expand an"
	@echo "                         extract with the default settings"
	@echo "  run-c-publishExtract   Build the C sample and then use it to publish an extract"
	@echo "                         with the default settings"
	@echo "  run-cpp-buildExtract   Build the C++ sample and then use it to create or expand an"
	@echo "                         extract with the default settings"
	@echo "  run-cpp-publishExtract Build the C++ sample and then use it to publish an extract"
	@echo "                         with the default settings"
	@echo
	@echo "NOTE: To successfully run either the C sample or C++ sample with the"
	@echo "      \"--publish\", you will need to provide, via command line arguments,"
	@echo "      the hostname, username, password, siteID; and, optionally, the extract"
	@echo "      filename, and the desired project name and datasource name. A Tableau"
	@echo "      Server must be installed and running on the hostname you specify. Admin"
	@echo "      privileges on the target server are required to publish using the Tableau"
	@echo "      SDK Server API. For details, please see the Tableau SDK documentation:"
	@echo
	@echo "      https://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm"

clean :
	rm -f DataExtract.log TableauSDK*.log \
        TableauSDKSample-c TableauSDKSample-cpp order-c.tde order-cpp.tde \

build-c : TableauSDKSample.c
	$(CC) $(CFLAGS) $(LDFLAGS) TableauSDKSample.c $(LIBS) -o TableauSDKSample-c

build-cpp : TableauSDKSample.cpp
	$(CXX) $(CXXFLAGS) $(LDFLAGS) TableauSDKSample.cpp $(LIBS) -o TableauSDKSample-cpp

build-both : build-c build-cpp

run-c : build-c
	./TableauSDKSample-c

run-cpp : build-cpp
	./TableauSDKSample-cpp

run-c-buildExtract : build-c
	./TableauSDKSample-c --build

run-cpp-buildExtract : build-cpp
	./TableauSDKSample-cpp --build

run-c-publishExtract : build-c
	./TableauSDKSample-c --publish

run-cpp-publishExtract : build-cpp
	./TableauSDKSample-cpp --publish
