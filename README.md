# Description

BlobCity DB is an All-in-One Database. It offers support for natively storing 17 different formats of data, including JSON, XML, CSV, PDF, Word, Excel, Log, GIS, Image amongst others. It run two full feature storage engines. One that stores data in memory and the other that stores data on disk. In-memeory storage offers sheer performance for real-time analytics, while the disk storage make BlobCity an excellent alternative for DataLakes.

BlobCity DB is a Gartner Cool Vendor 2016. [View Report](https://www.gartner.com/doc/3288923) 

# Sub Projects

* *Code Management Projects*:
    * **Launcher** - Starts up the database process
    * **Bean Manager** - Common project to allow cross-project bean sharing inside the database
    * **Distribution** - This module creates the release package of the BlobCity DB. It is only used by the build process and does not contain any real code.
* *End Point Projects*:
    * **TCP End Point** - Controls all exposed TCP end points for the database
    * **Web End Point** - Controls all exposed Web end points (REST over GET/POST) for the database
* *Database Code Projects*:
    * **Engine** - Core engine for the database
    
# Docs

Full technical docs for using BlobCity can be found at [https://docs.blobcity.com](https://docs.blobcity.com)

# Contribute

Your contributions are welcome. [Join our Slack community](https://join.slack.com/t/blobcity-community/shared_invite/enQtNDE1ODExNDIzMTUyLWI3Y2UxOWRjMDU1ZDQ3YjI0ZWQ0OWViODRkOTc4ZmZlN2M1MDE0ZjYxMzYyY2FkN2VlNTg0OGNmYzhlOGZkOWM) and request to become a contributor.

## Dos

* Follow Java standards for class, variable and method names
* Have Java documentation for your classes and methods. If you're generous, for your instance level variables too
* Ensure your documentation specifies all possible return values and exceptions (with reasons for throwing the exceptions)
* Project read me files are up to date
* Code is readable

## Don'ts

* Commit unformatted code
* Commit without appropriate commit messages
* Not follow the project structure and Spring life cycle
* Add unnecessary project dependencies

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
