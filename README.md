# Movies

## Project description
The purpose of this repository is mainly to explore scrapy features, database storage as well as cloud interactions (azure) and all together.

To do so we will try to scrap a very popular movie website (allocine.fr) in order to retrieve data on movies (series will be implemented later or not).

**First stage**
Will focus on the scraper only. In other words learning how to use scrapy basics. We'll restrict the number of scrapped items to ensure all is working fine (i.e. we retrieve and clean data successfuly)

**Second stage**
Will consist in creating a database to be used in place of (or concurrently to) the csv file.
  * Purpose here will mainly be to learn and put in practice the `SQLAlchemy` python library 
  * For ease we will use an sqlite database wich offers the great advantages of being:
    * Natively available in python environments (i.e. no pip install required)
    * Very light and quite fast.
    * Perfect for quick tests on local hosts.

**Third stage**
Will consist in using a postgre database in place of the sqlite one (see above). That part of the project will be the moment to try cloud interactions, together with the work done so far, since the postgreSQL database we will create, and play with, will be hosted on azure cloud.

**Fourth stage** (and probably the last one)
will be to move our scraper from the local machine to an azure server as well. The obvious purpose of doing so is that the scrapper, in principle, is not intended to be run on a local computer but on a cloud server, at any time or on a scheduled basis.
  * This will be the moment to explore azure ressources in order to get the o,e required to execute scrapping as expexcted. At this moment nothing is decided: Azure function ? Data factory ? Azure Container registry (ACR + ACI) together with a Docker container ?

## Project status
> Which Stage is the project ?
* At this moment (2024/07/06) the second stage is fully complete and available (on `main` branch). As for the third stage it is under development (on `development` branch).
* At this stage, aprt from being available on a csv file (provided the good command is run. See here after) the scraped data are available in a locally hosted database of `sqlite` type but not on a the cloud yet.

> How to run the scrapper ?
Running the scraper is quite easy:
  1. Going to main directory (i.e. `Movies` directory where you also will find The 'README.md' file as well as a sub direcctory called `Movies` too)
  2. Gessing poetry environment is already installed and `poetry shell`is active exceute: `scrapy crawl movie_spider -O data.csv`
**n.b :** "-O data.csv" is just a way to get a csv file and be sure the file is overwritten if it already exists.
