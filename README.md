# Movies

Purpose of this repo is to explore scrapy features, database storage and cloud interactions (azure storage, azure data factory) all together.

* To do so we will try to scrap a very popular movie web site (allocine.fr) in order to retrieve data on movies (series will be implemented later).
* The first stage will consist in limiting the number of scrapped item to ensure all is working fine and record results in a csv file.
  * The scraper will get an attribute called 'limit' in order to control the number of movies to scrap.
  * Simply Set this parameter to None to scrap all movies
* The second stage will consist in creating a database to be used in place of (or concurrently to) the csv file (which need special run to be created. See below)
  * Purpose here is mainly to learn and put in practice the python library called "SQLAlchemy"
  * For ease we will use an sqlite database
* The third stage will consist in using a postgre database in place of the sqlite one (see above).
  * This will be the moment to try cloud interactions and particularly databases off course.
* The fourth (and probably last stage) will be to place the whole project on a azure server so that running it can be triggered.
  * This will be the moment to explore azure data factory other related ressources in order to design and run a pipeline on a scheduled way.

N.B :

- At this moment (2024/07/06) the third stage of the project is on progress.
- So at this stage, scraped data are available in a locally hosted database of `sqlite` type but not on a the cloud.

`<h2>` RUNING THE SCRAPER Running the scraper is quite easy.

1. Going to main directory (i.e. `Movies` directory where you also will find a 'Movies' sub directory as well as 'README.md' and 'pyproject.toml' for instance)
2. Gessing poetry environment is already installed and its shell active, type :
   scrapy crawl movie_spider -O data.csv

   n.b : "-O data.csv" is just a way to get a csv file and be sure the file is overwritten if it already exists.
