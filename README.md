# Movies

## Project description

The purpose of this repository is mainly to explore scrapy features, database storage as well as cloud interactions (azure), all with github.

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
Will be to move the scraper from the local machine to an azure server as well. The obvious purpose of doing so is that the scrapper, in principle, is not intended to be run on a local computer but on a cloud server, at any time or on a scheduled basis.

* This will be the moment to explore azure ressources in order to execute scrapping as expected. At the moment I am writing that lines, nothing is decided: Azure function ? Data factory ? Azure Container registry (ACR + ACI) together with a Docker container, something else ?

## Project status

> Which Stage is the project ?

* At this moment (2024/07/06) the second stage is fully complete and available (on `main` branch). As for the third stage it is under development (on `development` branch).
* At this stage, aprt from being available on a csv file (provided the good command is run. See here after) the scraped data are available in a locally hosted database of `sqlite` type but not on a the cloud yet.

> How to run the scrapper ?

1. Going to main directory (i.e. `Movies` directory where you also will find The 'README.md' file as well as a sub direcctory called `Movies` too)
2. Gessing poetry environment is already installed and `poetry shell`is active exceute: `scrapy crawl movie_spider -O data.csv`
   * n.b : *"-O data.csv" is just a way to get a csv file and be sure the file is overwritten if it already exists.*


<!-- ```mermaid
erDiagram
    CUSTOMER ||--o{ ORDER : places
    CUSTOMER {
        string name
        string custNumber
        string sector
    }
    ORDER ||--|{ LINE-ITEM : contains
    ORDER {
        int orderNumber
        string deliveryAddress
    }
    LINE-ITEM {
        string productCode
        int quantity
        float pricePerUnit
    }

``` -->



```mermaid
erDiagram
    CUSTOMER ||--o{ ORDER : places
    CUSTOMER {
      string name
      string custNumber
      string sector
    }
    CUSTOMER }|..|{ DELIVERY-ADDRESS : uses
    ORDER ||--|{ LINE-ITEM : contains

## Schema
<!-- BEGIN_SQLALCHEMY_DOCS -->
```mermaid
  erDiagram
    movies {
      INTEGER Id PK
      VARCHAR Awards "nullable"
      VARCHAR Budget "nullable"
      VARCHAR Category "nullable"
      INTEGER Duration "nullable"
      VARCHAR Format "nullable"
      VARCHAR Poster_URL "nullable"
      NUMERIC(2, 1) Press_Rating "nullable"
      INTEGER Production_Year "nullable"
      NUMERIC(2, 1) Public_Rating "nullable"
      DATE Release_Date "nullable"
      VARCHAR Release_Place "nullable"
      VARCHAR Synopsis "nullable"
      VARCHAR Title "nullable"
      VARCHAR Title_Fr
      VARCHAR Visa "nullable"
    }

    persons {
      INTEGER Id PK
      VARCHAR Full_Name
    }

    actors {
      INTEGER MovieId PK,FK
      INTEGER PersonId PK,FK
      VARCHAR Characters "nullable"
    }

    directors {
      INTEGER MovieId PK,FK
      INTEGER PersonId PK,FK
    }

    screenwriters {
      INTEGER MovieId PK,FK
      INTEGER PersonId PK,FK
    }

    companies {
      INTEGER Id PK
      VARCHAR Full_Name
    }

    distributors {
      INTEGER CompId PK,FK
      INTEGER MovieId PK,FK
    }

    genres {
      VARCHAR Genre PK
      INTEGER MovieId PK,FK
    }

    countries {
      VARCHAR Country PK
      INTEGER MovieId PK,FK
    }

    languages {
      VARCHAR Language PK
      INTEGER MovieId PK,FK
    }

    movies ||--o{ actors : MovieId
    persons ||--o{ actors : PersonId
    movies ||--o{ directors : MovieId
    persons ||--o{ directors : PersonId
    movies ||--o{ screenwriters : MovieId
    persons ||--o{ screenwriters : PersonId
    movies ||--o{ distributors : MovieId
    companies ||--o{ distributors : CompId
    movies ||--o{ genres : MovieId
    movies ||--o{ countries : MovieId
    movies ||--o{ languages : MovieId

```
<!-- END_SQLALCHEMY_DOCS -->