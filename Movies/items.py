# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MoviesItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    # MOVIE
    title = scrapy.Field()              # Data scraped then cleaned in-place
    title_fr = scrapy.Field()           # Data scraped then cleaned in-place
    synopsis = scrapy.Field()           # Data scraped then cleaned in-place
    film_poster = scrapy.Field()        # Data scraped then cleaned in-place


    # MOVIE CREATORS
    creators = scrapy.Field()           # Scraped data
    directors = scrapy.Field()          # >> Data created after scraping stage
    screenwriters = scrapy.Field()      # >> Data created after scraping stage

    # MOVIE METADATA
    metadata = scrapy.Field()           # Scraped data
    categories = scrapy.Field()         # >> Data created after scraping stage
    runtime_min = scrapy.Field()        # >> Data created after scraping stage
    release_date = scrapy.Field()       # >> Data created after scraping stage
    release_place = scrapy.Field()      # >> Data created after scraping stage

    # MOVIE TECHNICAL DATA
    tech_data = scrapy.Field()          # Scraped data
    tech_headers = scrapy.Field()       # Scraped data
    visa = scrapy.Field()               # >> Data created after scraping stage
    types = scrapy.Field()              # >> Data created after scraping stage
    color = scrapy.Field()              # >> Data created after scraping stage
    budget = scrapy.Field()             # >> Data created after scraping stage
    awards = scrapy.Field()             # >> Data created after scraping stage
    languages = scrapy.Field()          # >> Data created after scraping stage
    distributors = scrapy.Field()       # >> Data created after scraping stage
    nationalities = scrapy.Field()      # >> Data created after scraping stage
    production_year = scrapy.Field()    # >> Data created after scraping stage

    # MOVIE RATINGS
    ratings = scrapy.Field()            # Scraped data
    press_rating = scrapy.Field()       # >> Data created after scraping stage
    public_rating = scrapy.Field()      # >> Data created after scraping stage

    # MOVIE CASTING
    casting = scrapy.Field()            # Data scraped then cleaned in-place