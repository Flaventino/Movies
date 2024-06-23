# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MoviesItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    # MOVIE
    title = scrapy.Field()
    title_fr = scrapy.Field()
    synopsis = scrapy.Field()
    film_poster = scrapy.Field()


    # MOVIE CREATORS
    creators = scrapy.Field()
    directors = scrapy.Field()
    screenwriters = scrapy.Field()

    # MOVIE METADATA
    metadata = scrapy.Field()
    duration = scrapy.Field()
    categories = scrapy.Field()
    release_date = scrapy.Field()
    availability = scrapy.Field()

    # MOVIE TECHNICAL DATA
    technical = scrapy.Field()
    visa = scrapy.Field()
    types = scrapy.Field()
    color = scrapy.Field()
    budget = scrapy.Field()
    awards = scrapy.Field()
    languages = scrapy.Field()
    distributors = scrapy.Field()
    nationalities = scrapy.Field()
    production_year = scrapy.Field()

    # MOVIE RATINGS
    ratings = scrapy.Field()
    press_rating = scrapy.Field()
    public_rating = scrapy.Field()

    # MOVIE CASTING
    casting = scrapy.Field()