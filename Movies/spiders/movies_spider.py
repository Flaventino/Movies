import scrapy
from urllib.parse import urljoin
from Movies.items import MoviesItem


class MoviesSpiderSpider(scrapy.Spider):
    name = "movies_spider"
    limit = 6 # To limit the number of movies to retrieve. None otherwise
    start_urls = ["https://allocine.fr/films/"]
    allowed_domains = ["allocine.fr"]


    # CUSTOM SCRAPPING SETTINGS
    custom_settings = {
        'LOG_LEVEL': 'WARNING'} # Adjust logging level not to overload console}

    # METHODS OF THE RELATED SPIDERS INSTANCES
    def parse(self, response):
        """
        Navigates between movies pages
        """

        # BASIC SETTINGS & INITIALIZATION
        stop = False
        movies = response.xpath("//li[@class='mdl']")
        self.n = 0 if not hasattr(self, 'n') else self.n

        # EXPLORES EACH MOVIE PAGE & RETRIEVES RELATED DATA
        for movie in movies:
            self.n += 1
            movie_url = movie.xpath('.//h2/a/@href').get()

            if self.limit and self.limit <= self.n:
                stop = True
                break
            else:
                yield response.follow(movie_url, self.parse_movie)

        # LOOKS FOR A POTENTIAL NEW PAGE TO EXPLORE AND MOVE TO IT IF ANY
        next_page = self.get_next_page(response)
        if next_page and not stop:
            yield response.follow(next_page, callback=self.parse)


    def parse_movie(self, response):
        """
        Parse a movie page to retrieve related data (title, synopsis, etc.)
        """

        # BASIC SETTINGS & INITIALIZATION OR FEATURES
        grab = lambda x: '¤'.join(response.xpath(x).getall())
        meta = "//div[contains(@class, 'card') and contains(@class, 'entity')]"
        casting_url = grab("//a[contains(@title, 'Casting')]/@href")

        # IMPLEMENTATING DATA PATHS FOR PURELY TEXT VALUES
        paths = {
            'title' : f"{meta}//div[@class='meta-body-item']",
            'ratings': f"{meta}//div[contains(@class, 'rating')]",
            'title_fr': "//h1",
            'synopsis': "//section[starts-with(@id, 'synopsis')]//p",
            'creators': f"{meta}//div[contains(@class, 'oneline')]",
            'metadata': f"{meta}//div[contains(@class, 'info')]",
            'technical': "//section[contains(@class, 'technical')]"}

        # IMPLEMENTING DATA PATHS FOR TAG ATTRIBUTES
        attributes = {'film_poster': f"{meta}//figure//img/@src"}

        # RETRIEVING MOVIE GENERAL DATA
        data = {key: grab(f'{path}//text()') for key, path in paths.items()}
        data.update({key: grab(path) for key, path in attributes.items()})

        # INSTANCIATION OF A 'MovieItem' FINALLY FILLED WITH THE SCRAPED DATA
        item = MoviesItem(**data)

        # RETRIEVING MOVIE CASTING DATA IF AVAILABLE
        if not casting_url:
            yield item
        else:
            casting_url = urljoin(response.url, casting_url)
            yield scrapy.Request(url=casting_url,
                                 meta={'item': item},
                                 callback=self.parse_casting)


    def parse_casting(self, response):
        """
        Parse the cast page to retrieve casting data.
        """

        # RETRIEVES CASTING DATA
        path = "//section[contains(@class, 'actor')]//text()"
        casting = "¤".join(response.xpath(path).getall())

        # UPDATES MOVIE DATA WITH ITS CASTING DATA
        #response.meta['data'].update({'casting': casting}) # Old version
        response.meta['item']['casting'] = casting

        # print("##########################################################")
        # #for key, value in response.meta['data'].items():
        # for key, value in response.meta['item'].items():
        #     print(f'{key}:\n§{repr(value)}§')
        #     print()
        #     print()
        # #print(dir(response.meta['item']))
        # #print(response.meta['item'])
        # print("##########################################################")
        # print("-----------------------------------------------------------")

        # FUNCTION OUTPUT
        #yield response.meta['data'] # Old version (when item not implemented)
        yield response.meta['item']


    def get_next_page(self, response):
        """Returns the new page url to follow or none"""

        # BASIC SETTINGS & INITIALIZATION
        url = None
        current = "[contains(@class, 'current')]"
        hub_path = "//nav[starts-with(@class, 'pag')]/div/span{}/text()".format

        # RETRIEVES THE CURRENT PAGE ID (i.e. current page number)
        page_id = 1 + int(response.xpath(hub_path(current)).get().strip())

        # GET THE ID OF THE VERY LAST AVAILABLE PAGE
        numbers = response.xpath(hub_path('')).getall()
        numbers = [number.strip() for number in numbers]
        numbers = [int(number) for number in numbers if number.isnumeric()]
        last_id = max(numbers)

        # UPDATES 'url' IF REQUIRED
        if page_id <= last_id:
            url = f'{self.start_urls[0]}/?page={page_id}'

        # FUNCTION OUTPUT
        return url