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

    # METHODS OF THE RELATED SPIDER INSTANCES
    def parse(self, response):
        """
        The purpose here is to drive the scraping process
        """

        # GET THE FULL LIST OF MOVIE GENRES
        path = "//ul[contains(@data-name, '{}')]//text()".format
        #genres_path = "//ul[contains(@data-name, 'genre')]//text()"
        #self.genres = '¤'.join(response.xpath(genres_path).getall())
        self.genre = '¤'.join(response.xpath(path('genre')).getall())
        # print('##############################################################')
        # print('GENRES DE FILMS DEPUIS "PARSE"')
        # print(dir(response))
        # print(response.url)
        # print(path("'GASTON'"))
        # print(self.genre[:20])
        # print('##############################################################')

        # GET THE FULL LIST OF COUNTRIES
        #countries_path = "//ul[contains(@data-name, 'pays')]//text()"
        # self.countries = '¤'.join(response.xpath(countries_path).getall())
        self.country = '¤'.join(response.xpath(path('pays')).getall())
        # print('##############################################################')
        # print('PAYS DEPUIS "PARSE"')
        # print(path('GASTON'))
        # print(self.country[:20])
        # print('##############################################################')

        # SCRAP MOVIES
        yield from self.parse_pages(response)

    def parse_pages(self, response):
        """
        Navigates one mmovie listing page to another.
        """

        # BASIC SETTINGS & INITIALIZATION
        stop = False
        movies = response.xpath("//li[@class='mdl']")
        self.n = 0 if not hasattr(self, 'n') else self.n

        # EXPLORES EACH MOVIE DEDICATED PAGE & RETRIEVES RELATED DATA
        for movie in movies:
            self.n += 1
            movie_url = movie.xpath('.//h2/a/@href').get()

            if self.limit and self.limit <= self.n:
                stop = True
                break
            else:
                yield response.follow(movie_url, self.parse_movie)

        # LOOKS FOR A NEW PAGE WITH OTHER MOVIES TO SCRAP & MOVES TO IT IF ANY
        next_page = self.get_next_page(response)
        if next_page and not stop:
            yield response.follow(next_page, callback=self.parse_pages)

    def parse_movie(self, response):
        """
        Parse a movie page to retrieve related data (title, synopsis, etc.)
        """

        # BASIC SETTINGS & INITIALIZATION OR FEATURES
        grab = lambda x: '¤'.join(response.xpath(x).getall())
        tech ="//section[contains(@class, 'technical')]"
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
            'tech_data': f"{tech}//div[@class='item']",
            'tech_headers': f"{tech}//span[contains(@class, 'light')]"}

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