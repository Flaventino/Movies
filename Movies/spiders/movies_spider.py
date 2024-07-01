import regex as re
import scrapy, requests
from urllib.parse import urljoin
from Movies.items import MoviesItem


class MoviesSpiderSpider(scrapy.Spider):
    name = "movies_spider"
    limit = 4 # To limit the number of movies to retrieve. None otherwise
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
        self.genre = self.concatenate(response.xpath(path('genre')).getall())

        # GET THE FULL LIST OF COUNTRIES
        self.country = self.concatenate(response.xpath(path('pays')).getall())

        # SCRAP MOVIES
        yield from self.parse_pages(response)

    # Subsection dedicated to pages parsing (i.e. where movies are listed)
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

            if self.limit and self.limit < self.n:
                stop = True
                break
            else:
                yield response.follow(movie_url, self.parse_movie)

        # LOOKS FOR A NEW PAGE WITH OTHER MOVIES TO SCRAP & MOVES TO IT IF ANY
        next_page = self.get_next_page(response)
        if next_page and not stop:
            yield response.follow(next_page, callback=self.parse_pages)

    def get_next_page(self, response):
        """
        Returns the new page url to follow. Returns None If no page exists.
        """

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

    # Subsection dedicated to scraping movies homepage.
    def parse_movie(self, response):
        """
        Parse a movie page to retrieve related data (title, synopsis, etc.)
        """

        # BASIC SETTINGS & INITIALIZATION OR FEATURES
        grab = lambda x: self.concatenate(response.xpath(x).getall())
        tech ="//section[contains(@class, 'technical')]"
        meta = "//div[contains(@class, 'card') and contains(@class, 'entity')]"
        casting_url = self.get_casting_url(response)
        #casting_url = grab("//a[contains(@title, 'Casting')]/@href")     # Old

        # IMPLEMENTING DATA PATHS FOR PURELY TEXT VALUES
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
        if not self.is_valid_url(casting_url):
            yield item
        else:
            yield scrapy.Request(url=casting_url,
                                 meta={'item': item},
                                 callback=self.parse_casting)

    # Subsection dedicated to scraping mmovies casting
    def get_casting_url(self, response):
        """
        Builds and returns the movie casting url.

        Several tryout showed that prasing a movie page to get its casting url
        is not ever possible but a pattern exists. This methods leverages that
        to return a catsing url which should work in any case...
        """

        # PARSES THE MOVIE DEDICATED PAGE TO GET ITS ID (allocine movie id)
        movie_id = re.sub('\D+', '', response.url)

        # FUNCTION OUTPUT
        return urljoin(response.url, f'/film/fichefilm-{movie_id}/casting/')

    def parse_casting(self, response):
        """
        Parse the cast page to retrieve casting data (people names and roles).
        """

        # RETRIEVES CASTING DATA
        path = "//section[contains(@class, 'casting-actor')]"
        casting = self.concatenate(response.xpath(path).getall())

        # UPDATES SCRAPY ITEM (i.e. movie item) WITH ITS CASTING DATA
        response.meta['item']['casting'] = casting

        # FUNCTION OUTPUT
        yield response.meta['item']

    # Miscellaneous subsection
    def is_valid_url(self, url: str):
        """
        Checks url validity by sending a `head` request. Returns a boolean.

        Parameter(s):
            url (str): url to be checked
        """

        # TESTING A `HEAD` REQUEST FAILS (if it fails, the url is not valid)
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def concatenate(self, list_or_set):
        """Changes collections into strings with a unique separator.

        Very important method since scraped collections (lists or sets) can be
        changed into strings with the same very identifiable and distinct
        separator '¤' (rare so efficient separator). The main purpose of this
        method is essentially to simplify further regex parsing."""

        return "¤".join(list_or_set)