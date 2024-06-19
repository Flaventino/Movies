import scrapy


class MoviesSpiderSpider(scrapy.Spider):
    name = "movies_spider"
    limit = 5 # To limit the number of movies to retrieve. None otherwise
    start_urls = ["https://allocine.fr/films/"]
    allowed_domains = ["allocine.fr"]

    def parse(self, response):
        """Navigates between movies pages"""

        # BASIC SETTINGS & INITIALIZATION
        stop = False
        movies = response.xpath(f"//li[@class='mdl']")
        self.n = 0 if not hasattr(self, 'n') else self.n

        # EXPLORES EACH MOVIE PAGE & RETRIEVES RELATED DATA
        for movie in movies:
            self.n += 1
            movie_url = movie.xpath('.h2/a/@href').get()

            if self.limit and self.limit >= self.n:
                stop = True
                break
            else:
                yield response.follow(movie_url, self.parse_movie)

        # LOOKS FOR A POTENTIAL NEW PAGE TO EXPLORE
        next_page = self.get_next_page(response)
        if next_page and not stop:
            yield response.follow(next_page, callback=self.parse)


    def get_next_page(self, response):
        """Returns the new page url to follow or None"""

        # BASIC SETTINGS & INITIALIZATION
        url = None
        current = "[contains(@class, 'current')]"
        hub_path = "//nav[starts-with(@class, 'pag')]/div/span{}/text()".format

        # RETRIEVES THE CURRENT PAGE ID (i.e. current page number)
        page_id = 1 + int(response.xpath(hub_path(current)).get().strip())

        # GET THE ID OF THE VERY LAST AVAILABLE PAGE
        numbers = response.xpath(hub_path('')).getall()
        numbers = [number.strip() for number in numbers]
        last_id = max([int(number) for number in numbers if id.isnumeric()])

        # UPDATES 'url' IF REQUIRED
        if page_id <= last_id:
            url = f'{self.start_urls[0]}/?page={page_id}'

        # FUNCTION OUTPUT
        return url

    def parse_movie(self, response):
        pass