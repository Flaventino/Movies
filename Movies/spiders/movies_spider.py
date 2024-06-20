import scrapy


class MoviesSpiderSpider(scrapy.Spider):
    name = "movies_spider"
    limit = 6 # To limit the number of movies to retrieve. None otherwise
    start_urls = ["https://allocine.fr/films/"]
    allowed_domains = ["allocine.fr"]

    def parse(self, response):
        """Navigates between movies pages"""

        # BASIC SETTINGS & INITIALIZATION
        stop = False
        movies = response.xpath("//li[@class='mdl']")
        self.n = 0 if not hasattr(self, 'n') else self.n

        # print('##################################################')
        # print(f"LONGUEUR DE 'movies'{len(movies)}")
        # print('##################################################')

        # EXPLORES EACH MOVIE PAGE & RETRIEVES RELATED DATA
        for movie in movies:
            self.n += 1
            movie_url = movie.xpath('.//h2/a/@href').get()
            # print('##################################################')
            # print(movie_url)
            # print('##################################################')
            # # print(type(movie_url))
            # # print('##################################################')
            # # movie_url = movie_url[5:]
            # # print(movie_url)
            # # print('##################################################')

            if self.limit and self.limit <= self.n:
                stop = True
                print("#####################################################")
                print(F"{self.limit}/{self.n} -- BREAK")
                print("#####################################################")
                break
            else:
                print("#####################################################")
                print("MOUCHARD DE TEST")
                print("#####################################################")
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
        numbers = [int(number) for number in numbers if number.isnumeric()]
        last_id = max(numbers)

        # UPDATES 'url' IF REQUIRED
        if page_id <= last_id:
            url = f'{self.start_urls[0]}/?page={page_id}'

        # FUNCTION OUTPUT
        return url


    def parse_movie(self, response):
        """
            Parse a movie page to retrieve related data (title, synopsis, etc.)
        """

        # BASIC SETTINGS & INITIALIZATION
        meta = "//div[contains(@class, 'card') and contains(@class, 'entity')]"

        # IMPLEMENTATING DATA PATHS FOR PURELY TEXT VALUES
        paths = {
            'Title' : f"{meta}//div[@class='meta-body-item']",
            'Title_fr': "//h1",
            'Synopsis': "//section[starts-with(@id, 'synopsis')]//p",
            'Creators': f"{meta}//div[contains(@class, 'oneline')]",
            'MetaData': f"{meta}//div[contains(@class, 'info')]",
            'Techinal': "//section[contains(@class, 'technical')]",
            'MovieScores': f"{meta}//div[contains(@class, 'rating')]"}

        # IMPLEMENTING DATA PATHS FOR TAG ATTRIBUTES
        attributes = {'MoviePoster': f"{meta}//figure//img/@src"}

        # DATA SCRAPING
        grab = lambda x: '¤'.join(response.xpath(x).getall())
        data = {key: grab(f'{path}/text()') for key, path in paths.items()}
        data.update({key: grab(path) for key, path in attributes.items()})

        # FUNCTION OUTPUT
        yield data