import dateparser, nltk, regex as re
import sqlalchemy.exc as alchemyError
from Databases import queries, schema
from itemadapter import ItemAdapter

# DOWNLOAD THE STOPWORDS CORPUS FROM NLTK
nltk.download('stopwords')

# IMPLEMENTING THE `CLEANING PIPELINE`
class MovieScraperPipeline:
    """
    PIPELINE DEDICATED TO SCRAPED DATA CLEANING
    """
    # Making a python `set` of french words to stop (i.e. drop)
    fr_stopset = set(nltk.corpus.stopwords.words('french'))

    # SETTER METHODS
    def get_spider_attr(self, spider):
        """
        Clones `genre` and `country` attributes of a `movies_spider` instance.

        The purpose is not only to get the said spider's attributes but also to
        clean them and set them as new attributes for the current instance of
        `MovieScraperPipeline`.
        
        Parameter(s):
            spider (movies_spider): The movies_spider instance to target.
        """

        # CLONING AND CLEANING PROCESS
        for attribute in ['genre', 'country']:
            # CLEANING STAGE
            values = self.flatten(getattr(spider, attribute))
            values = self.get_all(regex=r'[\p{L}\s-]+', string=values)

            # CLONING STAGE
            setattr(self, attribute, values)


    # GENERAL AND/OR COMMON DATA CLEANING METHODS
    def get_first(self, regex, string):
        """
        Parses given string with given regex. Returns 1st match or None.

        Parameter(s):
            regex  (str): Regex expression to use to parse the given string.
            string (str): String to be parsed in search of the given regex.
        """

        # EXTRACTION PROCESS
        string = re.search(regex, string)

        # FUNCTION OUTPUT
        return string.group() if string else None

    def get_all(self, regex: str, string: str, unique : bool = True):
        """
        Applies `re.findall` with given Parameter(s) and returns enhanced result.

        Once words are ectracted, according the given regex, each item of the
        resulting list is stripped then discarded if its length is null.
        The underlying regex engine used here is : `regex`

        Parameter(s):
            regex   (str): Regular expression to use with `regex.findall`
            string  (str): String to be parsed
            unique (bool): Whether to return all extracted words or a `set`.
                           Default value is True

        Returns: Either a list or a `set` (possibly empty, depending on regex)
        """

        # PARSING PROCESS (extraction and cleaning)
        string = re.findall(regex, string)              # Gets groups of words
        string = [item.strip(' ¤') for item in string]  # Removes extra spaces
        string = [x for x in string if len(x) > 0]      # Keeps relevant groups

        # FUNCTION OUTPUT
        return set(string) if unique else string

    def flatten(self, txt: str, commas: bool = True):
        """
        Removes controls characters, commas (if non numeric), and extra spaces.

        Parameter(s):
            txt     (str): String to be cleaned
            commas (bool): Whether to drop any commas (float numbers ignored)

        Returns: A clean string (cleaned version of the given string)
        """

        # SETTING UP REGEXES TO BE APPLIED SUCCESSIVELY
        regexes = [r'(?<!\d),(?!\d)'] if commas else [] # Commas management
        regexes += [r'[^ \S]', r'\s*¤+\s*', r'¤+\W*¤+'] # Ctrl & Irrelev. chars

        # FLATTENING & CLEANING PROCESS:
        for regex in regexes:
            txt = re.sub(regex, '¤', txt)

        # FUNCTION OUTPUT
        return re.sub(r'\s+', ' ', txt) # Returns flatten withiut extra spaces


    # METHODS DEDICATED TO ITEM CLEANING
    def process_item(self, item, spider):
        """
        MONITORING FUNCTION TO DRIVE THE CLEANING PROCESS OF SCRAPED DATA.
        """

        # BASIC SETTINGS & INITIALIZATION
        self.adapter = ItemAdapter(item)
        _ = None if hasattr(self, 'genre') else self.get_spider_attr(spider)

        # DATA CLEANING PIPELINE
        self.clean_titles()      # Cleaning of the french and original titles
        self.clean_synopsis()    # Synopsis cleaning
        self.clean_film_poster() # Movie poster (get clean url)
        self.clean_creators()    # Extract directors and writers
        self.clean_metadata()    # Extract release date and place, genres, etc.
        self.clean_technnical()  # Extract distributors, origin, languages etc.
        self.clean_ratings()     # Extract Press and Public ratings
        self.clean_casting()     # Extracts actor names and related role(s)


        # print("##############################################################")
        # #for key, value in response.meta['data'].items():
        # for key, value in item.items():
        #     # if value:
        #     print(f'{key}:\n{repr(value)}')
        #     try:
        #         print(f" >>> {self.flatten(self.adapter.get(key))}")
        #     except:
        #         print("None or Error!")
        #     print()
        # #print(dir(response.meta['item']))
        # #print(response.meta['item'])
        # print("##############################################################")
        # print("--------------------------------------------------------------")

        return item

    def clean_titles(self):
        # CLEANING PROCESS (applied to each given title field)
        for field in ['title', 'title_fr']:
            # GET FIELD TO CLEAN AND FLATTEN IT
            title = self.flatten(self.adapter.get(field))

            # STRIP THE ABOVE RESULT ON RIGHT SIDE (drops spaces and "¤")
            title = re.sub(r'[\s¤]*$', '', title)

            # EXTRACT THE MOVIE TITLE AND STRIP IT ON THE LEFT SIDE
            title = self.get_first(regex=r'[\p{L}\d\p{P}\s]+$', string=title)
            title = re.sub(r'^[\s¤]*', '', title) if title else None

            # UPDATING THE ITEM'S FIELD WITH A CLEAN VALUE OR SIMPLY NONE.
            self.adapter[field] = title

        # CHECK MOVIE TITLE IN FRANCE
        if not self.adapter.get('title_fr'):
            self.adapter['title_fr'] = self.adapter.get('title')

    def clean_synopsis(self):
        """
        Applies a basic cleaning on scraped data. Returns a string or None.

        Basic cleaning consists in applying `flatten` methods (see docstring).
        If ever the scraping process did not find synopsis a None is returned. 
        """

        # BASIC CLEANING PROCESS
        synopsis = self.flatten(self.adapter.get('synopsis'), commas=False)
        synopsis = synopsis.strip(" ,¤")

        # FUNCTION OUTPUT
        self.adapter['synopsis'] = synopsis if synopsis else None

    def clean_film_poster(self):
        # INITIALIZATION
        field, regex = 'film_poster', r'http.+\.jpg'

        # CLEANING PROCESS
        self.adapter[field] = self.get_first(regex, self.adapter.get(field))

    def clean_creators(self):
        """
        This method extracts the list of directors and that of screenwriters.
        """

        # BASIC SETTINGS & INITIALIZATION
        field = 'creators'

        # CLEANS 'creators' FIELD IN-PLACE BEFORE EXTRACTING DETAILED DATA
        self.adapter[field] = self.flatten(self.adapter.get(field))

        # EXTRACTS SCREENWRITER NAMES FROM 'creators' (raw data to be cleaned)
        makers = self.adapter.get(field)
        writers = self.get_first(r'(?i)(?<=¤+\s*par\s*¤+).*$', makers)

        # EXTRACTS DIRECTORS NAMES FROM 'creators' (ras data to be cleaned)
        directors = re.sub(r'(?i)¤+\s*par\s*¤+.*$', '', makers)
        directors = self.get_first(r'(?i)(?<=¤+\s*de\s*¤+).*$', directors)

        # CLEANING OF SCREENWRITER AND DIRECTOR NAMES + IN-PLACE BACKUP
        makers = {'directors': directors, 'screenwriters': writers}
        for field, names in makers.items():
            # CLEANING PROCESS
            names = self.get_all(r'[\p{L}\s]+', names if names else '')

            # IN-PLACE BACKUP
            self.adapter[field] = "¤".join(names) if names else None

    def clean_ratings(self):
        """
        Extracts movie ratings (press & public) and reformats it.
        """

        # BASIC SETTINGS & INITIALIZATION
        master = 'ratings'
        regexp = r'(?i)(?<={}\W+)\d+[.,]{{0,1}}\d*'.format
        fields = {'press_rating': 'presse', 'public_rating': 'spectateurs'}

        # CLEANS 'ratings' FIELD IN-PLACE BEFORE EXTRACTING DETAILED DATA
        self.adapter[master] = self.flatten(self.adapter.get(master))

        # EXTRACTING & CLEANING PROCESS + SCRAPY ITEM UPDATE (field by field)
        for field, header in fields.items():
            rating = self.get_first(regexp(header), self.adapter.get(master))
            rating = float(re.sub(r',', '.', rating)) if rating else None
            self.adapter[field] = rating


    # Sub section dedicated to `metadata` cleaning
    def clean_metadata(self):
        """
        Parses metadata to get clean date, medium, duration and genre of movie.
        """

        # BASIC SETTINGS & INITIALIZATION
        field = 'metadata'

        # CLEANS 'metadata' FIELD IN-PLACE BEFORE EXTRACTING DETAILED DATA
        self.adapter[field] = self.flatten(self.adapter.get(field))

        # EXTRACTION & CLEANING PROCESS
        backup = self.adapter.get(field) # Creates raw data backup (see below)
        self.get_movie_date()            # Retrieves and reformat release date
        self.get_runtime()               # Retrieves and reformat movie runtime
        self.get_genres()                # Retrieves all genres
        self.get_place()                 # Rectrieves release place

        # RECOVERS ORIGINAL 'metadata' FIELD (for post processing check only)
        self.adapter[field] = backup

    def get_movie_date(self):
        """
        Extracts the release date from the scraped data and reformats it (min).

        Additionnaly, the 'metadata' field is realtime updated to drop the
        freshly extracted date making subsequent extractions easier. 
        """

        # BASIC SETTINGS & INITIALIZATION
        meta, isodate = 'metadata', '%Y/%m/%d'

        # MOVIE RELEASE DATE - STAGE 1 - Extracting alphanumeric date
        regx = r'\d+\s*\p{L}+\s*\d{4}'                      # Date pattern
        date = self.get_first(regx, self.adapter.get(meta)) # Get date or none

        # MOVIE RELEASE DATE - STAGE 2 - Reformating date + scrapy Item update
        date = dateparser.parse(date) if date else None # Creates a date parser
        date = date.strftime(isodate) if date else None # Changes date format
        self.adapter['release_date'] = date             # Update scrapy item

        # UPDATE 'metadata' FIELD (For easier subsequent cleaning process only)
        self.adapter[meta] = re.sub(regx, '', self.adapter.get(meta))

    def get_runtime(self):
        """
        Extracts movie duration from scraped data, parses and reformats it.

        Additionnaly, the 'metadata' field is realtime updated to drop the
        freshly extracted duration making subsequent extractions easier.
        """

        # BASIC SETTINGS & INITIALIZATION
        meta = 'metadata'

        # MOVIE DURATION - STAGE 1 - Extracting alphanumeric duration
        regx = r'(?i)\d+\s*h\s*\d+[\p{L}\s]*(?=¤)'          # Duration pattern
        time = self.get_first(regx, self.adapter.get(meta)) # Get time or none

        # MOVIE DURATION - STAGE 2 - Reformating duration
        if time:
            expr = r'(?i)(?<=\d+)\s*h\s*0*'         # Regex to match 'h'
            time = re.sub(expr, '*60+', time)       # 'h' becomes '*60'
            time = re.sub(r'[\p{L}\s]*', '', time)  # Drops any leters & spaces
            time = int(eval(time))                  # Computes length (minutes)

        # MOVIE DURATION - STAGE 2 - Scrapy Item update
        self.adapter['runtime_min'] = time         # Update scrapy item

        # UPDATE 'metadata' FIELD (For easier subsequent cleaning process only)
        self.adapter[meta] = re.sub(regx, '', self.adapter.get(meta))

    def get_genres(self):
        """
        Retrieves all genres of the movie being scraped.

        Additionnaly, the 'metadata' field is realtime updated to drop the
        freshly extracted duration making subsequent extractions easier.

        -- ALL POSSIBLE GENRE LISTS MUST HAVE BEEN SCRAPED BEFORE --
        """
        # BASIC SETTINGS & INITIALIZATION
        meta, field = 'metadata', 'categories'

        # MOVIE GENRES - Extraction + scrcapy Item update
        regex = r"|".join(self.genre)                      # Genres pattern
        genres = re.findall(regex, self.adapter.get(meta)) # Get time or none
        genres = '¤'.join(set(genres)) if genres else None # Merging result
        self.adapter[field] = genres if genres else None   # Saving

        # UPDATE 'metadata' FIELD (For easier subsequent cleaning process only)
        self.adapter[meta] = re.sub(regex, '', self.adapter.get(meta))

    def get_place(self):
        """
        Retrieves the release place(s) of the movie being scraped.
        """

        # BASIC SETTINGS & INITIALIZATION
        md = 'metadata'

        # REMOVES ANY NON RELEVANT WORDS FROM RAW DATA (i.e. drops stop words)
        regx = '(?:^|(?<=\W)){}(?=\W|$)'.format                   # Unit regex
        regx = "|".join([regx(word) for word in self.fr_stopset]) # Full regex
        self.adapter[md] = re.sub(regx, '', self.adapter.get(md)) # Drops words

        # EXTRACTING RELEASE PLACE(S)
        places = self.get_all(regex=r'[\p{L}\s]+', string=self.adapter.get(md))

        # UPDATE OF THE SCRAPY ITEM
        self.adapter['release_place'] = '¤'.join(places) if places else None

    # Sub section dedicated to `technical` data cleaning
    def clean_technnical(self):
        """
        Extracts technical details from scraped data (origin, distributor, etc).
        """

        # BASIC SETTINGS & INITIALIZATION
        data = 'tech_data'
        heads = 'tech_headers'
        fields = {'visa': 'visa',
                  'types': 'film',
                  'color': 'couleur',
                  'budget': 'budget',
                  'awards': 'r.compense',
                  'languages': 'langue',
                  'distributors': 'distributeur',
                  'nationalities': 'nation',
                  'production_year': 'ann.e'}

        # IN-PLACE CLEANING OF SCRAPED DATA BEFORE DETAILS EXTRACTION
        # >>> Remmoves any controls, comas (if not numeric), and extra spaces
        for field in (data, heads):
            self.adapter[field] = self.flatten(self.adapter.get(field))

        # GETS CLEAN HEADERS SET
        # >>> To make the recovery of related data easier and more robust
        headers = self.get_all(r'[^¤]*', self.adapter.get(heads)) # Headers set

        # EXTRACTION & CLEANING PROCESS FIELD BY FIELD
        regex = r'(?i)(?:^|[^¤])*{}(?:[^¤]|$)*'.format
        regmask = lambda x: "|".join(header for header in (headers - set([x])))
        for field, header in fields.items():
            # EXTRACTS AND CLEANS THE HEADER ASSOCIATED WITH THE CURRENT FIELD
            header = self.get_first(regex(header), self.adapter.get(heads))

            # REPLACES ALL HEADERS BY '§' EXCEPT THE ONE BEING PROCESSED 
            value = re.sub(regmask(header), '§', self.adapter.get(data))

            # EXTRACTS DATA RELATED TO CURRENTLY PROCESSED FIELD (I.E. HEADER)
            value = self.get_first(f'(?<={header})[^§]*', value)     #Gets data
            value = self.get_all(r'[^¤]*', value) if value else []   #Cleaning

            # UPDATING SCRAPY ITEM
            self.adapter[field] = '¤'.join(value) if value else None

        # ADDITIONAL STAGES (to be implemented)
        # Dissociating `Budget` amount from the currency >> dedicated method
        # Decomposing `color`. For instance some movies are "Couleur et N/B"
        # etc.

    # Sub section dedicated to `casting` data cleaning
    def clean_casting(self):
        """
        Leverages scraped date related to the casting to get actors & roles.

        NB: Actors and roles are a litle part of scraped.
        """

        # BASIC SETTINGS & INITIALIZATION
        field, roles = 'casting', {}
        del_role_txt = r'(?i)r[oô]le\s*:*\s*'
        actors_regex = r'(?i)(?<=link[^>]*>)[^<]*'
        roles_regexp = r'(?i)(?<={}.*light[^>]*>)[^<]*'.format

        # IN-PLACE CLEANING OF SCRAPED DATA BEFORE DETAILS EXTRACTION
        # >>> Remmoves any controls, comas (if not numeric), and extra spaces
        self.adapter[field] = self.flatten(self.adapter.get(field))
        self.adapter[field] = re.sub(del_role_txt, '', self.adapter.get(field))

        # GETS ACTORS LIST        
        actors = self.get_all(actors_regex, self.adapter.get(field))

        # GETS THE MOVIE CHARACTERS (I.E ROLES)
        for actor in actors:
            role = self.get_first(roles_regexp(actor), self.adapter.get(field))
            role = role.strip(' <>¤') if role else None
            roles[actor] = role

        # UPDATE SCRAPY ITEM
        self.adapter[field] = roles

# IMPLEMENTING THE `STORAGE PIPELINE` OR `DATABASE PIPELINE` (save data in DB)
class MovieDataBasePipeline:
    # DO NOT FORGET TO ACTIVATE:DEACTIVATE THIS PIPELINE IN SETTINGS

    # ACTIVATING DATABASE CONNECTION
    def open_spider(self, spider):
        self.session_maker = schema.db_connect(echo=False)
        self.session = self.session_maker()

    # SAVING DATA METHODS (Filling the database)
    def process_item(self, item, spider):
        """
        Save clean scraped data into a dedicated movie database.

        This function is somehow the conductor of the saving process as it does
        not really make the job itself but calls dedicated sub methods.
        """

        # FILLING OF PRIMARY TABLES
        self.update_movies_table(item)
        self.update_persons_table(item)
        self.update_companies_table(item)

        # FILLING DEPENDENT TABLES
        self.update_genres_table(item)
        self.update_countries_table(item)
        self.update_languages_table(item)

        # FILLING ASSOCIATION TABLES
        self.update_actors_table(item)
        self.update_directors_table(item)
        self.update_screenwriters_table(item)
        self.update_distributors_table(item)
        #self.session.commit()
        return item

    # Subsection dedicated to primary tables
    def update_movies_table(self, item):
        """
        Gets dedicated data from the item and saves them into `movies` table.

        Returns a warning message in the console if movie already registered.
        """

        # BASIC SETTINGS & INITIALIZATION
        film = item['title_fr']
        release_date =self.get_python_date(item['release_date'])

        # INSTANCIATES A MOVIE FROM `MOVIES` CLASS (i.e. builds a new row)
        movie = schema.Movies(
            # Main movie characteristics
            Title = item['title'],
            Title_Fr = film,
            Synopsis = item['synopsis'],
            Duration = item['runtime_min'],
            Poster_URL = item['film_poster'],
            Press_Rating = item['press_rating'],
            Public_Rating = item['public_rating'],
            # Tecnical details
            Visa = item['visa'],
            Awards = item['awards'],
            Budget = item['budget'],
            Format = item['color'],
            Category = item['types'],
            Release_Date = release_date,
            Release_Place = item['release_place'],
            Production_Year = item['production_year'])

        # ADD THE NEW MOVIE (i.e. the new row) IN THE `Movies` TABLE
        message = f"`{film}` is already in the database!"
        self.add_and_commit(movie, warner=message)

        # RETRIEVES THE MOVIE ID
        self.movie_id = queries.get_movie_id(film, release_date, self.session)

    def update_persons_table(self, item):
        """
        Gets dedicated data from the item and saves them into `persons` table.
        """

        # GETS PEOPLE NAMES (all people related to the movie)
        persons = set(item['casting']) if item['casting'] else set()
        for field in ('directors', 'screenwriters'):
            persons.update(self.split(item[field]))

        # ADDS PERSONS NAME IN THE `persons` TABLE
        for name in persons:
            self.add_and_commit(schema.Persons(Full_Name=name), warner=None)

        # RETRIEVES PERSONS `Id` AND SAVE THEM TEMPORARY FOR QUICK ACCESS
        # >>> Required to fill association tables (`actors`, `directors`, etc.)
        self.persons = queries.get_persons_id(persons, self.session)

    def update_companies_table(self, item):
        """
        Gets dedicated data from the item and saves them into `persons` table.
        """

        # GETS COMPANIES NAME (all people related to the movie)
        companies = set(self.split(item['distributors']))

        # ADDING PERSONS NAME IN THE `persons` TABLE
        for name in companies:
            self.add_and_commit(schema.Companies(Full_Name=name), warner=None)

        # RETRIEVES COMPANIES `Id` AND SAVE THEM TEMPORARY FOR QUICK ACCESS
        # >>> Required to fill association tables
        self.companies = queries.get_companies_id(companies, self.session)

    # Subsection dedicated to dependent tables
    def update_genres_table(self, item):
        """
        Gets dedicated data from the item and saves them into `genres` table.
        """

        # ADDING PERSONS NAME IN THE `persons` TABLE
        for genre in set(self.split(item['categories'])):
            movie_genre = schema.Genres(MovieId=self.movie_id, Genre=genre)
            self.add_and_commit(movie_genre, warner=None)

    def update_countries_table(self, item):
        """
        Gets dedicated data from the item and saves them in `countries` table.
        """

        # ADDING PERSONS NAME IN THE `persons` TABLE
        for country in set(self.split(item['nationalities'])):
            movie_country = schema.Countries(MovieId=self.movie_id,
                                             Country=country)
            self.add_and_commit(movie_country, warner=None)

    def update_languages_table(self, item):
        """
        Gets dedicated data from the item and saves them in `languages` table.
        """

        # ADDING PERSONS NAME IN THE `persons` TABLE
        for language in set(self.split(item['languages'])):
            movie_language = schema.Languages(MovieId=self.movie_id,
                                              Language=language)
            self.add_and_commit(movie_language, warner=None)

    # Subsection dedicated to association tables
    def update_actors_table(self, item):
        """
        Fills the `actors` association table which links movies and persons
        """

        # FILLING PROCESS
        for name, role in item['casting'].items():
            actor = schema.Actors(MovieId=self.movie_id,
                                  PersonId=self.persons[name],
                                  Characters=role)
            self.add_and_commit(actor, warner=None)

    def update_directors_table(self, item):
        """
        Fills the `directors` association table which links movies and persons
        """

        # FILLING PROCESS
        for name in set(self.split(item['directors'])):
            director = schema.Directors(MovieId=self.movie_id,
                                        PersonId=self.persons[name])
            self.add_and_commit(director, warner=None)

    def update_screenwriters_table(self, item):
        """
        Fills the `directors` association table which links movies and persons
        """

        # FILLING PROCESS
        for name in set(self.split(item['screenwriters'])):
            screenwriter = schema.ScreenWriters(MovieId=self.movie_id,
                                                PersonId=self.persons[name])
            self.add_and_commit(screenwriter, warner=None)

    def update_distributors_table(self, item):
        """
        Fills the `directors` table which associates movies and companies
        """

        # FILLING PROCESS
        for name, compid in self.companies.items():
            distributor = schema.Distributors(MovieId=self.movie_id,
                                              CompId=compid)
            self.add_and_commit(distributor, warner=None)

    # VARIOUS HELPER METHODS (Involved in the saving process but not directly)
    def add_and_commit(self, item, warner=""):
        """
        Commit changes if ACID compliant or rollback otherwise with message.

        Parameter(s):
            item (MobieDB): Instance (i.e. row) of one table to be added to it. 
            warner (str): OPTIONAL. Message to display if transaction aborted.
                          Default: `Transaction aborted. Session rolled back`
                          Optionally : `warner` can be set to None in order not
                          to show any warning message. At your own risk !
        """
        # PROCESSING WARNING MESSAGE
        txt = "Transaction aborted. Session rolled back"
        warner = txt if warner == "" else warner

        # ADDING GIVEN INSTANCE TO THE DATABASE
        self.session.add(item)

        # COMMITING PROCESS (validation of the transaction)
        try:
            self.session.commit()
        except alchemyError.IntegrityError:
            self.session.rollback()
            if warner:
                print(warner)

    def get_python_date(self, date: str):
        """
        Parse a string reprensenting a date and returns a `datetime` object.

        Parameter(s):
            date (str): String representing a date otherwise None

        Returns: A datetime.date object or simply None
        """

        date = dateparser.parse(date) if date else None
        return date.date() if date else None

    def split(self, string: str, sep: str = '¤'):
        """
        Split a string according the given separator and returns a list.

        The result can be an empty llist as each sub string extracted this way
        is discarded if it is mepty (i.e. if its length is 0).

        Parameter(s):
            string (str): String to be splitted.
            sep    (str): Character(s) to be used as the splitting indicator

        returns: A list of strings potentially empty
        """

        # SPLITTING PROCESS: first split and strip, then discard non relevant.
        result = [itm.strip() for itm in string.split(sep)] if string else []
        return [itm for itm in result if itm and len(itm) > 1]

    # CLOSING DATABASE CONNECTION
    def close_spider(self, spider):
        # Fermer la connection à la base de données
        self.session.close()