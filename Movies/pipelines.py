import regex as re
import dateparser, nltk
from itemadapter import ItemAdapter


# Download the stopwords corpus
nltk.download('stopwords')

# PIPELINE CLASSES
class MovieScraperPipeline:
    # Makeing a python `set` of french words to stop (i.e. drop)
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
        string = re.findall(regex, string)          # Gets groups of words
        string = [item.strip() for item in string]  # Removes extra spaces
        string = [x for x in string if len(x) > 0]  # Keeps relevant groups

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
        # self.clean_synopsis()  # Synopsis cleaning --USELESS--
        self.clean_film_poster() # Movie poster (get clean url)
        self.clean_creators()    # Extract directors and writers
        self.clean_metadata()    # Extract release date and place, genres, etc.
        self.clean_technnical()  # Extract distributors, origin, languages etc.
        self.clean_ratings()     # Extract Press and Public ratings


        print("##########################################################")
        #for key, value in response.meta['data'].items():
        for key, value in item.items():
            if value:
                print(f'{key}:\n{repr(value)}')
                try:
                    print(f">>>{self.flatten(self.adapter.get(key))}")
                except:
                    pass
                print()
                print()
        #print(dir(response.meta['item']))
        #print(response.meta['item'])
        print("##########################################################")
#        print("-----------------------------------------------------------")

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

        # INITIALIZATION
    #     data = self.adapter.get('metadata')  # Retrieves scraped data
    #     data = self.flatten_raw_string(data) # Cleans controls & extra spaces
    #     self.adapter['metadata'] = data      # Updates Item 'metadata' field

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
            expr = r'(?i)(?<=\d+)\s*h\s*'           # Regex to match 'h'
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

    # Sub section dedicated to `technical` info cleaning
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