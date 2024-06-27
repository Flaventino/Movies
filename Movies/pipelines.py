import regex as re
import dateparser, nltk
from itemadapter import ItemAdapter


# Download the stopwords corpus
nltk.download('stopwords')

# PIPELINE CLASSES
class MovieScraperPipeline:
    # # Makeing a python `set` of french words to stop (i.e. drop)
    # fr_stopset = set(nltk.corpus.stopwords.words('french'))

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

    # def set_genre_country(self, spider):
    #     """
    #     Sets the “genre” and “country” attributes (“set” type) of the pipeline.

    #     Raw data extracted from enponyme but string type spider's attributes.
    #     """
    #     # ADDING LISTS OF COUNTRIES AND GENRES TO THE CLEANUP PIPELINE
    #     self.genre = self.convert_to_set(spider.genre)
    #     self.country = self.convert_to_set(spider.country)


    # # GENERAL AND/OR COMMON DATA CLEANING METHODS
    # def convert_to_set(self, data: str):
    #     """
    #     Finds all word groups whithin a given string and returns a set.
    #     """

    #     # EXTRACTION & CLEANING PROCESS
    #     data = self.flatten_raw_string(data)   # Cleans controls & extra spaces
    #     data = re.sub(r'¤[\W\d]+¤', '¤', data) # Drops non relevant characters
    #     data = data.strip('¤').split('¤')      # Lists ready-to-use items

    #     # FUNCTION OUTPUT (returns a python set of the clean extracted items)
    #     return set(data)

    # def flatten_raw_string(self, data: str):
    #     """
    #     Changes all control characters into '¤'. Drops comas & extra spaces.

    #     Parameter(s):
    #         data (str): Dirty string mixing word groups to extract with some
    #                     ugly control characters and/or extra spaces.

    #     Returns: a flatten string with no controls neither extra spaces.
    #     """

    #     # FLATTENING & CLEANING PROCESS
    #     data = re.sub(r'[^\S ¤,]+', '¤', data)   # Replaces any controls by '¤'
    #     data = re.sub(r'\s+', ' ', data)        # Drops all extra spaces

    #     # FUNCTION OUTPUT
    #     return data

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

    # def alter_scrap(self, field, regex, string=''):
    #     """
    #     Change the given field in place using a regex and a replacement string.

    #     Purpose is not to alter the field name but the related sccraped data.
    #     This function basically implements 'search' method from 'regex' module.

    #     Parameter(s):
    #         field  (str): Label (or name) of the scrapy Item to update.
    #         regex  (str): Regular expression to use whithin 'search' method.
    #         string (str): Replacement string to use to replace regex matches.
    #     """

    #     self.adapter[field] = re.sub(regex, string, self.adapter.get(field))

    def flatten(self, txt: str):
        """
        Removes controls characters, commas (if non numeric), and extra spaces.

        Parameter(s):
            txt (str): String to be cleaned

        Returns: A clean string (cleaned version of the given string)
        """

        regex = r'\s*(?:[^\S ]|¤|(?<!\d),(?!\d))+\s*'       # Main regex
        return re.sub(r'\s+', ' ', re.sub(regex, '¤', txt)) # Cleaning process


    # METHODS DEDICATED TO ITEM CLEANING
    def process_item(self, item, spider):
        """
        MONITORING FUNCTION TO DRIVE THE CLEANING PROCESS OF SCRAPED DATA.
        """

        # # BASIC SETTINGS & INITIALIZATION
        # #_ = None if hasattr(self, 'adapter') else self.adapter := ItemAdapter(item)
        # _ = None if hasattr(self, 'genre') else self.set_genre_country(spider)
        # _ = None if hasattr(self, 'adapter') else setattr(self, 'adapter', ItemAdapter(item))
        self.adapter = ItemAdapter(item)
        #_ = None if hasattr(self, 'genre') else self.get_spider_attr(spider)
        
        if not hasattr(self, 'genre'):
            self.get_spider_attr(spider)
            print(repr(spider.genre))
            # print(f"\n>>>{self.flatten(spider.genre)}")
            # print()
            # print()

            print(self.genre)
            print()
            print(self.country)
            # print(repr(spider.country))
            # print(f"\n>>>{self.flatten(spider.country)}")
            #setattr(self, 'genre', spider)print(repr(spider.genre))

        # DATA CLEANING PIPELINE
        self.clean_titles()      # Cleaning of the french and original titles
        # self.clean_synopsis()  # Synopsis cleaning --USELESS--
        self.clean_film_poster() # Movie poster (get clean url)
        self.clean_creators()    # Extract directors and writers
        #self.clean_metadata()    # Extract release date and place, genres, etc.
        # self.clean_technnical()  # Extract distributors, origin, languages etc.


        print("##########################################################")
        #for key, value in response.meta['data'].items():
        for key, value in item.items():
            if value:
                print(f'{key}:\n{repr(value)}')
                print(f">>>{self.flatten(self.adapter.get(key))}")
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

    # Sub section dedicated to `metadata` cleaning
    def clean_metadata(self):
        """
        Parses metadata to get clean date, medium, duration and genre of movie.
        """

        # INITIALIZATION
    #     data = self.adapter.get('metadata')  # Retrieves scraped data
    #     data = self.flatten_raw_string(data) # Cleans controls & extra spaces
    #     self.adapter['metadata'] = data      # Updates Item 'metadata' field

    #     # EXTRACTION & CLEANING PROCESS
    #     self.get_movie_date()                # Retrieves and reformat date
    #     self.get_runtime()                   # Retrieves and reformat duration
    #     self.get_genres_and_release_places() # Extracts clean target data

    #     # RECOVERS ORIGINAL 'metadata' FIELD (for post processing check only)
    #     self.adapter['metadata'] = data
        pass

    # def get_movie_date(self):
    #     """
    #     Extracts movie release date from scraped data, parses and reformats it.

    #     Additionnaly, 'metadata' field of the scrapy 'Item' is updated to drop
    #     the extracted date so that subsequent extractions (runtime, genre, etc)
    #     become a little bit easier... 
    #     """

    #     # BASIC SETTINGS & INITIALIZATION
    #     meta = 'metadata'

    #     # MOVIE RELEASE DATE - Stage 1 - Extracting alphanumeric date
    #     regx = r'(?i)\d+\s+\p{L}+\s+\d{4}'                  # Date pattern
    #     date = self.get_first(regx, self.adapter.get(meta)) # Get date or none

    #     # MOVIE RELEASE DATE - Stage 2 - reformating date + Item update
    #     isodate = dateparser.parse(date)           # Instanciates a date object
    #     isodate = isodate.strftime('%Y/%m/%d') if date else None # Formats date
    #     self.adapter['release_date'] = isodate
        
    #     # UPDATE 'metadata' FIELD (For easier subsequent cleaning process only)
    #     self.alter_scrap(field=meta, regex=f'{date if date else ""}')

    # def get_runtime(self):
    #     """
    #     Extracts movie duration from scraped data, parses and reformats it.

    #     Additionnaly, 'metadata' field of the scrapy 'Item' is updated to drop
    #     the extracted duration so that subsequent extractions (genre, medium)
    #     become a little bit easier... 
    #     """

    #     # BASIC SETTINGS & INITIALIZATION
    #     meta = 'metadata'

    #     # MOVIE RUNTIME - Stage 1 - Extracting alphanumeric runtime (or length)
    #     regex = r'(?i)\d+h[\w\s]*(?=¤)'                         # Runtime regex
    #     runtime = self.get_first(regex, self.adapter.get(meta)) # Get duration

    #     # MOVIE RUNTIME - Stage 2 - UPDATE 'metadata' FIELD (delete runtime)
    #     self.alter_scrap(field=meta, regex=f'{runtime if runtime else ""}')

    #     # MOVIE RUNTIME - Stage 3 - Reformating runtime to get it in minutes
    #     if runtime:
    #         runtime = re.sub(r'(?i)(?<=\d+)h\s*', '*60+', runtime) # Hour * 60
    #         runtime = re.sub(r'\p{L}*|\s+', '', runtime)           # keep digit
    #         runtime = int(eval(runtime))                           # Calculates

    #     # SCRAPY 'ITEM' UPDATE 
    #     self.adapter['runtime_min'] = runtime

    # def get_genres_and_release_places(self):
    #     """
    #     Extracts pure and clean genre(s) and release-place from scrapped data.
    #     """

    #     # REMOVES ANY NON RELEVANT WORDS FROM RAW DATA (i.e. drops stop words)
    #     regex = "|".join([f'(?<=\W+){itm}(?=\W+)' for itm in self.fr_stopset])
    #     self.alter_scrap(field='metadata', regex=f'(?i){regex}')

    #     # EXTRACTS ALL SINGLE WORDS OR GROUPS OF WORDS SEPARATED BY SPACES.
    #     words = re.findall(r'[\p{L} ]+', self.adapter.get('metadata'))
    #     words = [word.strip() for word in words]            # Drop extra spaces

    #     # GET LIST OF CLEAN GENRES AND CLEAN RELEASE PLACES
    #     genres = "¤".join(set(words) & self.genre)
    #     places = "¤".join(set(words) - self.genre)

    #     # UPDATES SCRAPY `ITEM` FIELDS FOR GENRE AND RELEASE PLACES
    #     self.adapter['categories'] = genres if len(genres) > 0 else None
    #     self.adapter['release_place'] = places if len(places) > 0 else None

    # # Sub section dedicated to `technical` info cleaning
    # def clean_technnical(self):
    #     """
    #     Extracts required technical details from related scraped data.
    #     """
    #     # BASIC SETTINGS & INITIALIZATION
    #     data, heads = 'tech_data', 'tech_headers'
    #     fields = {'visa': 'visa',
    #               'types': 'film',
    #               'color': 'couleur',
    #               'budget': 'budget',
    #               'awards': 'r\p{L}compense',
    #               'languages': 'langue',
    #               'distributors': 'distributeur',
    #               'nationalities': 'nation',
    #               'production_year': 'ann\p{L}e'}

    #     # IN-PLACE CLEANING OF SCRAPED DATA (controls, extra spaces and comas)
    #     for itm in (data, heads):
    #         self.adapter[itm] = self.flatten_raw_string(self.adapter.get(itm))

    #     # EXTRACTING HEADERS AS SINGLE STRING (i.e. category names)
    #     headers = re.self.adapter.get(heads).split('¤')
    #     #headers = set([re.sub(r'^[\s¤]+|[\s¤]+$', '', itm) for itm in headers])
    #     # IMPLEMENTING A SEARCH STOPPER
    #     # stopper = self.adapter.get(heads).split('¤')
    #     # stopper = "|".join(set([header.strip('¤ ') for header in stopper]))
    #     # #stopper = "|".join(f'{}' for itm in stopper)
    #     print("##############################################################")
    #     print(f"{self.adapter.get(heads) = }")
    #     # #print(set([word.strip('¤ ') for word in self.adapter.get(heads).split('¤')]))
    #     # print(f"{stopper = }")

    #     # # EXTRACTING & CLEANING REQUIRED FIELDS
    #     # data = self.adapter.get(data)
    #     # print(f"{data = }")
    #     # for field, word in fields.items():
    #     #     #reg = '(?i)\|*\p{L}*' + word + '\p{L}*\|*'
    #     #     reg = f"(?i)[\p{'L'}° ]*{word}[\p{'L'}° ]*"
    #     #     key = re.search(reg, stopper)
    #     #     key = key.group().strip() if key else None
    #     #     print(f"{reg = }  >>>>  {key = }")
    #     #     if key:
    #     #         value = re.search(f'(?<={key})[¤\w ]+(?={stopper})', data)
    #     #         value = value.group() if value else None
    #     #     else:
    #     #         value = None
    #     #     print(value)
        
    #     print("##############################################################")
    # #     pass