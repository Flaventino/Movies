# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import dateparser
import regex as re
from itemadapter import ItemAdapter


class MovieScraperPipeline:
    # SETTER METHODS
    def set_genre_country(self, spider):
        """
        Sets the “genre” and “country” attributes (“set” type) of the pipeline.

        Raw data extracted from enponyme but string type spider's attributes.
        """
        # ADDING LISTS OF COUNTRIES AND GENRES TO THE CLEANUP PIPELINE
        self.genre = self.convert_to_set(spider.genre)
        self.country = self.convert_to_set(spider.country)


    # GENERAL AND/OR COMMON DATA CLEANING METHODS
    def convert_to_set(self, data: str):
        """
        Finds all word groups whithin a given string and returns a set.
        """

        # EXTRACTION & CLEANING PROCESS
        data = self.flatten_raw_string(data)   # Cleans controls & extra spaces
        data = re.sub(r'¤[\W\d]+¤', '¤', data) # Drops non relevant characters
        data = data.strip('¤').split('¤')      # Lists ready-to-use items

        # FUNCTION OUTPUT (returns a python set of the clean extracted items)
        return set(data)

    def flatten_raw_string(self, data: str):
        """
        Replaces all control characters with '¤' and removes all extra spaces.

        Arguments:
            data (str): Dirty string mixing word groups to extract with some
                        ugly control characters and/or extra spaces.

        Returns: a flatten string with no controls neither extra spaces.
        """

        # FLATTENING & CLEANING PROCESS
        data = re.sub(r'[^\S ]+', '¤', data)    # Replaces any controls by '¤'
        data = re.sub(r'\s+', ' ', data)        # Drops all extra spaces

        # FUNCTION OUTPUT
        return data

    def get_first(self, regex, string):
        """
        Parses given string with given regex. Returns 1st match or None

        Arguments:
            regex  (str): Regex expression to use to parse the given string.
            string (str): String to be parsed in search of the given regex.
        """

        # EXTRACTION PROCESS
        string = re.search(regex, string)

        # FUNCTION OUTPUT
        return string.group() if string else None


    # METHODS DEDICATED TO ITEM CLEANING
    def process_item(self, item, spider):
        """
        MONITORING FUNCTION TO DRIVE THE DATA CLEANING PROCESS
        """

        # BASIC SETTINGS & INITIALIZATION
        self.adapter = ItemAdapter(item)
        _ = None if hasattr(self, 'genre') else self.set_genre_country(spider)

        # DATA CLEANING PIPELINE
        self.clean_titles()      # Titles cleaning
        self.clean_synopsis()    # Synopsis cleaning
        self.clean_film_poster() # Movie poster (get clean url)
        self.clean_creators()    # Extract directors and writers
        self.clean_metadata()    # Extract date, duration and genres


        print("##########################################################")
        #for key, value in response.meta['data'].items():
        for key, value in item.items():
            if value:
                print(f'{key}:\n§{repr(value)}§')
                print()
                print()
        #print(dir(response.meta['item']))
        #print(response.meta['item'])
        print("##########################################################")
        print("-----------------------------------------------------------")

        return item

    def clean_titles(self):
        for field in ['title', 'title_fr']:
            # GET STRING EXPRESSION OF THE FIELD TO CLEAN
            value = self.adapter.get(field)

            # CLEANING PROCESS
            value = re.sub(r'[\s¤]+$', '', value)
            value = re.search(r'[^¤]*$', value)
            value = value.group().strip() if value else ''
            value = re.sub(r'\s+', ' ', value)

            # UPDATING THE ITEM'S FIELD WITH A CLEAN VALUE OR SIMPLY NONE.
            self.adapter[field] = value if len(value) > 0 else None

    def clean_synopsis(self):
        # INITIALIZATION
        field = 'synopsis'

        # CLEANING PROCESS
        self.adapter[field] = self.adapter.get(field).strip()

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

        # CLEANS the 'creators' FIELD IN PLACE BEFORE EXTRACTING DATA
        self.adapter[field] = self.flatten_raw_string(self.adapter.get(field))

        # RAW FIELDS EXTRACTION (get raw data for directors and screenwriters)
        writers = re.search(r'(?i)(?<=¤\s*par\s*¤).*', self.adapter.get(field))
        directors = re.sub(r'(?i)¤\s*par\s*¤.*$', '', self.adapter.get(field))
        directors = re.search(r'(?i)(?<=¤\s*de\s*¤).*', directors)
        movie_makers = {'directors': directors, 'screenwriters': writers}

        # DATA CLEANING (getting names of all directors and writers)
        for field, value in movie_makers.items():
            # Name extraction & cleaning process
            value = value.group() if value else ''    # Get raw data to clean
            value = re.findall(r'[\w\s]+', value)     # Get all people names
            value = [name.strip() for name in value]  # Drop extra spaces
            value = "¤".join(value)                   # Merge to a single str

            # Updates scrapy Item fields whith clean value (or simply 'None').
            self.adapter[field] = value if len(value) > 0 else None

    def clean_metadata(self):
        """
        Extract clean date, medium, duration and genre of movie from metadata.
        """

        # INITIALIZATION
        data = self.adapter.get('metadata')  # Retrieves scraped data
        data = self.flatten_raw_string(data) # Cleans controls & extra spaces
        self.adapter['metadata'] = data      # Updates Item 'metadata' field

        # EXTRACTION & CLEANING PROCESS
        self.extract_movie_date()            # Retrieves and reformat date

        # # MOVIE RUNTIME
        # # 1. Extracting the movie runtile (i.e. duration)
        # runtime = r'(?i)\d+h[\w\s]*(?=¤)'    # Reg. exp. to match dates
        # runtime = re.search(runtime, data)  # Look for runtime pattern in data
        # runtime = runtime.group() if runtime else None

        # # 2. Change runtime from alphanumeric format to minutes
        # #duration = dateparser.parse(runtime)
        # print('#############################################################')
        # print(runtime)
        # if runtime:
        #     runtime = re.sub(r'(?i)(?<=\d+)h\s*', '*60+', runtime)
        #     print(runtime)
        #     runtime = re.sub(r'\p{L}*|\s+', '', runtime)
        #     print(runtime)
        #     print(eval(runtime))
        # # print(type(duration))
        # # print(dir(duration))
        # # print(duration.hour)
        # # print(duration.minute)
        # print('#############################################################')
        # #duration = duration.total_seconds() // 60 if duration else None
        # self.adapter['duration_min'] = "duration"


        # self.adapter['metadata'] = data
        # # FUNCTION OUTPUT (returns updated item)
        # #return item
        # RECOVERS ORIGINAL 'metadata' FIELD FOR POST PROCESSING CHECK
        self.adapter['metadata'] = data


    def extract_movie_date(self):
        """
        Extracts movie release date from scraped data, parses and reformats it.

        Additionnaly, 'metadata' field of the scrapy 'Item' is updated to drop
        the extracted date so that subsequent extractions (runtime, genre, etc)
        become a little bit easier... 
        """
        # # MOVIE RELEASE DATE
        # # 1. Extracting the movie release date info
        # date = r'(?i)\d+\s+\p{L}+\s+\d{4}'    # Reg. exp. to match dates 
        # date = re.search(date, data)          # Look for dates in the data
        # date = date.group() if date else None # Extract date if any or set None

        # # 2. Formating the movie release date + Item update
        # isodate = dateparser.parse(date)      # Instanciates a date object
        # isodate = isodate.strftime('%Y/%m/%d') if date else None
        # self.adapter['release_date'] = isodate
        pass