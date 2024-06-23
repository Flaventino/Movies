# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
#import dateparser
import regex as re
from itemadapter import ItemAdapter


class MovieScraperPipeline:
    # # INSTANCES INITIALIZATION
    # def close_spider(self, spider):

    #     # ADDING LISTS OF COUNTRIES AND GENRES TO THE CLEANUP PIPELINE
    #     self.genres = self.clean_relations(spider.genres)
    #     self.countries = self.clean_relations(spider.countries)

    #     print('##############################################################')
    #     print('GENRES DE FILMS')
    #     print(self.genres)
    #     print('##############################################################')
    #     print('##############################################################')
    #     print('GENRES DE FILMS')
    #     print(self.countries)
    #     print('##############################################################')

    # SETTER METHODS
    def set_genre_country(self, spider):
        """
        Sets the “genre” and “country” attributes (“set” type) of the pipeline.

        Raw data extracted from enponyme but string type spider's attributes.
        """
        # ADDING LISTS OF COUNTRIES AND GENRES TO THE CLEANUP PIPELINE
        self.genre = self.convert_to_set(spider.genre)
        self.country = self.convert_to_set(spider.country)

        # print('##############################################################')
        # print('GENRES DE FILMS DANS set_genre_country')
        # print(self.genre)
        # print(spider.name)
        # print('##############################################################')
        # print('##############################################################')
        # print('GENRES DE FILMS DANS set_genre_country')
        # #print(self.country)
        # print('##############################################################')


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


    # METHODS DEDICATED TO ITEM CLEANING
    def process_item(self, item, spider):
        """
        MONITORING FUNCTION TO DRIVE THE DATA CLEANING PROCESS
        """

        # BASIC SETTINGS & INITIALIZATION
        self.adapter = ItemAdapter(item)
        _ = None if hasattr(self, 'genre') else self.set_genre_country(spider)

        # DATA CLEANING PIPELINE
        item = self.clean_titles(item)      # Titles cleaning
        item = self.clean_synopsis(item)    # Synopsis cleaning
        item = self.clean_film_poster(item) # Movie poster (get clean url)
        item = self.clean_creators(item)    # Extract directors and writers
        item = self.clean_metadata(item)    # Extract date, duration and genres


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

    def clean_titles(self, item):
        for field in ['title', 'title_fr']:
            # GET STRING EXPRESSION OF THE FIELD TO CLEAN
            value = self.adapter.get(field)

            # CLEANING PROCESS
            value = re.sub(r'[\s¤]+$', '', value)
            value = re.search(r'[^¤]*$', value)
            value = '' if value is None else value.group().strip()
            value = re.sub(r'\s+', ' ', value)

            # UPDATING THE ITEM'S FIELD WITH A CLEAN VALUE OR SIMPLY NONE.
            self.adapter[field] = value if len(value) > 0 else None

        # FUNCTION OUTPUT
        return item

    def clean_synopsis(self, item):
        # INITIALIZATION
        field = 'synopsis'

        # CLEANING PROCESS
        self.adapter[field] = self.adapter.get(field).strip()

        # FUNCTION OUTPUT
        return item

    def clean_film_poster(self, item):
        # INITIALIZATION
        field = 'film_poster'

        # CLEANING PROCESS
        poster_url = re.search(r'http.+\.jpg', self.adapter.get(field))
        self.adapter[field] = poster_url.group(0) if poster_url else None

        # FUNCTION OUTPUT
        return item

    def clean_creators(self, item):
        """
        This method extracts the list of directors and that of screenwriters.
        """

        # INITIALIZATION (get creators field from item and preprocesses it)
        creators = re.sub(r'[^\S ]+', '¤', self.adapter.get('creators'))

        # RAW FIELDS EXTRACTION (get raw data for directors and screenwriters)
        writers = re.search(r'(?i)(?<=¤\s*par\s*¤).*', creators)
        directors = re.sub(r'(?i)¤\s*par\s*¤.*$', '', creators)
        directors = re.search(r'(?i)(?<=¤\s*de\s*¤).*', directors)
        movie_makers = {'directors': directors, 'screenwriters': writers}

        # DATA CLEANING (gets names of all directors and writers)
        for field, value in movie_makers.items():
            # Name extraction & cleaning process
            value = value.group() if value else ''    # Get raw data to clean
            value = re.findall(r'[\w\s]+', value)     # Get all people names
            value = [name.strip() for name in value]  # Drop extra spaces
            value = "¤".join(value)                   # Merge to a single str

            # Updates scrapy Item fields whit clean value (or simply 'None').
            self.adapter[field] = value if len(value) > 0 else None

        # FUNCTION OUTPUT
        return item

    def clean_metadata(self, item):
        """
        Extract clean date, medium, duration and genre of movie from metadata.
        """

        # INITIALIZATION (get creators field from item and preprocesses it)
        data = self.adapter.get('metadata')  # Retrieves scraped data 
        #data = re.sub(r'[^\S ]+', '¤', data) # Replaces any controls by '¤' 
        #data = re.sub(r'\s+', ' ', data)     # Drops any supernumeraries spaces

        #data = self.flatten_raw_string(data) # cleans controls & extra spaces

        # print('##############################################################')
        # print('GENRES DE FILMS DANS METADATA CLEANING')
        # print(self.genre)
        # print('##############################################################')
        # print('##############################################################')
        # print('GENRES DE FILMS DANS METADATA CLEANING')
        # print(self.country)
        # print('##############################################################')

        #self.adapter['metadata'] = "Tartampion"
        # FUNCTION OUTPUT (returns updated item)
        return item