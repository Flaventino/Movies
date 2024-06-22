# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import regex as re
from itemadapter import ItemAdapter


class MovieScraperPipeline:
    def process_item(self, item, spider):
        """
        MONITORING FUNCTION TO DRIVE THE DATA CLEANING PROCESS
        """

        # INITIALIZATION
        self.adapter = ItemAdapter(item)

        # DATA CLEANING PIPELINE
        item = self.clean_titles(item)      # Titles cleaning
        item = self.clean_synopsis(item)    # Synopsis cleaning
        item = self.clean_film_poster(item) # Movie poster (get clean url)
        item = self.clean_creators(item)    # Extract directors and writers


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

        # RAW FIELDS EXTRACTION
        writers = re.search(r'(?i)(?<=¤\s*par\s*¤).*', creators)
        directors = re.sub(r'(?i)¤\s*par\s*¤.*$', '', creators)
        directors = re.search(r'(?i)(?<=¤\s*de\s*¤).*', directors)

        # DATA CLEANING
        data = {'directors': directors, 'screenwriters': writers}
        for field, value in data.items():
            # Processes effective cleaning
            value = value.group() if value else ''
            value = re.findall(r'[\w\s]+', value)     # Get all people names
            value = [name.strip() for name in value]  # Drop extra spaces
            value = "¤".join(value)                   # Merge to a single str

            # Updates scrapy Item fields whit clean value or simply 'None'.
            self.adapter[field] = value if len(value) > 0 else None

        # FUNCTION OUTPUT
        return item