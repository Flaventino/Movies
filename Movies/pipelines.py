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
        MONITORING FUNCTION TO DRIVE DATA CLEANING PROCESS
        """

        # TITLES CLEANING
        item = self.clean_titles(item)

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
        adapter = ItemAdapter(item)
        for field in ['title', 'title_fr']:
            # Get string expression of the field
            value = adapter.get(field)

            # Cleaning process
            value = re.sub(r'[\s¤]+$', '', value)
            value = re.search(r'[^¤]*$', value)
            value = '' if value is None else value.group().strip()
            value = re.sub(r'\s+', ' ', value)

            # Replacement of bad values (i.e. 'NaN' values) by just None
            value = value if len(value) > 0 else None

            # Update of the field in the item
            adapter[field] = value
        return item