# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter


def normalize_keys(obj):
    if isinstance(obj, dict):
        return {
            key.lower().replace(" ", "_"): normalize_keys(value)
            for key, value in obj.items()
        }
    elif isinstance(obj, list):
        return [normalize_keys(item) for item in obj]
    else:
        return obj


class NlrbPipeline:
    def process_item(self, item, spider):
        return normalize_keys(item)
