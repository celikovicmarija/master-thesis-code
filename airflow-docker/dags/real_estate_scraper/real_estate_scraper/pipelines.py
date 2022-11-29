# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from scrapy.exceptions import DropItem
# useful for handling different item types with a single interface
from scrapy.pipelines.images import ImagesPipeline


class RealEstateScraperPipeline:
    def process_item(self, item, spider):
        return item


class CustomImagesPipeline(ImagesPipeline):

    def file_path(self, request, response=None, info=None):
        fileName = request.url.split('/')[-1]  # Used to extract the file extension
        return f'halooglasi_{fileName}.jpg'


class DropIfEmptyFieldPipeline(object):

    def process_item(self, item, spider):

        if not (any(item.values())):
            raise DropItem()
        else:
            return item
