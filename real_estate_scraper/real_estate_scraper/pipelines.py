# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.misc import md5sum
from scrapy.exceptions import DropItem
class RealEstateScraperPipeline:
    def process_item(self, item, spider):
        return item


class CustomImagesPipeline(ImagesPipeline):

    def file_path(self, request, response=None, info=None):
        fileName = request.url.split('/')[-1] # Used to extract the file extension
        return f'halooglasi_{fileName}.jpg'




class DropIfEmptyFieldPipeline(object):

    def process_item(self, item, spider):


        if not(any(item.values())):
            raise DropItem()
        else:
            return item