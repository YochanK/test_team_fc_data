from scrapy.spiders import Spider
from scrapy.http import Request, FormRequest
import pandas as pd
from scrapy.utils.project import data_path
from products.utils import checking_old_collection_with_new_one
from time import sleep
import datetime
import re
import json
import requests as rq
import validators


class CosSpider(Spider):
    name = "cos"
    download_delay = 0.2
    start_urls = [
        "https://www.cosstores.com/en_eur/index.html"
    ]
    brand = "COS"

    _collectionToSkip = ["magazine", "men", "new arrivals", "sale"]
    _categoryToSkip = ["home & livestyle"]
    _subcategoryToSkip = []

    _collectionToEnter = []
    _categoryToEnter = []
    _subcategoryToEnter = []

    def parse(self, response, **kwargs):
        """
        Parse collection and category

        :param response:
        :return:
        """

        collection_selector = response.css("div.category-wrapper > div.categories")
        if not collection_selector:
            self.logger.critical("Browsing: Don't found collection selector")
            return

        # Checking if the collections changed
        collection_list_raw = response.css("div.category-wrapper > div.categories::attr(data-value)").getall()
        checking_old_collection_with_new_one(spider_name=self.name, collection_list=collection_list_raw, spider=self)

        for col in collection_selector:
            # get collection name
            col_name = col.css("::attr(data-value)").get()
            if not col_name or not col_name.strip():
                self.logger.critical("Browsing: Collection name don't found")
                continue

            col_name = col_name.strip()

            if col_name.lower() in self._collectionToSkip:
                continue

            category_selector = col.css("div.category-list > a")
            if not category_selector:
                self.logger.critical(
                    "Browsing: Category selector don't work for collection " + col_name
                )
                continue

            for cat in category_selector:
                category_name = cat.css("::text").get()

                if not category_name or not category_name.strip():
                    self.logger.warning(
                        "Browsing: Don't found category name for collection:"
                        + col_name
                        + "\n"
                    )
                    continue

                category_name = category_name.strip()

                if category_name.lower() in self._categoryToSkip:
                    continue

                # get category link
                category_link = cat.css("::attr(href)").get()
                if not category_link:
                    self.logger.critical(
                        "Browsing: Don't found category link for collection: "
                        + col_name
                        + "\n"
                        + "category: "
                        + category_name
                    )
                    continue

                path = [col_name, category_name, ""]

                self.logger.info(path)

                yield Request(
                    response.urljoin(category_link),
                    dont_filter=True,
                    cb_kwargs=dict(path_info=path),
                    callback=self.parse_pages,
                )

    def parse_pages(self, response, path_info):
        nb_max_product = response.css("#productCount::attr(class)").get()
        if not nb_max_product:
            self.logger.warning(
                "Getting: Don't found max product for url:"
                + response.url
                + "\n"
            )
            return
        nb_max_product = int(nb_max_product)
        for nb_product_start in range(0, nb_max_product+12, 12):
            if nb_product_start > nb_max_product:
                break
            actual_url = response.url.replace(".html", "/")
            next_page_url = actual_url + "_jcr_content/genericpagepar/productlisting.products.html?start={}".format(nb_product_start)
            yield Request(
                next_page_url,
                cb_kwargs=dict(path_info=path_info),
                dont_filter=True,
                callback=self.parse_list,
            )

    def parse_list(self, response, path_info):
        product_tiles = response.css("#reloadProducts > div.column")
        if not product_tiles:
            self.logger.warning(
                "Getting: Don't found product tiles for url:"
                + response.url
                + "\n"
            )
            return

        for product in product_tiles:
            product_url = product.css("div.o-product > div > div > a::attr(href)").get()
            if not product_url:
                self.logger.warning(
                    "Getting: Don't found product url for url:"
                    + response.url
                    + "\n"
                )
                continue

            yield Request(
                product_url,
                cb_kwargs=dict(path_info=path_info),
                dont_filter=True,
                callback=self.parse_product,
            )

    def parse_product(self, response, path_info):

        headers = self.settings.get("EXPORT_FIELDS", [])
        item = dict()
        for field in headers:
            item[field] = None

        item["pays"] = "France"
        item["brand"] = self.brand
        item["website"] = self.brand
        item["collection"] = path_info[0].upper()
        item["category"] = path_info[1]
        item["sub_category"] = path_info[2]


        # get product id
        product_id = response.css("div.article-number::text").get()
        if not product_id:
            self.logger.error(
                "Scraping: Don't found product id for url: "
                + response.url
                + "\n"
                + "path: "
                + str(path_info)
            )
            return
        item["id_mode_item"] = product_id

        # get designation
        designation = response.css("div.title > h1::text").get()
        if not designation:
            self.logger.error(
                "Scraping: Don't found designation for url: "
                + response.url
                + "\n"
                + "path: "
                + str(path_info)
            )
            return
        item["designation"] = designation

        # get description
        description = response.css("div.product-description > div.description-text > p::text").getall()
        if not description:
            self.logger.error(
                "Scraping: Don't found description for url: "
                + response.url
                + "\n"
                + "path: "
                + str(path_info)
            )
            return
        description_list = list(description)
        description_clean = [sentence.replace("\\r", "").replace("\\n", "").replace("\\t", "").strip() for sentence in
                             description_list if sentence.replace("\\r", "").replace("\\n", "").replace("\\t", "").strip() != ""]
        description = ". ".join(description_clean[0:-2])

        item["description"] = description

        # get composition
        try:
            composition = description_clean[-2]
        except Exception:
            self.logger.error(
                "Scraping: Don't found composition for url: "
                + response.url
                + "\n"
                + "path: "
                + str(path_info)
            )
            return
        item["composition"] = composition

        # get price
        price = response.css("div.price > span.productPrice::text").get()
        if not price:
            self.logger.error(
                "Scraping: Don't found price for url: "
                + response.url
                + "\n"
                + "path: "
                + str(path_info)
            )
            return
        price_clean = price.replace("€", "").strip()

        item["price"] = price_clean

        # get original_price
        original_price = response.css("div.price > span.is-deprecated::text").get()
        if not original_price:
            item["original_price"] = item["price"]
        else:
            item["original_price"] = original_price.replace("€", "").strip()

        # get color
        color = response.css("#pdpDropdown::attr(data-value)").get()
        if not color:
            self.logger.error(
                "Scraping: Don't found color for url: "
                + response.url
                + "\n"
                + "path: "
                + str(path_info)
            )
            return

        item["color"] = color

        # get size
        sizes_bloc = str(response.css("div.content-section > div.parbase > script::text").get())
        if not sizes_bloc:
            self.logger.error(
                "Scraping: Don't found size selector for url: "
                + response.url
                + "\n"
                + "path: "
                + str(path_info)
            )
            return

        size_list_re = re.search(r"'variants' : (\[(.|\n)*?\])", sizes_bloc)
        if not size_list_re:
            self.logger.error(
                "Scraping: Don't found size regex selector for url: "
                + response.url
                + "\n"
                + "path: "
                + str(path_info)
            )
            return

        # Get which item is available
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 6.1; fr; rv:1.9.0.6) Gecko/2009011913 Firefox/3.0.6"
        }
        json_available = rq.get(
            "https://www.cosstores.com/webservices_cos/service/product/cos-europe/availability/{}.json".format(item["id_mode_item"][:-3]),
            headers=headers,
            proxies={
                "http": "http://lum-customer-hl_01ede8dc-zone-data_center:2sdptommqulk@zproxy.lum-superproxy.io:22225",
                "https": "http://lum-customer-hl_01ede8dc-zone-data_center:2sdptommqulk@zproxy.lum-superproxy.io:22225",
            },
        )

        if json_available.status_code == 403:
            sleep(1)
            json_available = rq.get(
                "https://www.cosstores.com/webservices_cos/service/product/cos-europe/availability/{}.json".format(item["id_mode_item"][:-3]),
                headers=headers,
                proxies={
                    "http": "http://lum-customer-hl_01ede8dc-zone-data_center:2sdptommqulk@zproxy.lum-superproxy.io:22225",
                    "https": "http://lum-customer-hl_01ede8dc-zone-data_center:2sdptommqulk@zproxy.lum-superproxy.io:22225",
                },
            )
        if json_available.status_code == 403:
            sleep(2)
            json_available = rq.get(
                "https://www.cosstores.com/webservices_cos/service/product/cos-europe/availability/{}.json".format(item["id_mode_item"][:-3]),
                headers=headers,
                proxies={
                    "http": "http://lum-customer-hl_01ede8dc-zone-data_center:2sdptommqulk@zproxy.lum-superproxy.io:22225",
                    "https": "http://lum-customer-hl_01ede8dc-zone-data_center:2sdptommqulk@zproxy.lum-superproxy.io:22225",
                },
            )

        if json_available.status_code == 403:
            sleep(5)
            json_available = rq.get(
                "https://www.cosstores.com/webservices_cos/service/product/cos-europe/availability/{}.json".format(item["id_mode_item"][:-3]),
                headers=headers,
                proxies={
                    "http": "http://lum-customer-hl_01ede8dc-zone-data_center:2sdptommqulk@zproxy.lum-superproxy.io:22225",
                    "https": "http://lum-customer-hl_01ede8dc-zone-data_center:2sdptommqulk@zproxy.lum-superproxy.io:22225",
                },
            )

        json_available = json_available.json()

        size_list = size_list_re.group().strip()[15:-2].replace("\r", "").replace("\n", "").replace("\t", "").replace('"', "").split("},{")
        size_dict = ""
        for dict_size in size_list:
            clean_dict = json.loads("{" + dict_size.replace("{", "").replace("}", "").replace("'", '"').replace(" ", '')[:-1] + "}")
            if clean_dict["variantCode"] in json_available["availability"]:
                size_dict += clean_dict["sizeName"] + ", "

        item["size"] = size_dict

        # get url item
        item["url_item"] = response.url

        # get images urls
        images_selector = response.css("#mainImageList > li > div > div > img")
        if not images_selector:
            self.logger.error(
                "Scraping: Don't found image selector for url: "
                + response.url
                + "\n"
                + "path: "
                + str(path_info)
            )
            return
        img_url_list = ["https:"+str(img_url.css("::attr(data-zoom-src)").get()) for img_url in images_selector]

        item["image_urls"] = img_url_list

        # get datetime
        now = datetime.datetime.now()
        item["timestamp"] = now.strftime("%d/%m/%Y")


        yield item