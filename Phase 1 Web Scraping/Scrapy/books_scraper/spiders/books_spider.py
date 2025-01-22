import scrapy
# from books_scraper.items import BookItem  

class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["http://books.toscrape.com/"]

    def parse(self, response):
        for book in response.css("article.product_pod"):
            title = book.css("h3 a::attr(title)").get()
            price = book.css("p.price_color::text").get()
            rating = book.css("p.star-rating::attr(class)").get().replace('star-rating', '').strip()
            availability = book.css("p.availability::text").getall()
            availability = ''.join([text.strip() for text in availability]).strip()

            yield {
                "title": title,
                "price": price,
                "rating": rating,
                "availability": availability,
            }

        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, self.parse)
