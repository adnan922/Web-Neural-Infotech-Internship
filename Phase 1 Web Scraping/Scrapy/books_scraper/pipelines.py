import csv

class CSVPipeline:
    def open_spider(self, spider):
        self.file = open("books.csv", "w", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.file, fieldnames=["title", "price", "availability", "rating"])
        self.writer.writeheader()

    def process_item(self, item, spider):
        # item["price"] = item["price"].replace("Â£", "").strip()
        item["price"] = ''.join([c for c in item["price"] if c.isdigit() or c == '.'])
        rating_mapping = {
            "One": 1,
            "Two": 2,
            "Three": 3,
            "Four": 4,
            "Five": 5
        }
        item["rating"] = rating_mapping.get(item["rating"], 0)  # Default to 0 if no match

        self.writer.writerow(item)
        return item

    def close_spider(self, spider):
        self.file.close()
