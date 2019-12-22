from mrjob.job import MRJob, MRStep
import re
import numpy as np
from datetime import datetime
count = 0
class MRTopProduct(MRJob):
    def mapper_pre_process_customer(self, _, line):
        try:
            cols = line.split(',')
            size = len(cols) - 1
            product_id = cols[1]
            product_name = "Top"
            product_amount = float(cols[size-4])
            product_price = float(cols[size-2])
            # Use combination of product id and product name as key and keyword to differentiate quantity and revenue
            yield ('amount', product_id, product_name), product_amount
            yield ('revenue', product_id, product_name), product_amount * product_price
        except:
            yield None, None
    def combine_customer_indicators(self, product_id, indicator):
        if product_id != None: # Remove useless elements
            yield product_id, (product_id, sum(indicator)) # Send all product IDs with same ID to same reducer
    def reducer_top_customer_by_indicator(self, product_id, indicator):
        try:
            yield None, (product_id, sum(indicator)) # Get the final reduced value of each product - send all to same reducer
        except:
            x = 1
    def reducer_find_top_product(self, _ , product):
        top_amount = ("", 0) # Initially no top product
        top_revenue = ("", 0)
        item = next(indicator, None) # Go thorough all the products
        while item is not None:
            indicator_type, product_name = item[0]
            if indicator_type == "revenue" and item[1] > top_revenue:
                top_revenue = product_name, item[1]
            if indicator_type == "amount" and item[1] > top_amount:
                top_amount = product_name, item[1]
            item = next(product, None)
        yield top_revenue
        yield top_amount

    def steps(self):
        return [MRStep(mapper=self.mapper_pre_process_customer,
        combiner=self.combine_customer_indicators,
        reducer=self.reducer_top_customer_by_indicator),
        MRStep(reducer=self.reducer_find_top_product)]

if __name__ == '__main__':
    MRTopProduct.run()