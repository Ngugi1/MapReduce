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
            product_name = "".join(cols[2: (size-4)])
            product_amount = float(cols[size-4])
            product_price = float(cols[size-2])
            yield (('amount', product_id, product_name), product_amount)
            yield (('revenue', product_id, product_name), product_amount * product_price)
        except:
            yield None, None

    def combine_customer_indicators(self, product_id, indicator):
        if product_id != None and indicator != None: # Remove useless elements
            indicator_type, _,_ = product_id
            yield None, (product_id, sum(indicator)) # Send to same reducer
    def reducer_top_customer_by_indicator(self, _, indicator):
        top_amount = ("", "", -1) # Initially no top product
        top_revenue = ("", "", -1)
        item = next(indicator, None) # Go thorough all the products
        while item is not None:
            indicator_type, idx, name = item[0]
            if indicator_type == "revenue" and item[1] > top_revenue[2]:
                top_revenue =  idx, name, item[1]
            if indicator_type == "amount" and item[1] > top_amount[2]:
                top_amount = idx, name, item[1]
            item = next(indicator, None)
        yield "Most Popluar by Revenue - ",  top_revenue
        yield "Most Popluar by Amount - ", top_amount
    # def reducer_find_top_product(self, _ , product):
    #     top_amount = ("", 0) # Initially no top product
    #     top_revenue = ("", 0)
    #     item = next(indicator, None) # Go thorough all the products
    #     while item is not None:
    #         indicator_type, product_name = item[0]
    #         if indicator_type == "revenue" and item[1] > top_revenue:
    #             top_revenue = product_name, item[1]
    #         if indicator_type == "amount" and item[1] > top_amount:
    #             top_amount = product_name, item[1]
    #         item = next(product, None)
    #     yield top_revenue
    #     yield top_amount

    def steps(self):
        return [MRStep(mapper=self.mapper_pre_process_customer,
        combiner=self.combine_customer_indicators,
        reducer=self.reducer_top_customer_by_indicator)]
        # MRStep(reducer=self.reducer_find_top_product)]

if __name__ == '__main__':
    MRTopProduct.run()