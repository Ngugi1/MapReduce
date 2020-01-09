from mrjob.job import MRJob, MRStep
import re
from datetime import datetime
class MRWordFreqCount(MRJob):
    def mapper_pre_process_customer(self, _, line):
        try: # This prevents us from the first line of csv
            # Get components of the record
            spit_line = line.split(",") 
            length = len(spit_line) - 1
            # Customer Id
            customerId = spit_line[length-1]
            # Price
            price = float(spit_line[length-2])
            # year
            year = spit_line[length-3].split(" ")[0].split("/")[-1]
            # Quantity
            quantity = float(spit_line[length-4])
            yield (customerId, year),  (price * quantity)
        except:
            yield None, 0
    def combine_customer_purchases(self, id_year, amounts):
        if id_year != None: # Remove useless elements
            customerId, year = id_year
            yield year, (customerId, year, sum(amounts))
    def reducer_top_10_per_year(self, year, customer_info):
        buffer = [(None, year, -1) for i in range(10)]
        item = next(customer_info, None)
        while item is not None:
            _, _, buffered_amount = buffer[0]
            _, _, amount = item
            if buffered_amount < amount:
                buffer[0] = item
            buffer.sort(key=lambda x: x[2])
            item = next(customer_info, None)
        for item in buffer:
            yield year, item
    def reducer_find_top_customers(self, year, top_10_customers):
        for item in top_10_customers:
            customerId, _, amount = item
            yield year, "Customer Id = {} and Amount = {}".format(customerId, amount)

    def steps(self):
        return [MRStep(mapper=self.mapper_pre_process_customer,
        combiner=self.combine_customer_purchases,
        reducer=self.reducer_top_10_per_year),
        MRStep(reducer=self.reducer_find_top_customers)]

if __name__ == '__main__':
    MRWordFreqCount.run()