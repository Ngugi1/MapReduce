from mrjob.job import MRJob, MRStep
import numpy as np
row_count_m = 0
row_count_n = 0
m_rows = 4
m_cols = 3
n_rows = 3
n_cols = 4
result_matrix = np.zeros((m_rows,n_cols))
class MRDotProduct(MRJob):
    def mapper_pre_process_matrix(self, _, line):
        global row_count_m, row_count_n, m_rows, m_cols, n_rows, n_cols
        # Matrix A - m_rows x m_cols, Matrix B - (m_cols == n_rows) x n_cols
        # We have read a line
        row = line.split(" ") # Split the line to get all elements of a matrix row
        if  len(row) == m_cols: # Process first matrix
            for m_col in range(len(row)): # Go through every element of the row
                # Replicate this element as many times as the columns in the
                # other matrix
                for n_col in range(n_cols):
                    yield (row_count_m, n_col), ('M',m_col, float(row[m_col])) # Remember the column this item belongs to
            row_count_m = row_count_m + 1
        else:
            for n_col in range(len(row)):
                # Replicate element of N as many times as the rows in M matrix to produce all components that need to be mulplied
                for m_row in range(m_rows):
                    yield (m_row, n_col), ('N',row_count_n, float(row[n_col]))
            row_count_n = row_count_n + 1
    def combiner_multi_pairs(self, key, value):
        item = next(value, None)
        while item is not None:
            (matrix, r_c, val) = item
            (r,c) = key
            item = next(value, None)
            yield (r,c,r_c), val
    def reducer_additive_pairs(self, key, value):
        (r,c,_) = key
        item = next(value, None)
        item2 = next(value, None)
        yield (r,c), (item*item2)

    def reducer_add(self, key, value):
        r,c = key
        result_matrix[r][c] = sum(value)
    def steps(self):
        return [MRStep(mapper=self.mapper_pre_process_matrix,
        combiner=self.combiner_multi_pairs,
        reducer=self.reducer_additive_pairs),
        MRStep(reducer=self.reducer_add)]
       

if __name__ == '__main__':
    MRDotProduct.run()
    print (result_matrix)