from itertools import count
from mrjob.job import MRJob
from mrjob.step import MRStep

class OrderByProductSorted(MRJob):
    def steps(self):
        return [
                MRStep(mapper=self.mapper_customer,
                reducer=self.reducer_totals_customer),

                MRStep(mapper=self.mapper_amount_transaction,
                reducer=self.reducer_totals_amount_transaction),

                MRStep(mapper=self.mapper_product_transaction,
                reducer=self.reducer_totals_product_transaction)
]
    def mapper_amount_transaction(self, _, line):
        (Product, Type, amount_transaction) = line.split(',')
        yield Product, Type, int(amount_transaction)

    def reducer_totals_amount_transaction(self, Product, Type, amount_transaction):
        yield Product, Type, sum(amount_transaction)


    def mapper_mapper_customer(self, Product, Type, id_customer):
        yield  Product, Type, int(id_customer)

    def reducer_totals_customer(self, Product, Type, id_customers):
        sum(id_customers)
        for id_customer in id_customers:
            yield Product, Type, count(id_customer)

    def mapper_product_transaction(self, Product, Type):
        yield  Product, Type

    def reducer_totals_product_transaction(self, Product, Types):
        sum(Types)
        for Type in Types:
            yield Product, count(Type)

if __name__== '__main__':
    OrderByProductSorted(MRJob).run()

