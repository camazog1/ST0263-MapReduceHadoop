from mrjob.job import MRJob
import csv

class ContarPorMunicipio(MRJob):

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line], delimiter=','))

            if row[0] == "Fecha":
                return

            municipio = row[-1].strip()
            yield municipio, 1
        except Exception as e:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    ContarPorMunicipio.run()
