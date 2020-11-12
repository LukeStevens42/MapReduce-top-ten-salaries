from mrjob.job import MRJob
from mrjob.step import MRStep

class MRtop_ten(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper,reducer=self.reducer),\
                MRStep(mapper = self.secondmapper, reducer = self.secondreducer)]

    def mapper(self,_,lines):
        salaries = lines.split()
        for sal in salaries:
            yield sal, 1

    def reducer(self,key,values):
        yield key, sum(values)

    def secondmapper(self, salary, num_sal):
        yield None, (int(salary), num_sal)

    def secondreducer(self,key,values):
        self.alist = []
        for value in values:
            self.alist.append(value)
        self.blist = []
        for i in range(10):
            self.blist.append(max(self.alist))
            self.alist.remove(max(self.alist))
        for i in range(10):
            yield self.blist[i]

if __name__ == '__main__':
    MRtop_ten.run()
