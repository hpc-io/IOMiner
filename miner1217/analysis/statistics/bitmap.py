class Bitmap(object):
    def __init__(self, max):
        self.size = int((max + 31 - 1)/31)
        self.array = [0 for i in range(self.size)]

    def calcElemIndex(self, num, up = False):
        if up:
            return int((num + 31 - 1 /31))
        return num/31

    def calcBitIndex(self, num):
        return num%31
   
    def set(self, num):
       elemIndex = self.calcElemIndex(num)
       byteIndex = self.calcBitIndex(num)
       elem = self.array[elemIndex]
       self.array[elemIndex] = elem | (1 << byteIndex)
    def clean(self, i):
        elemIndex = self.calcElemIndex(i)
        byteIndex = self.calcBitIndex(i)
        elem = self.array[elemIndex]
        self.array[elemIndex] = elem & (~(1 << byteIndex))
    def test(self, i):
        elemIndex = self.calcElemIndex(i)
        byteIndex = self.calcBitIndex(i)
        if self.array[elemIndex] & (1<<byteIndex):
            return True;
        return False;

    def orme(self, other):
        for i in range(0, self.size):
            self.array[i] = self.array[i] | other.array[i]
    def andme(self, other):
        for i in range(0, self.size):
            self.array[i] = self.array[i] & other.array[i]

#bitmap = Bitmap(90)
#print "element count is %d\n"%(bitmap.size)
#print "62 is on %dth elem\n"%bitmap.calcElemIndex(0)
#print "47 is stored on %dth array and %dth bit"%(bitmap.calcElemIndex(47), bitmap.calcBitIndex(47))
#

#bitmap = Bitmap(90)
#bitmap.set(0)
##bitmap.set(34)
#print bitmap.array
#print bitmap.test(0)
#bitmap.set(1)
#print bitmap.test(1)
#print bitmap.test(2)
##print bitmap.array
#bitmap.clean(1)
#print bitmap.test(1)
