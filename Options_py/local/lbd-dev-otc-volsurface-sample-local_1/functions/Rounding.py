"""
Metodos de redondeo en Precia
"""
from functions.Decorators.Decorator import rd_vectorized
@rd_vectorized
def rd(x, y=0):
    ''' A classical mathematical rounding by Voznica '''
    m = int('1'+'0' * y) # multiplier - how many positions to the right
    q = round(x*m, 2) # shift to the right by multiplier
    c = int(q) # new number
    i = int( (q-c)*10 ) # indicator number on the right
    if i >= 5:
        c += 1
    return c/m