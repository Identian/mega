"""
Metodos decoradores de funciones en Precia
"""
import numpy as np
def rd_vectorized(func) :
    def wrapper(x, y):
        rd_vec = np.vectorize(func)
        return rd_vec(x, y)
    return wrapper