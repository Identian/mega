import sys
from scipy import interpolate
import numpy as np
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class Interpol():
    """Clase que contiene los metodos para interpolacion de distintos vectores numericos
    """
    def __init__(self, x_nodes, y_nodes, x_points):
        validate_x_nodes = not (isinstance(x_nodes, list) | isinstance(x_nodes, np.ndarray))
        validate_y_nodes = not (isinstance(y_nodes, list) | isinstance(y_nodes, np.ndarray))
        validate_x_points = not (isinstance(x_points, list) | isinstance(x_points, np.ndarray)|
                                 isinstance(x_points, int) | isinstance(x_points, float))
        if (validate_x_nodes or validate_y_nodes or validate_x_points):
            not_interpolable = {'dias_x': validate_x_nodes, 
                                'valores_y' : validate_y_nodes ,
                                'dias_interpol_x': validate_x_points}
            not_interpolable_s = {key: value for key, value in not_interpolable.items() if value == True}
            logger.error('No se puede interpolar')
            logger.error('La variable: ' + str([name for name in not_interpolable_s.keys()]) + 
                            ' no es lista o array')
            raise TypeError()
        if (len(x_nodes) != len(y_nodes)):
            logger.error('La longitud de dias_x y valores_y no coincide, por lo que no se puede generar la interpolacion')
            raise TypeError()
        self.x_nodes = x_nodes
        self.y_nodes = y_nodes
        self.x_points = x_points
    
    def method(self, interpol_method):
        """Aplicacion del metodo de interpolacion seleccionado
        """
        switcher = {
            'linear_interpol': self.linear_interpol,
            'cubic_splines_interpol': self.cubic_splines_interpol}
        try:
            case = switcher.get(interpol_method, 'error')
            if (case == 'error'):
                msg = 'metodo de interpolacion invalido %s' % interpol_method
                raise ValueError(msg)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('[Interpolacion] No se pudo seleccionar el metodo de interpolacion. Fallo en linea: ' + error_line + '. Razon: '+ str(e))
            raise ValueError('[Interpolacion] No se pudo realizar el metodo de interpolacion')
        return case()
            
    def linear_interpol(self):
        """Interpolacion lineal por medio del scipy-intepolate.
        Formula: y = (x-x1)/(x2-x1) * (y2- y1) + y1
        
        Args:
            x_nodes (list, numpy.ndarray): Vector/arreglo de puntos de 1 dimension del eje X
            y_nodes (list, numpy.ndarray): Vector/arreglo de N dimensiones del eje Y
            x_points (list, numpy.ndarray): Vector/arreglo de 1 dimension con los valores del eje X a interpolar
        Return:
            y_points (numpy.ndarray): Vector de 1 dimension con los valores interpolados de Y
        """
        try:
            f_interpolate = interpolate.interp1d(self.x_nodes, self.y_nodes, kind = 'linear', fill_value= 'extrapolate')
            y_out = f_interpolate(self.x_points)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('[Interpolacion] No se pudo realizar la interpolacion. Fallo en linea: ' + error_line + '. Motivo: '+ str(e))
            raise ValueError('[Interpolacion] No se pudo realizar la interpolacion')
        return y_out

    def cubic_splines_interpol(self):
        """Interpolacion cubica por medio del scipy-intepolate-cubic.        
        Args:
            x_nodes (list, numpy.ndarray): Vector/arreglo de puntos de 1 dimension del eje X
            y_nodes (list, numpy.ndarray): Vector/arreglo de N dimensiones del eje Y
            x_points (list, numpy.ndarray): Vector]/arreglo de 1 dimension con los valores del eje X a interpolar
        Return:
            y_points (numpy.ndarray): Vector de 1 dimension con los valores interpolados de Y
        """
        try:
            f_interpolate = interpolate.interp1d(self.x_nodes, self.y_nodes, kind = 'cubic', fill_value= 'extrapolate')
            y_out = f_interpolate(self.x_points)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('[Interpolacion] No se pudo realizar la interpolacion. Fallo en linea: ' + error_line + '. Motivo: '+ str(e))
            raise ValueError('[Interpolacion] No se pudo realizar la interpolacion')
        return y_out 