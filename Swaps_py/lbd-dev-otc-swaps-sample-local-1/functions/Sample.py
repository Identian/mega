import pandas as pd
import logging
import sys
import numpy as np
from common.DateUtils import DateUtils

logger = logging.getLogger()
logger.setLevel(logging.INFO)
class SamplingSwaps():
    """Clase con los metodos generales para la creacion de a la muestra de swaps
    """
    def summary_swp(self, data):
        """Funcionales utiles para la conformacion de la muestra swap:
            tasa ponderada por monto, valor maximo y valor minimo de una seleccion de trades-quotes
            
        Args:
            data (pd.DataFrame): DataFrame de trades y/o quotes para aplicar los funcionales
        Return:
            res (pd.DataFrame): DataFrame con los resultados de los funcionales
        """
        try:
            result = {
                'weighted': sum(data['precio'] * data['nominal'])/sum(data['nominal']),
                'max': data['precio'].max(),
                'min': data['precio'].min()
            }
            res = pd.Series(result)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error calculando estadisticas de la muestra de swaps. Fallo linea: ' + error_line)
            logger.error(e)
            raise Exception(500)        
        return res
    def match_warrant_today(self, data, swp_today, warrant):
        """Seleccion de filas conformes con los criterios de datos y tipo de orden
        para insertar en la posicion especifica de la muestra swap local
        
        Args:
            data (pd.DataFrame): DataFrame con multiples indices que contiene los resultados de uno de los 8 criterios jerarquicos 
            aplicados a las muestras de swap locales
            swp_today (pd.DataFrame): Muestra de swap local
            warrant (str): Tipo de orden por la cual filtrar
        Return:
            
        """
        try:
            data_warrant = data.iloc[data.index.get_level_values('tipo-orden') == warrant]
            swp_warrant_data = swp_today.join(data_warrant.reset_index(level = ['tipo-orden']), on=['tenor'],how='left')
            pos_warrant = swp_warrant_data.loc[swp_warrant_data['tipo-orden'] == warrant].index.to_list()
            rows = swp_warrant_data.iloc[pos_warrant]
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error con el match de ordenes. Fallo linea: '+error_line)
            logger.error(e)
            raise Exception(500)
        return  rows
    
class DayCount():
    """Clase que realiza los metodos de conteos de dias especificos dentro de la muestra swaps locales (IBR, IBRUVR, USDCO).
    """
    def local_swaps_daycount(self, val_date, calendar, tenors, starting_day_convention ='spot_starting', business_day_convention = 'modified_following'):
        """Conteo de dias de los swaps locales
        Se encuentra las fechas de cada tenor a tener en cuenta desde la fecha efectiva, con una convencion de inicio de dia y de dia tipo de busqueda de dia bursatil
        A todas estas fechas, se les resta dos dias efectivos y se hayan los dias al plazo del tenor como la resta entre estas fechas y la fecha de valoracion
        Para el nodo ON, se haya el siguiente dia habil al dia de valoracion y se realiza la respectiva resta de fechas
        
        Args:
            val_date (dt.date): Fecha de valoracion
            calendar (list): Listado de fechas (dt.date) de un calendario de dias habiles (Colombia)
            tenors (list): Listado de tenores a los cuales se les hallara la fecha y los dias tenor
            starting_day_convetion (str): Parametro de convecion de inio del conteo
            business_day_convetion (str): Parametro de convecion de busqueda de dias bursatiles
        Return:
            on_day (int): Dia ON
            days (list): Listado de dias al tenor para los items de la variable tenors
        """
        err_mss = 'Se genero un error en el calculo del conteo de dias de la curva swap. Fallo linea: '
        try:
            logger.info('Creacion de plazo de dias')
            tenor_dates = [DateUtils().tenor_date(
                val_date, x, calendar, starting_day_convention, business_day_convention) for x in tenors]
            spot_date = DateUtils().starting_date(val_date, calendar, starting_day_convention)
            days = [(x-spot_date).days for x in tenor_dates]
            on_day = (DateUtils().add_business_days(val_date, 1, calendar) - val_date).days
            
        except AttributeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except ValueError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except NameError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except TypeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        return on_day, days
        
