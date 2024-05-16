import pandas as pd
import logging
import sys
from common.DateUtils import DateUtils, CalendarHandler
import datetime as dt
import numpy as np
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        try:
            logger.info('Creacion de plazo de dias')
            trade_date = DateUtils().add_business_days(val_date, 2, calendar)
            tenor_dates = [DateUtils().tenor_date(
                trade_date, x, calendar, starting_day_convention, business_day_convention) for x in tenors]
            days = [(x-trade_date).days for x in tenor_dates]
            on_day = (DateUtils().add_business_days(val_date, 1, calendar) - val_date).days
            
        except AttributeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error en el calculo del conteo de dias de la curva swap. Fallo linea: ' +  error_line)
            logger.error(e)
            raise AttributeError(404)
        except ValueError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error en el calculo del conteo de dias de la curva swap. Fallo linea: ' +  error_line)
            logger.error(e)
            raise ValueError(204)
        except NameError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error en el calculo del conteo de dias de la curva swap. Fallo linea: ' +  error_line)
            logger.error(e)
            raise NameError(404)
        except TypeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error en el calculo del conteo de dias de la curva swap. Fallo linea: ' +  error_line)
            logger.error(e)
            raise TypeError(406)
        except Exception as e:
            if e.args[0] == 409:
                error_line = str(sys.exc_info()[-1].tb_lineno)
                logger.error('Se genero un error en el calculo del conteo de dias de la curva swap. Fallo linea: ' +  error_line)
                logger.error(e)
                raise Exception(409)
            else:
                error_line = str(sys.exc_info()[-1].tb_lineno)
                logger.error('Se genero un error en el calculo del conteo de dias de la curva swap. Fallo linea: ' +  error_line)
                logger.error(e)
                raise Exception(500)
        return on_day, days
