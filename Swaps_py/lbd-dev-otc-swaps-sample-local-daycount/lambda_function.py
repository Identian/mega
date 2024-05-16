import os
import sys
import json
import logging
import datetime as dt
from functions.Sample import DayCount

logger = logging.getLogger()
logger.setLevel(logging.INFO)
try:
    failed_init = False
    logger.info('Inicia el proceso del conteo de dias de la muestra de swap local')
    tenors_medium = os.environ['TENORS_USDCO_MEDIUM'].split(',')
    tenors_long = os.environ['TENORS_USDCO_LONG'].split(',')
except Exception as e:
        failed_init = True
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('Se genero un error cargando las variables de entorno. Fallo linea : '+ error_line)
        logger.error(e)

def lambda_handler(event, context):
    """
    """
    try:
        try:
            val_date = dt.datetime.strptime(event['valuation_date'], '%Y-%m-%d').date()
        except:
            val_date = dt.date.today()
        swap = event['swap_curve']
        logger.info('Inicio calculo de dias bursatiles hasta el pago del tenor para el swap: '+ swap+ '. Para el: ' + dt.datetime.strftime(val_date,'%Y-%m-%d'))
        swap_cal = list(event['calendars'].keys())[0]
        swap_starting_convention = json.loads(os.environ['SWAP_STARTING_DAY_CONVENTION'])[swap]
        swap_business_convention = json.loads(os.environ['SWAP_BUSINESS_DAY_CONVENTION'])[swap]
        logger.info('Lectura del calendario: '+ swap_cal)
        settCalendar = event['calendars'][swap_cal]
        calendar_dt = [dt.datetime.strptime(x,'%Y-%m-%d').date() for x in settCalendar]
        swap_nodes = json.loads(os.environ['SWAP_NODES'])[swap]
        swap_nodes_copy = swap_nodes.copy()
        on_tenor = [swap_nodes_copy.pop(0)]
        on_day, tenor_days = DayCount().local_swaps_daycount(val_date, calendar_dt, swap_nodes_copy, 
        swap_starting_convention, swap_business_convention)
    except AttributeError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('Se genero un error en el calculo de dias de la muestra swap '+ swap +'. Fallo linea : '+ error_line)
        logger.error(e)
        raise AttributeError(404)
    except ValueError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('Se genero un error en el calculo de dias de la muestra swap '+ swap +'. Fallo linea : '+ error_line)
        logger.error(e)
        raise ValueError(204)
    except NameError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('Se genero un error en el calculo de dias de la muestra swap '+ swap +'. Fallo linea : '+ error_line)
        logger.error(e)
        raise NameError(404)
    except TypeError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('Se genero un error en el calculo de dias de la muestra swap '+ swap +'. Fallo linea : '+ error_line)
        logger.error(e)
        raise TypeError(406)
    except IndexError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('Se genero un error en el calculo de dias de la muestra swap '+ swap +'. Fallo linea : '+ error_line)
        logger.error(e)
        raise IndexError(406)
    except Exception as e:
        if e.args[0] == 409:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error en el calculo de dias de la muestra swap '+ swap +'. Fallo linea : '+ error_line)
            logger.error(e)
            raise Exception(409)
        else:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error en el calculo de dias de la muestra swap '+ swap +'. Fallo linea : '+ error_line)
            logger.error(e)
            raise Exception(500)
    logger.info('Proceso de muestra swap local: '+ swap+' existoso')
    return {
        'body': {'on_day': on_day,
        'swap_days': tenor_days}
    }
