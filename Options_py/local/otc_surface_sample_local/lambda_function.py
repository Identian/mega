import datetime as dt
import logging
import sys
import os
import json
import time


import pandas as pd

from functions.Sample import DayCount, SurfaceTransformation
from common.DateUtils import DateUtils
from LocalSurfaceApp import LocalOptions

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    failed_init = False
    val_date = dt.datetime.strptime(os.environ['VALUATION_DATE'], '%Y-%m-%d').date()
    s_val_date = val_date.strftime('%Y-%m-%d')
    logger.info('Inicia el proceso de muestra de superficie de volatilidad local para: ' + s_val_date)
    # Parametros de calendario 
    cal = os.environ['CAL']
    max_years = int(os.environ['MAX_YEARS'])
    # Parametros para el conteo de dias de la superficie de vol 
    starting_day_convention = os.environ['VOL_SURFACE_DAY_STARTING_DAY_CONVENTION']
    business_day_convention_short_1 = os.environ['VOL_SURFACE_DAY_BUSINESS_DAY_CONVENTION_SHORT_1']
    business_day_convention_short_2 = os.environ['VOL_SURFACE_DAY_BUSINESS_DAY_CONVENTION_SHORT_2']
    business_day_convention_long = os.environ['VOL_SURFACE_DAY_BUSINESS_DAY_CONVENTION_LONG']
    # Paramteros de la muestra
    logger.info('Carga de parametros de la muestra')
    min_amount = int(os.environ['VOL_SAMPLE_MIN_AMOUNT'])
    tenors_short = os.environ['TENORS_SHORT'].split(',')
    tenors_long = os.environ['TENORS_LONG'].split(',')
    tenors_long_citi = os.environ['TENORS_LONG_CITI'].split(',')
    jump_tenors = os.environ['TENOR_JUMPS'].split(',')
    dec = int(os.environ['ROUNDING_DEC'])
    tenor_jump = os.environ['TENOR_TO_JUMP']
    strategies = os.environ['STRATEGIES'].split(',')
    closing_hour =  os.environ['CLOSING_HOUR']
    min_minutes = int(os.environ['MIN_MINUTES_SAMPLE'])
except Exception as e:
        failed_init = True
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('Fallo la inicializacion. Fallo linea : '+ error_line)
        logger.error(str(e))

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    
    ----- Precia Parameters
    
    CAL: Nombre del calendario a usar.
    CLOSING_HOUR: Hora de cierre del mercado de opciones en Colombia
    MIN_MINUTES_SAMPLE: Minutos minimos en los cuales un quote debe permanecer para hacer parte de la tercera jerarquia de la muestra. 
    Ademas, es la cantidad de minutos para tomar previos al cierre de mercado, según lo establecido en la metodologia de valoracion.
    STRATEGIES: Estrategias estandar definidas en la metodologaa de valoracion.
    TENORS: Tenores estandar definidos en la metodologia de valoracion (+ nodo ON).
    VALUATION_DATE (opcional): Fecha de valoracion para la realizacion de la muestra de la superficie de volatilidad USDCOP. 
    (Si no esta definida, el codigo toma como default la fecha del dia en el cual se este ejecutando la lambda).
    VOL_SAMPLE_MIN_AMOUNT: Mínima cantidad negociada en un trade o quote de una volatilidad USDCOP para hacer parte de la muestra del dia.
    VOL_SURFACE_DAY_BUSINESS_DAY_CONVENTION: Convencion de dias habiles de la muestra de superficie de volatilidad USDCOP.
    VOL_SURFACE_DAY_STARTING_DAY_CONVENTION: Convencion de inicio del conteo de dias de la muestra de superficie de volatilidad USDCOP.
    
    Returns
    ------
    Resultado Muestra: dict

    """
    if failed_init:
        logger.critical('>>>[lambda_handler] Lambda interrumpida. Motivo: No se completo la inicializacion.')
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Proceso de muestra superficie no inicializado",
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }),
        }
    logger.info("Funcion " + context.function_name)
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    logger.info(event)
    err_mss = 'Se genero un error en la creacion de la muestra de superficie de volatiliadad local. Fallo linea : '
    try:
        #  Llamar calendario CO
        logger.info('Carga calendario: ' + cal)
        logger.info('Calendarios disponibles en el event: ' +str(list(event['calendars'])))
        settCOUS = event['calendars'][cal]
        calendar_dt = [dt.datetime.strptime(x,'%Y-%m-%d').date() for x in settCOUS]
        # Traer los ID PRECIA para la superficie de opciones local
        id_precia_opc = pd.DataFrame(event['id_precia'])
        # Traer informacion de trades-quotes del dia
        broker_info = pd.DataFrame(event['broker_info'])
        # Traer la informacion del salto
        spread_jumps = pd.DataFrame(event['spr_USDCOP'])
        # traer informacion de la superficie de volatilidad local del dia habil anterior
        opc_yest = pd.DataFrame(event['hist_opt']).set_index(
            ['estrategia', 'tenor']).loc[strategies, tenors_short+tenors_long+jump_tenors,:].reset_index().reindex(
                columns=['fecha-valoracion','tenor', 'estrategia', 'mid', 'bid','ask'])
        # Dias de la muestra de la superficie USDCOP
        tenors_copy = tenors_short.copy()
        on_tenor = [tenors_copy.pop(0)]
        logger.info(str(tenors_copy))
        on_day, tenor_days = DayCount().local_fwd_daycount(val_date, calendar_dt, tenors_copy, tenors_long + jump_tenors, starting_day_convention, business_day_convention_short_1,
        business_day_convention_short_2, business_day_convention_long)
        on_day_citi, tenor_days_citi = DayCount().local_fwd_daycount(val_date, calendar_dt, tenors_copy, tenors_long_citi, starting_day_convention, business_day_convention_short_1,
        business_day_convention_short_2, business_day_convention_long)
        days_citi = pd.DataFrame([on_day_citi]+tenor_days_citi)
        # Muestra Superficie USDCOP
        op_usdcop_tenors, opc_today, opc_today_jumps = LocalOptions(s_val_date, trades_quotes=broker_info, hist_sample=opc_yest, 
                                 tenors = tenors_copy+tenors_long+jump_tenors, jump_tenors = jump_tenors, strategies=strategies, logger = logger,closing_hour=closing_hour, 
                                 min_amount = min_amount, quotes_window=min_minutes).USDCOP_strategy_surface_sample()
        logger.info('Exito creando la muestra superficie de volatilidad USDCOP')
        opc_class = SurfaceTransformation()
        logger.info('Inicio conversion de superficie de estrategias a deltas')
        opc_today_d = opc_class.surface_to_deltas(opc_today, op_usdcop_tenors)
        logger.info('Exito conversion de superficie de estrategias a deltas')
        logger.info('Inicio aplicacion de saltos extendidos')
        opc_today_d, spread_jumps = opc_class.surface_jumps(opc_today_d, opc_today_jumps, opc_yest, jump_tenors, spread_jumps, tenor_jump)
        logger.info('Exito saltos extendidos')
        logger.info('Inicio interpolacion Nodo ON')
        opc_today_d = DayCount().add_on_to_deltas(opc_today_d, on_day, tenor_days, dec = dec)
        logger.info('Exito interpolacion Nodo ON')
        logger.info('Inicio conversion superficie en deltas a estrategias')
        opc_today = opc_class.surface_to_strategies(opc_today_d, on_tenor, s_val_date, pd.concat([opc_today, opc_today_jumps]).sort_index(), dec = dec)
        logger.info('Inicio creacion archivos de salida')
        # Insercion de informacion a BD
        output_cols = opc_yest.columns.to_list()
        output_cols.append('tipo-precio')
        opc_today.columns = output_cols
        opc_today = opc_today.merge(id_precia_opc, on=['estrategia', 'tenor'],how='left')
        opc_today.rename(columns = {'id_precia':'id-precia'}, inplace = True)
        opc_strategies = json.loads(opc_today.to_json(orient= 'records'))
        logger.debug(opc_today_d.to_string())
        opc_deltas = json.loads(opc_today_d.to_json(orient='records'))
        spread_jumps_dict = json.loads(spread_jumps.to_json(orient='records'))
        days_citi_dict = json.loads(pd.DataFrame(days_citi).to_json(orient = 'records'))
        
    except AttributeError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss + error_line)
        logger.error(e)
        raise AttributeError(404)
    except ValueError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss + error_line)
        logger.error(e)
        raise ValueError(204)
    except NameError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss + error_line)
        logger.error(e)
        raise NameError(404)
    except TypeError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss + error_line)
        logger.error(e)
        raise TypeError(406)
    except IndexError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss + error_line)
        logger.error(e)
        raise IndexError(406)
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss + error_line)
        logger.error(e)
        raise Exception(500)
    logger.info('Proceso de muestra superficie local existoso')
    return {"statusCode": 200,
                "body":{
                    'opt_strategies':{'data': opc_strategies},
                    'opt_deltas': {'data':opc_deltas},
                    'spr_USDCOP': {'data':spread_jumps_dict},
                    'd_opciones_citi': {'data':days_citi_dict}
        }
    }