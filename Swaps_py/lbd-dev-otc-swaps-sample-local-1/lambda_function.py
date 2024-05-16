import logging
import sys
import os
import json
import time
import pandas as pd
import numpy as np
import datetime as dt
from LocalSwapApp import LocalSwaps
logger = logging.getLogger()
logger.setLevel(logging.INFO)
pd.options.display.float_format = '{:20,.2f}'.format
try:
    failed_init = False
    logger.info('Inicia el proceso de la muestra de swap local')
    #Parametros
    closing_hour = os.environ['CLOSING_HOUR']
    min_minutes = int(os.environ['MIN_MINUTES_SAMPLE'])
    active_minutes = int(os.environ['ACTIVE_MINUTES'])
    ba_percentile = float(os.environ['BA_PERCENTILE'])
    dec = int(os.environ['ROUNDING'])
except Exception as e:
        failed_init = True
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('Se genero un error cargando las variables de entorno. Fallo linea : '+ error_line)
        logger.error(e)

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
    
    CLOSING_HOUR: Hora de cierre de la jornada de negociacion de swaps
    MIN_MINUTES_SAMPLE: Minutos minimos previos al cierre de la jornada de negociacion para establecer un intervalo de convocatoria,
    el cual es utilizado en la creacion de la muestra swap
    ACTIVE_MINUTES: Minutos minimos activos de un quote para tenerse en cuenta dentro de la muestra swap,
    aplicables al criterio jerarquico 3 y 5
    BA_PERCENTILE: Percentil del bid-ask spread para definir el bid/offer spread para filtro de puntas de compra y venta
    ROUNDING: Numero de decimales a redondear la muestra swap
    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    if failed_init:
        logger.critical('>>>[lambda_handler] Lambda interrumpida. Motivo: No se completo la inicializacion.')
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Proceso de muestra swap local no inicializado",
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }),
        }
    logger.info("Funcion " + context.function_name)
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    logger.info(event)
    add_dummie_node(event=event)
    try:
        try:
            intraday = eval(event['intraday'])
        except:
            intraday = False
        logger.info('Seleccion de la Curva par swap y de los parametros en su uso')
        swap = event['swap_curve']
        err_mss = 'Se genero un error en la creacion de la muestra swap '+ swap +'. Fallo linea : '
        logger.info('Curva a crear la muestra: '+ swap)
        val_date = dt.datetime.strptime(event['valuation_date'], '%Y-%m-%d').date()
        s_val_date = val_date.strftime('%Y-%m-%d')
        logger.info('Fecha de Valoracion: ' + s_val_date)
        swap_nodes = json.loads(os.environ['SWAP_NODES'])[swap]
        swap_nodes_sections = json.loads(os.environ['SWAP_NODES_SECTIONS'])[swap]
        
        logger.info('Lectura de los ID PRECIA para la curva swap especifica')
        try:
            id_precia_swp = pd.DataFrame(event['id_precia'])
        except:
            raise ValueError('No hay informacion en el evento de id_precia')
        logger.info('Lectura informacion de trades-quotes del dia, para la curva: '+ swap)
        try:
            broker_info = pd.DataFrame(event['broker_info'])
        except:
            raise ValueError('No hay informacion en el evento sobre broker_info')
        logger.info('Lectura informacion del DV01 para los filtros de las muestra: '+ swap)
        try:
            if intraday:
                dv01 = pd.DataFrame()
            else:
                dv01 = pd.DataFrame(event['dv01'])
        except:
            raise ValueError('No hay informacion de DV01 para la muestra swap: '+ swap)
        logger.info('Lectura del filtro bid-ask spread, para la curva: '+ swap)
        try:
            bid_ask_spread = pd.DataFrame(event['ba_spread'])
        except:
            raise ValueError('No hay informacion de bid-ask spread para la muestra swap: '+ swap)
        logger.info('Lectura de la muestra swap ' + swap + ' del dia habil previo')
        try:
            swap_yest = pd.DataFrame(event['hist_swp'])
        except:
            raise ValueError('No hay informacion de la muestra swap ' +swap+ ' del dia habil anterior')
        try:
            tenor_days = event['daycount']['swap_days']
        except:
            raise ValueError('No estan el conteo de dias de la curva ' + swap)

        swap_nodes = json.loads(os.environ['SWAP_NODES'])[swap]
        swap_nodes_copy = swap_nodes.copy()
        on_tenor = [swap_nodes_copy.pop(0)]
        logger.info('Calculo del bid/offer spread de la curva: '+ swap + '. Con percentil: '+str(ba_percentile))
        limit_spread = bid_ask_spread.groupby('tenor').apply(lambda x: np.percentile(x['bid-ask-spread'],ba_percentile)).to_frame()
        logger.info("Valor percentil: "+ limit_spread.to_string())
        logger.info('Creacion de la muestra')
        if broker_info.empty:
            swap_today = swap_yest.drop(swap_yest[swap_yest.tenor =='ON'].index)
            swap_today.dias = swap_today.dias.astype(int)
            swap_today.sort_values('dias', inplace=True)
            logger.info(swap_today.to_string())
            swap_today['tipo-precio'] = 'hist_price_8'
            swap_today['dias'] = tenor_days
            logger.info(swap_today.to_string())
        else:
            swap_today = LocalSwaps(swap, broker_info, dv01, swap_yest, limit_spread, swap_nodes_copy, 
            swap_nodes_sections, tenor_days, closing_hour, logger, min_minutes, active_minutes, dec).local_sample()
            logger.info('Union ID PRECIA')
            swap_today = swap_today.merge(id_precia_swp, on=['tenor'],how='left')
        logger.info("Muestra swap \n" + swap_today.to_string())
        swap_today['fecha-valoracion'] = s_val_date
        limit_spread = limit_spread.join(
            pd.DataFrame({'tenor':swap_nodes_copy, 'dias':tenor_days}
            ).set_index('tenor')).sort_values('dias').drop(['dias'], axis =1)
        bid_ask_spread_today = pd.DataFrame()
        logger.info('Calculo bid/offer spread del dia de la muestra')
        bid_ask_spread.fecha = pd.to_datetime(bid_ask_spread.fecha, format = '%Y-%m-%d')
        bid_ask_spread_yest = bid_ask_spread.loc[bid_ask_spread.fecha == bid_ask_spread.fecha.max(),:]
        if broker_info.empty:
            bid_ask_spread_today = bid_ask_spread_yest
            bid_ask_spread_today['fecha'] = s_val_date
            bid_ask_spread_today['instrumento'] = swap
        else:
            bid_ask_spread_today = LocalSwaps(swap, broker_info, dv01, swap_yest, limit_spread, swap_nodes_copy, 
            swap_nodes_sections, tenor_days, closing_hour, logger, min_minutes, active_minutes, dec).bid_ask_spread_builder(bid_ask_spread_yest,intraday)
            bid_ask_spread_today['fecha'] = s_val_date
        bid_ask_spread_today_dict = bid_ask_spread_today.to_dict(orient= 'records')
        swap_today_dict = swap_today.to_dict(orient= 'records')
        limit_spread_dict = (limit_spread.reset_index()).to_dict(orient = 'records')
    except AttributeError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss+ error_line)
        logger.error(e)
        raise AttributeError(404)
    except ValueError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss+ error_line)
        logger.error(e)
        raise ValueError(204)
    except NameError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss+ error_line)
        logger.error(e)
        raise NameError(404)
    except TypeError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss+ error_line)
        logger.error(e)
        raise TypeError(406)
    except IndexError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss+ error_line)
        logger.error(e)
        raise IndexError(406)
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(err_mss+ error_line)
        logger.error(e)
        raise Exception(500)
    logger.info('Proceso de muestra swap local: '+ swap+' existoso')
    return {
            "statusCode": 200,
            "body":{
                "ba_spread": {"data":bid_ask_spread_today_dict},
                "swap_local": {"data":swap_today_dict},
                "limit_spread": {"data":limit_spread_dict}
            }
    }

def add_dummie_node(event: dict):
    """
    Agrega un nodo dummie al evento de entrada de la lambda para evitar que el
    codigo falle al aplicar los criterios de seleccion de la muestra (en un
    fututuro deberia considerase corregir el codigo de la muestra)
    
    Args:
        event (dict): Evento de entrada de la lambda
    """    
    dummie_node = {
        "sistema": "ICAP",
        "hora-inicial": "09:00:00",
        "hora-final": "12:00:00",
        "tenor": "30Y",
        "nominal": 0,
        "tipo-orden": "BID",
        "precio": -99999999999999,
    }
    event["broker_info"].append(dummie_node)