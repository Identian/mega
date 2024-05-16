#
# =============================================================
#
# Nombre: lbd-utility-calendars-get
# Tipo: Lambda AWS
#
# Autores:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 03/06/2022
#
#
# Lambda que consulta los calendarios a la base de datos 
# precia_utils, actualmente disponible CO, US y su combinacion.
# Los calendarios inician desde 2018-01-01 hasta 2142-12-31
# Se requiere actualizar las fechas no bursatiles directamente
# a la base de datos
#
#
# Variables de entorno:
# DB_SCHEME: precia_utils
# DB_TABLE: precia_utils_calendars
# PAGINATION_MAX_LIMIT: 1000
# SECRET_DB_NAME: precia/rds8/optimusk/utils
# SECRET_DB_REGION: us-east-1
#
# Requerimientos:
# Pandas
# capa-precia-utils
#
# =============================================================
#

from datetime import datetime
import json
import logging
import sys

import precia_utils as utils
import pandas as pd


params_to_columns_dict = {
    'fields': {  # Diccionario de equivalencias entre columnas del db y params de url
        'CO':'bvc_calendar',
        'US':'federal_reserve_calendar',
    },
    'query_params': {  # Diccionario para sanitizar
        'fecha-final': {'column_name': 'dates_calendar', 'traits': {'type': 'date', 'length': 10, 'list': False}},
        'fecha-inicial': {'column_name': 'dates_calendar', 'traits': {'type': 'date', 'length': 10, 'list': False}}
    }
}

logger = logging.getLogger()  # Log, ver en CloaudWatch por log_group, log_stream, request_id
logger.setLevel(logging.DEBUG)
error_message = ''  # Mensaje de error para el cliente
failed_init = False  # Evita timeouts causados por errores en el codigo de inicializacion

try:
    logger.info('[INIT] Inicializando Lambda ...') 
    max_total_results = int(utils.get_environment_variable('PAGINATION_MAX_LIMIT'))
    db_scheme = utils.get_environment_variable('DB_SCHEME')
    secret_db_region = utils.get_environment_variable('SECRET_DB_REGION')
    secret_db_name = utils.get_environment_variable('SECRET_DB_NAME')
    db_secret = utils.get_secret(secret_db_region, secret_db_name)
    db_table = utils.get_environment_variable('DB_TABLE')
    logger.info('[INIT] Inicialización exitosa.')
except Exception as e:
    error_message = str(e)
    failed_init = True



def lambda_handler(event, context):
    try:
        db_connection = utils.connect_to_db(db_scheme, db_secret)
        data_response = execute_main(event, context, db_connection)
        return data_response
    except Exception as e:
        error_message = str(e)
        failed_init = True
    finally:
        db_connection.close()
        

def execute_main(event, context, db_connection):
    """
    Funcion que hace las veces de main en Lambas AWS
    :param event: JSON con los datos entregados a la Lambda por desencadenador
    :param context: JSON con el contexto del Lambda suministrado por AWS
    :return: JSON con los datos solicitados o mensaje de error
    """
    now_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if failed_init:  # Evita timeouts causados por errores en el codigo de inicializacion
        logger.critical('[lambda_handler] Lambda interrumpida. No se completo la inicializacion.')
        global error_message
        error_response = utils.create_error_response(500, error_message, now_time_str,context)
        return error_response

    logger.debug('[lambda_handler] event: ' + str(json.dumps(event)))

    try:
        logger.info("[event]" + str(event))
        logger.info("[lambda_handler] Obteniendo params: event['queryStringParameters'] ...")
        url_params = event['queryStringParameters']
        logger.debug('[lambda_handler] url_params = ' + str(url_params))
        logger.info('[lambda_handler] params encontrados.')
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(
            '[lambda_handler] event no tiene el formato esperado. Fallo en linea: ' + error_line + '. Falta: ' + str(e))
        logger.critical('[lambda_handler] Lambda interrumpida. Integration Request del API no configurado.')
        error_message = 'API no configurada: Integration Request'
        error_response = utils.create_error_response(500, error_message, now_time_str, context)
        return error_response

    try:
        logger.info(
            '[lambda_handler] Decodificando queries (total_results_query, data_query) a partir de los params URL ...')
        data_query, total_results_query = decode_queries(url_params, now_time_str)
        logger.info('[lambda_handler] Decodificaciones exitosa.')
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(
            '[lambda_handler] No se decodifico los queries. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
        logger.critical('[lambda_handler] Lambda interrumpida. Parametros de URL incorrectos.')
        error_response = utils.create_error_response(400, 'Parametros de URL incorrectos: ' + str(e), now_time_str, context)
        return error_response

    try:
        logger.info('[lambda_handler] Obteniendo total de resultados ...')
        total_results = count_total_results(total_results_query, db_connection)
        logger.info('[lambda_handler] total_results = ' + str(total_results))
        if total_results == 0:
            logger.info('[lambda_handler] Creando respuesta tipo GET, no ha dby datos para los parametros URL ...')
            data_response = utils.create_get_response('list', total_results, now_time_str, 0, 0, False,[])  # status code 204
            logger.info('[lambda_handler] La respuesta fue creada.')
            return data_response
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(
            '[lambda_handler] No se pudo obtener el total de resultados. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
        logger.critical('[lambda_handler] Lambda interrumpida.')
        error_message = 'Acceso fallido a los datos solicitados'
        error_response = utils.create_error_response(500, error_message, now_time_str, context)
        return error_response

    try:
        logger.info('[lambda_handler] Validando y/o implementando paginacion ...')
        limite_page, inicio_page, has_more_data = utils.validate_pagination(url_params, total_results, max_total_results)
        logger.info('[lambda_handler] Paginacion correcta.')
    except ValueError as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        error_message = str(e)
        logger.error(
            '[lambda_handler] No se concluyo la paginacion. Fallo en linea: ' + error_line + '. Motivo: ' + error_message)
        logger.critical('[lambda_handler] Lambda interrumpida.')
        error_response = utils.create_error_response(400, error_message, now_time_str, context)
        return error_response
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(
            '[lambda_handler] No se concluyo la paginacion. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
        logger.critical('[lambda_handler] Lambda interrumpida.')
        error_message = 'Paginacion inconclusa'
        error_response = utils.create_error_response(500, error_message, now_time_str,context)
        return error_response

    try:
        logger.info('[lambda_handler] Obteniendo datos de la base de datos en forma de un dataframe Pandas ...')
        data_df = get_data_from_db(data_query, db_connection)
        logger.debug('[lambda_handler] data_df :'+ str(data_df))
        logger.info('[lambda_handler] Datos obtenidos.')
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error(
            '[lambda_handler] No se obtuvo los datos de la base de datos. Fallo en linea: ' + error_line + '. Motivo: ' + str(
                e))
        logger.critical('[lambda_handler] Lambda interrumpida.')
        error_message = 'Acceso fallido a los datos solicitados'
        error_response = utils.create_error_response(500, error_message, now_time_str, context)
        return error_response

    try:
        logger.info('[lambda_handler] Creando respuesta tipo GET ...')
        data_response = utils.create_get_response('list', total_results, now_time_str, limite_page, inicio_page, has_more_data, data_df)
        logger.info('[lambda_handler] La respuesta fue creada.')
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('[lambda_handler] No se pudo transformar el dataframe Pandas. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
        logger.critical('[lambda_handler] Lambda interrumpida.')
        error_message = 'Fallo la transformacion de los datos para la respuesta.'
        error_response = utils.create_error_response(500, error_message, now_time_str, context)
        return error_response

    return data_response


def decode_queries(parameters, query_time):
    """
    Funcion que codifica los queries SQL necesarios para contar la totalidad de resultados (count_query) si se ejecuta
    el query para obtener los datos (data_query) bajo las condiciones estipuladas en los parametros URL (parameters)
    :param parameters: Diccionario con los parametros URL entregados por el event
    :param query_time: String de la Fecha y hora cuando se ejecuto el lambda_handler
    :returns data_query, count_query: queries necesarios para dar respuesta al requerimiento
    """
    params = utils.clean_params(parameters)
    # campos (Obligatorio): columnas
    logger.info('[decode_queries] Obteniendo columnas del query bajo el parametro "campos" ...')
    params_fields_str = params['campos']
    if params_fields_str == '':
        raise SystemError("'campos' vacio")
    params_fields_list = params_fields_str.split(',')
    params.pop('campos')
    columns = 'dates_calendar '
    where_calendars = ''
    for field in params_fields_list:
        where_calendars += str(params_to_columns_dict['fields'][field]) + ' = True AND '
    logger.info('[decode_queries] Columnas obtenidas')
    # limite (Opcional): LIMIT
    if 'limite' in params:
        limit_operator = 'LIMIT ' + utils.sanitize_int('limite', params['limite']) + ' '  # Sanitizar parametros: casting

        params.pop('limite')
    else:
        limit_operator = 'LIMIT ' + str(max_total_results) + ' '
    # inicio (Opcional): OFFSET
    if 'inicio' in params:
        offset_operator = 'OFFSET ' + utils.sanitize_int('inicio', params['inicio'])  # Sanitizar parametros: casting
        params.pop('inicio')
    else:
        offset_operator = 'OFFSET 0'
    # Otros query params
    if not bool(params):
        where_conditions = ''
    else:
        where_conditions = 'WHERE ' + where_calendars
        if bool(params):
            params_list = list(params)
            query_params_dic = params_to_columns_dict['query_params']
            # Casos especiales de query params, ejemplo: parametro entre dos limites
            for param_name in params_list:
                param_value_str = params[param_name]
                column_name, param_to_query = utils.get_sanitized_parameter(param_name, param_value_str, query_params_dic)
                if "','" in param_to_query: # significa que es una lista de condiciones
                    where_conditions += column_name + " IN (" + param_to_query + ")"
                elif "fecha-inicial" in param_name:
                    where_conditions += column_name + " >= " + param_to_query
                elif "fecha-final" in param_name:
                    where_conditions += column_name + " <= " + param_to_query
                else:
                    where_conditions += column_name + " IN " + param_to_query
                if not param_name == params_list[-1]:
                    where_conditions += ' AND '
                else:
                    where_conditions += ' '
    count_query = "SELECT COUNT(*) AS 'total_registers' FROM " + db_table + ' ' + where_conditions
    data_query = "SELECT " + columns + " FROM " + db_table + ' ' + where_conditions + 'ORDER BY dates_calendar ' + limit_operator + offset_operator
    logger.debug('[decode_queries] total_results_query: ' + count_query)
    logger.debug('[decode_queries] data_query: ' + data_query)
    return data_query, count_query


def count_total_results(query, db_connection):
    """
    Consulta a la base de datos la cantidad de resultados obtenidos utilizando las condiciones especificadas en los
    parametros URL, dichas condiciones se encuentran sanitizadas en el string query
    :param query: String query SQL que usa la sentencia COUNT
    :return: Integer con la cantidad de resultados de la bas de datos bajo las condiciones de los parametros URL
    """
    total_results = pd.read_sql(query, db_connection)
    return int(json.loads(total_results.to_json())['total_registers']["0"])


def get_data_from_db(query, db_connection):
    """
    Obtiene los datos de la base de datos conforme al 'query', y los transforma a un tipo de datos, en este caso lista.
    :param query: String del query SQL que trae los datos conforme a los parametros URL
    :return: List de los datos obtenidos con el 'query' SQL
    """
    data_df = pd.read_sql(query, db_connection)
    logger.debug('[get_data_from_db] type(data_df) = ' + str(data_df.dtypes))
    logger.debug('[get_data_from_db] data_df = ' + str(data_df))
    data_list = list(pd.to_datetime(data_df['dates_calendar']).dt.strftime('%Y-%m-%d'))
    return data_list