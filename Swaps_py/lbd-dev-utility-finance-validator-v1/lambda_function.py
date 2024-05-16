#
# =============================================================
#
# Nombre: lbd-dev-utility-finance-validator-v1.py
# Tipo: Lambda
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 2022-08-05
#
"""
A partir de la informacion validada del event se realiza dos
validaciones, un intervalo de confianza a un periodo al pasado
determinado, y un error absoluto porcentual con los datos del
día anterior, los umbrales limites que encienden las alarmas
son suministrados por medio del evento, cuando dichos umbrales
son superados se envia un correo HTML que informa los resultados
de las validaciones.
"""
#
# Variables de entorno:
# PAGINATION_MAX_LIMIT: '1000'
#
# Requerimientos:
# capa-pandas-requests
#
# =============================================================
#

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import json
import logging
import sys

import pandas as pd

import notification_utils as utils

logger = logging.getLogger()  # Log, ver en CloaudWatch
logger.setLevel(logging.INFO)
response = ''  # Mensaje para el cliente
failed_init = False  # Evita timeouts causados por errores en el codigo de inicializacion
style_class_to_html = [  # Creacion de las clases internas CSS
    {'selector': 'th.col_heading', 'props': 'text-align: center;'},
    {'selector': 'th.col_heading.level0', 'props': 'font-size: 1em;'},
    {'selector': 'td', 'props': 'text-align: center; font-weight: bold;'},
    {'selector': '.index_name', 'props': 'font-style: italic; color: black; font-weight:normal;'}, # Para index_names
    {'selector': 'th:not(.index_name)', 'props': 'background-color: #312AC3; color: white;'}, # Para headers
    {'selector': '.fail_max_test', 'props': 'background-color: #5DE357;'}, # Por fallar test, superar limite max
    {'selector': '.pass_test', 'props': 'background-color: white;'}, # Por test exitoso
    {'selector': '.fail_min_test', 'props': 'background-color: #DC5C52;'}, # Por fallar test, superar limite min
    {'selector': '.no_change_test', 'props': 'background-color: #FEFFBD;'} # Por fallar test, no presenta cambios 
]
max_level = 0 # Para el test confidence_interval_test
min_level = 0 # Para el test confidence_interval_test
columns_to_ignore = ['tenor', 'fecha-valoracion', 'instrument', 'fecha-efectiva']

try:
    logger.info("[INIT] Inicializando funcion ...")
    mail_from = utils.get_environment_variable('MAIL_FROM')
    logger.info("[INIT] Inicializacion finalizada")
except Exception as e:
    logger.error('[INIT] No se completo la inicializacion. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
    response = str(e)
    failed_init = True 


def lambda_handler(event, context):
    """
    Equivalente lambda de la funcion main, recibe la informacion del desencadenante event
    """
    if failed_init:  # Evita timeouts causados por errores en el codigo de inicializacion
        logger.critical('[lambda_handler] Lambda interrumpida. No se completo la inicializacion.')
        global response
        raise Exception(create_error_response(500, response, context))
    
    logger.debug('[lambda_handler] event: ' + str(json.dumps(event)))
    
    try:
        logger.info("[lambda_handler] Iniciando ejecucion del lambda ...")
        processed_event = process_event(event)
        passed_test_1, test_1_table = daily_variation_test(processed_event['daily_variation'])
        passed_test_2, test_2_table = confidence_interval_test(processed_event['confidence_interval'])
        notify_by_email = not(passed_test_1 and passed_test_2)
        if notify_by_email:
            logger.info("[lambda_handler] Las validaciones financieras fueron NO satisfactorias.")
            logger.info("[lambda_handler] Se requiere notificar los resultados.")
            html_body_to_mail = create_html_body(processed_event, passed_test_1, test_1_table, passed_test_2, test_2_table)
            send_mail(html_body_to_mail, processed_event)
            body_to_orchestrator = 'Validaciones NO satisfactorias para ' + processed_event['process'] + ' se notifico a '\
                                   + ', '.join(processed_event['email_recipients'])
        else:
            logger.info("[lambda_handler] Las validaciones financieras fueron satisfactorias.")
            logger.info("[lambda_handler] NO se requiere notificar los resultados.")
            body_to_orchestrator = 'Para ' + processed_event['process'] + 'los resultados de las validaciones fueron satisfactorios.'
        logger.info("[lambda_handler] Ejecucion del lambda finalizada exitosamente.")
        return {
            'statusCode': 200,
            'body': body_to_orchestrator
        }
    except UserError as e:
        logger.critical('[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(create_error_response(400, str(e), context))
    except Exception as  e:
        logger.critical('[lambda_handler] Ejecucion del lambda interrumpida. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(create_error_response(500, str(e), context))


def process_event(event_dict):
    """
    Procesa y valida el event entregado por el desencadenador, retorna variables para el proceso
    """
    error_msg = 'Fallo el procesamiento del event'
    try:
        logger.info("[process_event] Iniciando la validacion y procesamiento del event ...")
        end_point = event_dict['end_point']
        email_recipients = event_dict['email_params']['recipients']
        email_subject = event_dict['email_params']['subject']
        time_period = event_dict['tests']['confidence_interval']['historical_time_period']
        process = event_dict['process_path']
        today_date = event_dict['today_date']
        historical_data_dict = event_dict['data']
        logger.debug('[process_event] historical_data_dict = ' + str(historical_data_dict))
        today_data_list = historical_data_dict.pop(today_date)
        if len(today_data_list) == 0:
            raise ValueError("En la base de datos no se encontro informacion para " + process + " para el " + today_date)
        today_data_df = pd.DataFrame(today_data_list)
        yesterday_data_flag = False
        yesterday_date = ''
        yesterday_data_df = ''
        old_data_list = []
        historical_dates_list = list(historical_data_dict)
        for date in historical_dates_list:
            data_list = historical_data_dict[date]
            logger.debug('[process_event] ' + date + ' : ' + str(bool(data_list)))
            if bool(data_list):
                old_data_list += data_list
                if yesterday_data_flag == False:
                    logger.debug('[process_event] yesterday_data_flag = True')
                    yesterday_data_flag = True
                    yesterday_date = date
                    yesterday_data_df = pd.DataFrame(data_list)
        if yesterday_data_flag == False:
            raise ValueError("El historico no posee datos para compararlos con los del dia " + today_date)
        historical_data_df = pd.DataFrame(old_data_list)
        logger.debug('[process_event] RAW historical_data_df = \n' + historical_data_df.to_string())
        if 'id-precia' in historical_data_df:
                historical_data_df.set_index('id-precia', inplace=True)
                today_data_df.set_index('id-precia', inplace=True)
                yesterday_data_df.set_index('id-precia', inplace=True)
        logger.debug('[process_event] CLEAN historical_data_df = \n' + historical_data_df.to_string())
        historical_data_columns_list = list(historical_data_df)
        for column in historical_data_columns_list:
            if column in columns_to_ignore:
                historical_data_columns_list.remove(column) 
        # Test 1: daily_variation_dict
        daily_variation_dict = dict()
        daily_variation_dict['error_percent'] = event_dict['tests']['daily_variation']['error_percent']
        daily_variation_dict['today'] = {}
        daily_variation_dict['today']['date'] = today_date
        daily_variation_dict['today']['data'] = today_data_df
        daily_variation_dict['yesterday'] = {}
        daily_variation_dict['yesterday']['date'] = yesterday_date
        daily_variation_dict['yesterday']['data'] = yesterday_data_df
        daily_variation_dict['columns'] = historical_data_columns_list
        # Test 2: confidence_interval_dict
        confidence_interval_dict = dict()
        confidence_interval_dict['level'] = event_dict['tests']['confidence_interval']['confidence_level_percent']
        confidence_interval_dict['old_data'] = historical_data_df
        confidence_interval_dict['today_data'] = today_data_df
        confidence_interval_dict['today_date'] = today_date
        confidence_interval_dict['columns'] = historical_data_columns_list
        # Processed event:
        clean_event = dict()
        clean_event['email_recipients'] = email_recipients
        clean_event['email_subject'] = email_subject.format(process=process)
        clean_event['end_point'] = end_point
        clean_event['time_period'] = time_period
        clean_event['process'] = process
        clean_event['today_data_list'] = today_data_list
        clean_event['today_date'] = today_date
        clean_event['confidence_interval'] = confidence_interval_dict
        clean_event['daily_variation'] = daily_variation_dict
        logger.info("[process_event] Finalizo la validacion y procesamiento del event exitosamente")
        return clean_event
    except KeyError as e:
        logger.error('[process_event] El diccionario event no tiene la estructura esperada. Fallo en linea: ' + get_error_line() + '. Falta: ' + str(e))
        raise UserError(error_msg)
    except ValueError as e:
        logger.error('[process_event] El event no tiene los datos esperados. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise UserError(error_msg)
    except Exception as e:
        logger.error('[process_event] Error inesperado al procesar el event. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception(error_msg)


def daily_variation_test(params_dict):
    """
    Test variacion porcentual con los datos del dia anterior
    """
    try:
        logger.info("[daily_variation_test] Iniciando el test 'daily_variation_test' ...")
        historical_columns = params_dict['columns']
        percentual_error_df = percentual_error_between_two_dates(params_dict['today'], params_dict['yesterday'], historical_columns)
        color_df_columns = percentual_error_df.columns
        color_df_index = percentual_error_df.index
        color_style_class_by_cell_df = pd.DataFrame('pass_test ', columns=color_df_columns, index=color_df_index)
        error_percent = int(params_dict['error_percent'])
        for column in historical_columns:
            sub_error_df = percentual_error_df[column]
            if 'variación' in sub_error_df:
                color_style_class_by_cell_df[(column, 'variación')].where(sub_error_df['variación'] >= -error_percent, 'fail_min_test ', inplace=True)
                color_style_class_by_cell_df[(column, 'variación')].where(sub_error_df['variación'] <= error_percent, 'fail_max_test ', inplace=True)
                if percentual_error_df[(column, 'variación')].eq(0).all():
                    color_style_class_by_cell_df[(column, 'variación')] = 'no_change_test ' 
                percentual_error_df[(column, 'variación')] = percentual_error_df[(column, 'variación')].apply(str) + '%'
        logger.debug("[daily_variation_test] color_style_class_by_cell_df = \n" + color_style_class_by_cell_df.to_string(justify='center'))
        percentual_error_pretty_style = percentual_error_df.style.set_table_styles(style_class_to_html, overwrite=False)
        percentual_error_pretty_style = percentual_error_pretty_style.set_td_classes(color_style_class_by_cell_df)
        passed_test = color_style_class_by_cell_df.eq('pass_test ').all().all()
        test_result_html_table_str = percentual_error_pretty_style.to_html()
        logger.debug("[daily_variation_test] test_result_html_table_str = \n" + test_result_html_table_str)
        logger.info("[daily_variation_test] Supero el test satisfactoriamente: " + str(passed_test))
        logger.info("[daily_variation_test] Finalizo el test 'daily_variation_test' exitosamente.")
        return passed_test, test_result_html_table_str
    except Exception as e:
        logger.error(
            '[daily_variation_test] No se concluyo el test "daily_variation_test". Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception('Fallo la ejecucion del test "daily_variation_test"')


def percentual_error_between_two_dates(today_params, yesterday_params, columns_list):
    """
    Calcula el error porcentual de diferencia entre dos dataframes
    pandas que poseen las mismas columnas y filas.
    """
    try:
        logger.info("[percentual_error_between_two_dates] Iniciando el calculo 'percentual_error_between_two_dates' ...")
        t_1_date = yesterday_params['date']
        t_1_df = yesterday_params['data']
        t0_date = today_params['date']
        t0_df = today_params['data']
        percentual_error_df = pd.DataFrame()    
        for column in columns_list:
            try:
                percentual_error_df[column, t_1_date] = t_1_df[column] # t-1 = t_1
                percentual_error_df[column, t0_date] = t0_df[column]
                percentual_error_df[column, 'variación'] = (t0_df[column] - t_1_df[column]) * 100 / t_1_df[column].abs()
                percentual_error_df[column, 'variación'] = percentual_error_df[column, 'variación'].round(3)
            except TypeError:
                continue # Significa que la columna tiene valores no numericos, se ignora y se sigue con la siguiente
        columns = pd.MultiIndex.from_tuples(list(percentual_error_df))
        percentual_error_multi_column_df = pd.DataFrame(percentual_error_df, columns=columns)
        logger.debug("[percentual_error_between_two_dates] percentual_error_df = \n" + percentual_error_multi_column_df.to_string())
        logger.info("[percentual_error_between_two_dates] Finalizo el calculo 'percentual_error_between_two_dates' exitosamente.")
        return percentual_error_multi_column_df
    except Exception as e:
        logger.error(
            '[percentual_error_between_two_dates] No se concluyo el calculo "percentual_error_between_two_dates". Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception('Fallo la ejecucion del calculo "percentual_error_between_two_dates"')
    

def confidence_interval_test(valiable_dict):
    """
    Calcula los resultados de un test de intervalo de confianza con datos de un periodo historico.
    """
    try:
        logger.info("[confidence_interval_test] Iniciando el test 'confidence_interval_test' ...")
        level = int(valiable_dict['level'])
        global max_level
        max_level = level / 100
        max_level_str = str(level) + '%'
        global min_level
        min_level = 1 - max_level
        min_level_str = str(100 - level) + '%'
        data_columns = valiable_dict['columns']
        today_date_str = valiable_dict['today_date']
        historical_df = valiable_dict['old_data']
        percentiles_columns_dict = dict.fromkeys(data_columns, [percentile_min, today_value, percentile_max])
        percentiles_df = historical_df.groupby(historical_df.index).agg(percentiles_columns_dict)
        percentiles_df.dropna(axis=1, inplace=True)
        change_columns_dict = {
            'today_value': today_date_str,
            'percentile_min': min_level_str,
            'percentile_max': max_level_str
        }
        percentiles_df.rename(columns=change_columns_dict, inplace=True)
        logger.debug("[confidence_interval_test] OLD_DATA percentiles_df = \n" + percentiles_df.to_string(justify='center'))
        today_columns_dict = dict.fromkeys(data_columns, [today_value])
        today_data_df = valiable_dict['today_data'].groupby(valiable_dict['today_data'].index).agg(today_columns_dict)
        today_data_df.rename(columns=change_columns_dict, inplace=True)
        logger.debug("[confidence_interval_test] today_data_df = \n" + today_data_df.to_string(justify='center'))
        percentiles_df.update(today_data_df)
        logger.debug("[confidence_interval_test] TODAY_DATA percentiles_df = \n" + percentiles_df.to_string(justify='center'))
        cell_color_df = pd.DataFrame('pass_test ', columns=percentiles_df.columns, index=percentiles_df.index)
        for head_column in data_columns:
            sub_percentiles_df = percentiles_df[head_column]
            if min_level_str in sub_percentiles_df and max_level_str in sub_percentiles_df and today_date_str in sub_percentiles_df:
                cell_color_df[(head_column, today_date_str)].where(sub_percentiles_df[min_level_str] <= sub_percentiles_df[today_date_str], 'fail_min_test ', inplace=True)
                cell_color_df[(head_column, today_date_str)].where(sub_percentiles_df[max_level_str] >= sub_percentiles_df[today_date_str], 'fail_max_test ', inplace=True)
        logger.debug("[confidence_interval_test] cell_color_df = \n" + cell_color_df.to_string(justify='center'))
        percentiles_pretty_style = percentiles_df.style.set_table_styles(style_class_to_html, overwrite=False)
        percentiles_error_pretty_style = percentiles_pretty_style.set_td_classes(cell_color_df)
        passed_test = cell_color_df.eq('pass_test ').all().all()
        test_result_html_table_str = percentiles_pretty_style.to_html()
        logger.debug("[confidence_interval_test] test_result_html_table_str = \n" + test_result_html_table_str)
        logger.info("[confidence_interval_test] Supero el test satisfactoriamente: " + str(passed_test))
        logger.info("[confidence_interval_test] Finalizo el test 'confidence_interval_test' exitosamente.")
        return passed_test, test_result_html_table_str
    except Exception as e:
        logger.error(
            '[confidence_interval_test] No se concluyo el test "confidence_interval_test". Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception('Fallo la ejecucion del test "confidence_interval_test"')
        

def today_value(x):
    """
    Funcion implementada en calculos con columnas de un dataframe pandas que retorna el primer valor
    """
    return x[0]


def percentile_min(x):
    """
    Funcion implementada en calculos con columnas de un dataframe pandas que retorna el percentil de los datos
    ingresados, si los datos no se pueden procesar retorna una columna NA
    """
    try:
        return x.quantile(min_level)
    except TypeError:
        return pd.NA      


def percentile_max(x):
    """
    Funcion implementada en calculos con columnas de un dataframe pandas que retorna el percentil de los datos
    ingresados, si los datos no se pueden procesar retorna una columna NA
    """
    try:
        return x.quantile(max_level)
    except TypeError:
        return pd.NA


def create_html_body(clean_event, passed_1, table_1_str, passed_2, table_2_str):
    """
    A partir de los resultados de los dos test realizados crea una respuesta HTML
    para ser enviada por correo
    """
    try:
        logger.info("[create_html_body] Iniciando creacion del body para el correo en HTML ...")
        process_list = clean_event['process'].split('/')
        process = process_list[1].upper()
        bd_name = process_list[0].upper()
        del process_list[0]
        del process_list[0]
        product = ', '.join(process_list)
        today_date = clean_event['today_date']
        error_percent = clean_event['daily_variation']['error_percent']
        if passed_1:
            result_test_1 = 'Correcto'
        else:
            result_test_1 = 'Incorrecto'
        table_1_str = table_1_str.split('</style>')[-1]
        level_percent = clean_event['confidence_interval']['level']
        time_period = clean_event['time_period']
        if passed_2:
            result_test_2 = 'Correcto'
        else:
            result_test_2 = 'Incorrecto'
        table_2_str = table_2_str.split('</style>')[-1]
        end_point = clean_event['end_point']
        today_data_list = clean_event['today_data_list']
        today_raw_data = json.dumps({'data': today_data_list}, indent=4)
        style = """\
            <style type="text/css">
                th, td {
                    border: 1px solid black;
                    border-collapse: collapse;
                    padding: 5px;
                }
                .col_heading {
                    text-align: center;
		            background-color: #312AC3;
		        color: white;
                }
		        .row_heading{
                    text-align: center;
		            background-color: #312AC3;
		            color: white;
                }
                .col_heading.level0 {
                    font-size: 1em;
                }
                td {
                    text-align: center;
                    font-weight: bold;
                }
                .index_name {
                    font-style: italic;
                    color: black;
                    font-weight: normal;
                    background-color: #312AC3;
                    color: white;
	            }
                .fail_max_test {
                    background-color: #DC5C52;
                }
                .pass_test {
                    background-color: white;
                }
                .fail_min_test {
                    background-color: #F9B546;
                }
                .no_change_test {
                    background-color: #FEFFBD;
                }
            </style>
        """
        html = """\
        <html>
            <head>
               {style}
           </head>
           <body>
                <h4>Cordial saludo, </h4>
                <p>El validador de relevancia financiera de Optimus K encontró que para el proceso {process}, producto {product}, 
                almacenado en la base de datos {bd_name}, para el día {today_date}, supero los umbrales para algunas de las dos validaciones, a continuación, 
                se presenta un resumen de las pruebas:</p>
                <br>
                &nbsp&nbsp&nbsp&nbsp 1. Variación diaria (porcentaje de error {error_percent}%): {result_test_1}.
                {table_1}
                <br>
                &nbsp&nbsp&nbsp&nbsp 2. Intervalo de confianza (nivel de confianza de {level_percent}%, respecto a{time_period}): {result_test_2}.
                {table_2}
                <br>
                <br>
                 A pesar de no superar las validaciones se procesó la información y se continuó con el proceso. Para su corrección, se
                 puede en orden jerárquico:
                <br>
                <br>
                1. Recolectar la información con otro el proveedor (si es posible), programando una nueva consulta sin hora de ejecución.
                <br>
                2. Descargar el insumo original, solicitándolo por medio de correo a tecnología; modificarlo y cargarlo por medio de
                autorización de TI vía correo.
                <br>
                3. Modificar la base de datos, solicitando a tecnología realizar un método POST a: {end_point}, con el siguiente body:
                <br>
                <br>
                {today_raw_data}
                <br>
                <br>
                <br>
                <p>Validador de relevancia financiera <p>
                <b>Optimus - K</b>
                <img src="https://www.precia.co/wp-content/uploads/2018/02/P_logo_naranja.png" width="320"height="100" style="float :right" />
                <br>
                PBX: <FONT COLOR="blue"><b>(57-601)6070071</b></FONT>
                <br>
                Cra 7 No 71-21 Torre B Of 403 Bogotá
              <p>
           </body>
        </html>
        """
        html_to_mail = html.format(product=product, today_date=today_date, error_percent=error_percent, result_test_1=result_test_1,\
                                   table_1=table_1_str, level_percent=level_percent, time_period=time_period, result_test_2=result_test_2,\
                                   table_2=table_2_str, end_point=end_point, today_raw_data=today_raw_data, process=process, bd_name=bd_name,\
                                   style=style)
        logger.debug("[create_html_body] html_to_mail = \n" + html_to_mail)
        logger.info("[create_html_body] Finalizada la creacion del body para el correo en HTML exitosamente.")
        return html_to_mail
    except Exception as e:
        logger.error(
            '[create_html_body] No se concluyo creacion del body para el email. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception('Fallo la creacion del body para el email')


def send_mail(html_body_to_mail, processed_event):
    """
    Envia correos con cuerpo HTML con las respuestas de los tests
    """
    try:
        logger.info('[send_mail] Creando objeto "MIMEMultipart()" para ser enviado ...')
        message = MIMEMultipart()
        message['Subject'] = processed_event['email_subject']
        message['From'] = mail_from # mail_from: variable global
        message['To'] = (', ').join(processed_event['email_recipients'])
        message.attach(MIMEText(html_body_to_mail, "html"))
        logger.info('[create_mail] Objeto "MIMEMultipart()" creado exitosamente.')
        logger.info("[send_mail] Conectandose al servicio de mensajeria ...")
        smtp_connection = utils.connect_to_smtp()
        logger.info("[send_mail] Conexion exitosa.")
        logger.info("[send_mail] Enviando correo por el SMTP ...")
        smtp_connection.send_message(message)
        logger.info("[send_mail] Envio exitoso por el SMTP.")
    except Exception as e:
        logger.error(
            '[send_mail] No se concluyo el envio del correo. Fallo en linea: ' + get_error_line() + '. Motivo: ' + str(e))
        raise Exception('Fallo envio el correo de notificacin de los resultados')


class UserError(Exception):
    """
    Clase heredada de Exception que permite etiquetar los errores causados por la informacion suministrada por el event
    """
    def __init__(self, error_message='Los datos suministrados por el event no tienen la estructura y/o valores esperados.'):
        self.error_message = error_message
        super().__init__(self.error_message)
    def __str__(self):
        return self.error_message
        
    
def get_error_line():
    """
    Obtiene el numero de linea de la ultima exception generada
    """
    return str(sys.exc_info()[-1].tb_lineno)
    

def create_error_response(status_code, error_message, context):
    """
    Crea la respuesta del API en caso de error, cumple el HTTP protocol version 1.1 Server Response Codes. Entre los
    valores que el diccionario que retorna se encuentra 'log_group', 'error_message' y 'request_id' que permite buscar
    el log en CloudWatch AWS
    :param status_code: Integer de codigo de respuesta del servidor 4XX o 5XX conforme al HTTP protocol version 1.1
    Server Response Codes
    :param error_type: String del tipo de error relacionado con 'status_code' conforme al HTTP protocol version 1.1
    Server Response Codes
    :param error_message: String con un mensaje en espaniol para que el usuario del API
    :param query_time: String de la fecha y hora que se ejecuto la solicitud
    :param context: Contexto del Lambda AWS
    :return: Diccionario con la respuesta lista para retornar al servicio API Gateway AWS
    """
    status_code_dict = {
        400: 'Bad Request',
        401: 'Unauthorized',
        403: 'Forbidden',
        404: 'Not Found',
        405: 'Method Not Allowed',
        500: 'Internal Server Error',
        501: 'Not Implemented',
        503: 'Service Unavailable',
        511: 'Network Authentication Required',
    }
    try:
        logger.debug('[precia_utils.create_error_response] Creando respuesta: error ...')
        error_response = {'statusCode': status_code}
        body = {'error_type': status_code_dict[status_code],
                'error_message': error_message}
        stack_trace = {'log_group': str(context.log_group_name),
                       'log_stream': str(context.log_stream_name),
                       'request_id': str(context.aws_request_id)}
        body['stack_trace'] = stack_trace
        error_response['body'] = body
        logger.debug('[precia_utils.create_error_response] Respuesta creada.')
        return error_response
    except Exception as e:
        error_line = str(sys.exc_info()[-1].tb_lineno)
        logger.error('[precia_utils.create_error_response] No se pudo crear la respuesta: error. Fallo en linea: ' + error_line + '. Motivo: ' + str(e))
