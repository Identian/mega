"""
===============================================================================

Nombre: glue-otc-validator-fwi-points.py

Tipo: Glue Job

Autora:
    - Lorena Julieth Torres Hernández
    Tecnologia - Precia

Ultima modificacion: 15/09/2023

En este scrip se realiza la validación  de forward internacional y si se encuentra 
fuera del rango establecido enviará un correo al analista de valorción, indicando
los nodos que se encuentran por fuera de los umbrales

Parametros del Glue Job (Job parameters o Input arguments):
    "--DB_SECRET"= <<Nombre del secreto de BD>>
    "--SMTP_SECRET"= <<Nombre del secreto del servidor de correos>>
    "--VALUATION_DATE"= DATE (viene de la maquina de estados sm-*-otc-fwd-inter-points)
    "--INSTRUMENT"= NAME_INSTRUMENT" (viene de la maquina de estados sm-*-otc-fwd-inter-points)
    "--PARAMETER_STORE" = <<Nombre del parametro que contiene el nombre de la lambda que actualiza los estados>>


===============================================================================
"""

import boto3
import json
import logging
import numpy as np
import pandas as pd 
import sqlalchemy as sa
from datetime import timedelta, datetime
import sys
from awsglue.utils import getResolvedOptions
from precia_utils.precia_exceptions import PlataformError
from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError
from common_library_email_report.ReportEmail import ReportEmail
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from precia_utils.precia_aws import get_params


logger = setup_logging(logging.INFO)
date_format = "%Y-%m-%d"

try:
    params_key = [
        "DB_SECRET",
        "SMTP_SECRET",
        "VALUATION_DATE",
        "INSTRUMENT",
        "PARAMETER_STORE",
        "LAST_DATA"
    ]
    params_dict = get_params(params_key)
    secret_smtp = params_dict["SMTP_SECRET"]   
    secret_db = params_dict["DB_SECRET"]
    db_url_dict = get_secret(secret_db)
    db_url_utils = db_url_dict["conn_string_sources"]
    schema_utils = db_url_dict["schema_utils"]
    db_url_precia_utils = db_url_utils + schema_utils
    db_url_pub = db_url_dict["conn_string_publish"]
    schema_publish = db_url_dict["schema_pub_otc"]
    db_url_pub = db_url_pub + schema_publish
    parameter_store_name = params_dict["PARAMETER_STORE"]
    valuation_date = params_dict["VALUATION_DATE"]
    instrument = params_dict["INSTRUMENT"]
    last_data = params_dict["LAST_DATA"]
    
except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc




MAIL_STYLE = """\
            <style type="text/css">
                th, td {
                    border: 1px solid black;
                    border-collapse: collapse;
                    padding: 5px;
                }
                .col_heading {
                    text-align: center;
		            background-color: #9997DC;
		        color: white;
                }
		        .row_heading{
                    text-align: center;
		            background-color: #9997DC;
		            color: white;
                }
                .col_heading.level0 {
                    font-size: 1em;
                }
                .index_name {
                    font-style: italic;
                    color: black;
                    font-weight: normal;
                    background-color: #9997DC;
                    color: white;
	            }
                
            </style>
        """
        


class DBManager:
    """Gestiona la conexion y las transacciones a base de datos"""

    CONN_CLOSE_ATTEMPT_MSG = "Intentando cerrar la conexion a BD"

    def __init__(self, db_url: str) -> None:
        """Gestiona la conexion y las transacciones a base de datos

        Args:
            db_url (str): url necesario para crear la conexion a base de datos

        Raises:
            PlataformError: Cuando falla la creacion del objeto
        """
        try:
            self.db_url = db_url
        except (Exception,) as init_exc:
            error_msg = "Fallo la creacion del objeto DBManager"
            logger.error(create_log_msg(error_msg))
            raise PlataformError() from init_exc

    def create_connection(self) -> sa.engine.Connection:
        """Crea la conexion a base de datos

        Raises:
            PlataformError: Cuando falla la creacion de la conexion a BD

        Returns:
            sa.engine.Connection: Conexion a BD
        """
        try:
            logger.info("Creando el engine de conexion...")
            sql_engine = sa.create_engine(
                self.db_url, connect_args={"connect_timeout": 2}
            )
            logger.info("Engine de conexion creado con exito")
            logger.info("Creando conexion a BD...")
            db_connection = sql_engine.connect()
            logger.info("Conexion a BD creada con exito")
            return db_connection
        except (Exception,) as conn_exc:
            raise_msg = "Fallo la creacion de la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from conn_exc

    def get_params_variation(self, instrument_list):
        """Se realiza consulta de los insumos de acuerdo a los id_precia

        Args:
            precia_ids_list (list): id_precia a tener en cuenta en la consulta 
            select_query (param): Consulta query
            query_params (param): Parametros a agregar en la consulta
            fwd_params_fwd_nodes_last_7_df (pd.DataFrame): dataframe de con la informacion de insumo


        Raises:
            Cuando no puede trae la informacion del insumo
        """
        select_query = (f"""SELECT
            id_product, id_byproduct, instrument, target_db, confidence_level_percent, historical_validation_days, daily_variation_percent
            FROM precia_utils_validator_financial_params WHERE instrument in ('{instrument_list}') and id_byproduct = 'fwd_inter'
            """
        )
        query_params = {
            "instrument": instrument_list,
        }
        db_connection = self.create_connection()
        fwd_params_df = pd.read_sql(
            select_query, db_connection, params=query_params
        )

        db_connection.close()
        return fwd_params_df
    
    
    def get_fwd_inter_points_nodes(self, yesterday_date, valuation_date, instrument):
        """Se realiza consulta de los insumos de acuerdo a los id_precia

        Args:
            precia_ids_list (list): id_precia a tener en cuenta en la consulta 
            select_query (param): Consulta query
            query_params (param): Parametros a agregar en la consulta
            fwd_validation_params (pd.DataFrame): dataframe de con la informacion de insumo


        Raises:
            Cuando no puede trae la informacion del insumo
        """

        select_query = (f"""SELECT
            id_precia, instrument_fwd, days_fwd, mid_fwd, valuation_date            
            FROM pub_otc_forwards_inter_points_nodes WHERE instrument_fwd = '{instrument}' and valuation_date BETWEEN '{yesterday_date}' and '{valuation_date}'
            """
        )
        query_params = {
            "valuation_date": valuation_date,
        }
        db_connection = self.create_connection()
        fwd_validation_params = pd.read_sql(
            select_query, db_connection, params=query_params
        )
        
        db_connection.close()
        return fwd_validation_params
    
    
class validation_forward_nodes():
    def __init__(self, fwd_nodes_last_7_df,fwd_validation_params, valuation_date,instrument) -> None:
        self.fwd_nodes_last_7_df = fwd_nodes_last_7_df
        self.fwd_validation_params = fwd_validation_params
        self.valuation_date = valuation_date 
        self.instrument = instrument
        confidence_interval,confidence_level_percent,daily_variation_percent = self.confidence_interval_test(fwd_nodes_last_7_df,fwd_validation_params)  
        fwd_nodes_variation, confidence_interval = self.create_fwd_nodes_df(fwd_validation_params,fwd_nodes_last_7_df,confidence_interval,confidence_level_percent,daily_variation_percent)
        self.validate_variation(fwd_nodes_variation,confidence_interval,confidence_level_percent,daily_variation_percent,fwd_nodes_last_7_df)
    
    def create_fwd_nodes_df (self, variation_fwd_df, fwd_nodes_df,confidence_interval,confidence_level_percent,daily_variation_percent):
        """Se realiza la creación del dataframe de acuerdo con la información recolectada
        en base de datos 
        Args:
            variation_fwd_df (pd.DataFrame): dataframe con parametros por forward
            fwd_nodes_df (pd.DataFrame): dataframe historico
           
            confidence_level_percent (decimal): valor del porcentaje del intervalo de confianza 
            daily_variation_percent (decimal) valoo del intervalo de variación diaria 


        Raises:
            Cuando no puede realiar la creación del dataframe 
        """        
        try:
            
            fwd_nodes_df.rename(columns={'instrument_fwd': 'instrument'}, inplace=True)
            fwd_nodes_df = pd.merge(
                    variation_fwd_df, fwd_nodes_df, on=["instrument"], how="right"
                )
            fwd_nodes_df = fwd_nodes_df[['valuation_date','id_precia','confidence_level_percent','daily_variation_percent','days_fwd','mid_fwd']]

            fwd_nodes_variation,confidence_interval = self.modify_fwd_nodes(fwd_nodes_df,confidence_interval,confidence_level_percent,daily_variation_percent)
            
            return fwd_nodes_variation,confidence_interval
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro crear el dataframe con los puntos forward"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc  
    
    
    def modify_fwd_nodes (self, fwd_nodes_df,confidence_interval,confidence_level_percent,daily_variation_percent,):
        try:
            logger.debug(confidence_interval)
            variacion_mid = 'Variacion Mid'
            fwd_nodes_df['valuation_date'] = pd.to_datetime(fwd_nodes_df['valuation_date'])
            fwd_nodes_df  = fwd_nodes_df.sort_values(by='valuation_date', ascending=True)
            dates = fwd_nodes_df["valuation_date"].unique()
            logger.debug(dates)
            yesterday = dates[-2]
            today = dates[-1]
            logger.debug(confidence_level_percent)
            logger.debug(daily_variation_percent)
            fwd_nodes_df = fwd_nodes_df[(fwd_nodes_df["valuation_date"]==today)|(fwd_nodes_df["valuation_date"]==yesterday)]
            confidence_interval['pass_test'] = (confidence_interval['mid'+'_'+str(valuation_date)] <= confidence_interval[str(confidence_level_percent)+'%']) | (confidence_interval['mid'+'_'+str(valuation_date)] <=  confidence_interval[str(daily_variation_percent)+'%'])
            confidence_interval['valuation_date'] = valuation_date
            logger.debug(confidence_interval)
            confidence_interval_2 = confidence_interval[confidence_interval["pass_test"]==False]
            logger.debug(confidence_interval_2)
            if confidence_interval_2.empty:
                confidence_interval_2 = confidence_interval[confidence_interval["pass_test"]==True]
                logger.debug(confidence_interval_2)
                logger.info('No se presentaron variaciones')
            else:
                logger.debug(confidence_interval_2)
                confidence_interval_filter = confidence_interval_2['id_precia'].tolist()
                
                fwd_nodes_df = fwd_nodes_df[fwd_nodes_df['id_precia'].isin(confidence_interval_filter)]
                logger.debug(fwd_nodes_df)
                drop_columns = ['confidence_level_percent', 'daily_variation_percent']
                fwd_nodes_df = fwd_nodes_df.drop(drop_columns, axis=1)
                
                logger.debug(fwd_nodes_df.to_string())
                last_date = (fwd_nodes_df['valuation_date'].iloc[0])
                actually_date = (fwd_nodes_df['valuation_date'].iloc[-1])
                last_date_fwd_nodes_df = fwd_nodes_df[fwd_nodes_df["valuation_date"]==last_date]
                actually_date_fwd_nodes_df = fwd_nodes_df[fwd_nodes_df["valuation_date"]==actually_date]
                drop_columns = ['valuation_date']
                last_date_fwd_nodes_df = last_date_fwd_nodes_df.drop(drop_columns, axis=1)
                actually_date_fwd_nodes_df = actually_date_fwd_nodes_df.drop(drop_columns, axis=1)
                fwd_nodes_df = pd.merge(last_date_fwd_nodes_df,actually_date_fwd_nodes_df, on="id_precia", how='left' )
                column = fwd_nodes_df.columns.tolist()
                fwd_nodes_df = fwd_nodes_df.sort_values(by=column[3])
                fwd_nodes_df = fwd_nodes_df.iloc[:,[0,2,3,4]]
                logger.info(fwd_nodes_df)
                fwd_nodes_df[variacion_mid] = (((fwd_nodes_df['mid_fwd_y'] - fwd_nodes_df['mid_fwd_x'])*100)/ fwd_nodes_df['mid_fwd_x'].abs()).round(3)
                logger.info(fwd_nodes_df)
                fwd_nodes_df.columns = fwd_nodes_df.columns.str.replace("_x","_"+str(last_date.date()))
                fwd_nodes_df.columns = fwd_nodes_df.columns.str.replace("_y","_"+str(actually_date.date()))
                fwd_nodes_df = fwd_nodes_df.iloc[:,[0,1,3,2,4]]
            return fwd_nodes_df, confidence_interval_2
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro modificar el dataframe de los puntos forward"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc
        

    def validate_variation(self, fwd_nodes_variation,confidence_interval,confidence_level_percent,daily_variation_percent,fwd_nodes_df_today):
        """Se realiza el  cálculo de la variacion del mid, segun el historico 
        Args:
            confidence_interval (pd.DataFrame): dataframe para validar el intervalo de confianza 
            fwd_nodes_variation (pd.DataFrame): dataframe a validar la variacion
            confidence_level_percent (decimal): valor del porcentaje del intervalo de confianza 
            daily_variation_percent (decimal) valor del intervalo de variación diaria 
            fwd_nodes_df_today (pd.Dataframe) dataframe con la informacion  de hoy 


        Raises:
            Cuando no se logrevalidar la variacion"""
        try:
            logger.debug(confidence_interval)
            
            fwd_nodes_df_today = fwd_nodes_df_today.sort_values(by='days_fwd', ascending=True)
            logger.debug(fwd_nodes_df_today)
            fwd_nodes_df_today = fwd_nodes_df_today.reset_index()
            global exceeded_test
            
            exceeded_test = True
            confidence_interval_test = confidence_interval
            logger.debug(confidence_interval)
            for value in confidence_interval['pass_test']:
                if value is False:
                    exceeded_test = False
            if exceeded_test is False:
                logger.info('Se presentarios variaciones')
                confidence_interval = confidence_interval.iloc[:,[0,1,2,3,4]]
                
                self.create_html_df(fwd_nodes_variation,daily_variation_percent,confidence_interval,confidence_level_percent)
               
                logger.info('Se presentaron variaciones, se envia correo de validacion')
                raise PlataformError()

            else:
                logger.info('Paso el test de validacion')
                
            
            
            return confidence_interval_test, exceeded_test
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro validar la variacion"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc  


    def create_html_df(self,fwd_nodes_variation,variation_percent,confidence_interval,confidence_level_percent):
        """Se realiza la asignación del formato HTML a los dataframe previamente creados
        Args:
            confidence_level_percent (pd.DataFrame): dataframe para validar el intervalo de confianza 
            confidence_interval (pd.dataframe) : dataframe de intervalo de confianza
            variation_percent (decimal): valor del porcentaje del intervalo de confianza 
            fwd_nodes_variation (pd.dataframe) : dataframe a modificar 


        Raises:
            Cuando no se logre modificar el dataframe que se enviara por correo""" 
        try:
            column = fwd_nodes_variation.columns.to_list()

            fwd_nodes_variation[column[4]] = fwd_nodes_variation[column[4]].apply(str) + "%"
            test_result_html_table_str = fwd_nodes_variation.to_html(index=True)
            test_result_html_table_str = test_result_html_table_str.replace(" 00:00:00","")
            test_result_html_table_str_var = confidence_interval.to_html(index=False)
            test_result_html_table_str_var = test_result_html_table_str_var.replace(" 00:00:00","")
            logger.debug(fwd_nodes_variation)
            logger.debug(confidence_interval)
            html_to_mail = self.create_html_body(test_result_html_table_str,variation_percent,test_result_html_table_str_var,confidence_level_percent)
            self.send_validator_mail(html_to_mail)
																		  
															
            return test_result_html_table_str    

        except (Exception,) as fpc_exc:
            raise_msg = "No se logro validar la variacion"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc          


    def create_html_body(self, test_result_html_table_str,variation_percent,test_result_html_table_str_var, confidence_level_percent):
        """
        A partir de los resultados de los dos test realizados crea una respuesta HTML
        para ser enviada por correo
        """

        try:
            logger.info("Iniciando creacion del body para el correo en HTML ...")
            html_to_mail = f"""
            <html>
                <head>
                    {MAIL_STYLE}
                </head>
                <body>
                    <h4>Cordial saludo, </h4>
                    <p>El validador de relevancia financiera de Megatron encontró que para el proceso OTC, producto Forward, 
                    para el día {valuation_date}, supero los umbrales para algunas de las dos validaciones, a continuación, 
                    se presenta un resumen de las pruebas:</p>
                    <br>
                    &nbsp&nbsp&nbsp&nbsp 1. Variación diaria (porcentaje de error {variation_percent}%): 
                    <br>
                    {test_result_html_table_str}
                    <br>
                    <br>
                    2. Intervalo de confianza (nivel de confianza de {confidence_level_percent}%, respecto a {last_data} dias): Incorrecto. 
                    <br>
                    {test_result_html_table_str_var}
    
                    <br>
                    <br>
                    <br>
                    <p>Validador de relevancia financiera <p>
                    <b>Megatron</b>
                    <br>
                    <img src="https://www.precia.co/wp-content/uploads/2018/02/P_logo_naranja.png" width="320"height="100" style="float :right" />
                    <br>
                    PBX: <FONT COLOR="blue"><b>(57-601)6070071</b></FONT>
                    <br>
                    Cra 7 No 71-21 Torre B Of 403 Bogotá
                <p>
            </body>
            </html>
            """
            
            logger.info(
                "Finalizada la creacion del body para el correo en HTML exitosamente."
            )
            return html_to_mail
        except (Exception,) as bet_exc:
            error_msg = "No se concluyo creacion del body para el email"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from bet_exc


    def send_validator_mail(self,html_to_mail):
        """
        Se realiza el envió del correo 
        """
        error_msg = 'No se logro enviar el correo con las variaciones que superaron el umbral'
        try:
            logger.info('Se intenta realizar el envio del correo con la variacion...')
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Megatron: Validador de relevancia financiera, resultados para forward internacional {self.instrument}"
            body = html_to_mail
            email = ReportEmail(msg['Subject'], body)
            key_secret = get_secret(secret_smtp)
            mail_from =  key_secret["mail_from"]
            mail_to = key_secret["mail_to"] 
            data_connection_smtp = {
                "server": key_secret["smtp_server"],
                "port": key_secret["smtp_port"],
                "user": key_secret["smtp_user"],
                "password": key_secret["smtp_password"]}
            email = email.connect_to_smtp(data_connection_smtp)
            msg.attach(MIMEText(html_to_mail, 'html'))
            logger.debug(mail_to)
            mail_to = mail_to.split(',')
            for mailto in mail_to:
                email.sendmail(mail_from, mailto, msg.as_string())
                logger.info('Se realiza el envio del correo con la variacion')
        
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise    
    
    def confidence_interval_test(self,fwd_nodes_df,fwd_validation_params):
        """Se realiza el test del intervalo de confianza
        Args:
            fwd_nodes_df (pd.DataFrame): dataframe historico 
            fwd_validation_params (pd.dataframe) : dataframe parametros 
        Raises:
            Cuando no se logre modificar el dataframe que se enviara por correo"""            
        try:
            fwd_nodes_df  = fwd_nodes_df.sort_values(by='valuation_date', ascending=True)
            current_date = valuation_date

            
            confidence_level_percent = int(fwd_validation_params.at[0,"confidence_level_percent"])
            daily_variation_percent = int(fwd_validation_params.at[0,"daily_variation_percent"])
            confidence_level = confidence_level_percent/100
            daily_variation = daily_variation_percent/100
            logger.debug(confidence_level)
            logger.debug(daily_variation)
            fwd_nodes_df_sample = fwd_nodes_df[fwd_nodes_df["valuation_date"].astype(str)!=(current_date)]
            logger.debug(fwd_nodes_df_sample)
            grouped = fwd_nodes_df_sample.groupby('id_precia')['mid_fwd']
            percentile_a = grouped.apply(lambda x: np.percentile(x, confidence_level * 100))
            percentile_b = grouped.apply(lambda x: np.percentile(x, daily_variation * 100))
            logger.debug(percentile_a)
            logger.debug(percentile_b)
            fwd_nodes_df_sample[f'{int(confidence_level * 100)}%'] = fwd_nodes_df_sample['id_precia'].map(percentile_a)
            fwd_nodes_df_sample[f'{int(daily_variation * 100)}%'] = fwd_nodes_df_sample['id_precia'].map(percentile_b)
            fwd_nodes_df =  fwd_nodes_df[fwd_nodes_df["valuation_date"].astype(str)==current_date]

            fwd_nodes_df = pd.merge(fwd_nodes_df,fwd_nodes_df_sample, on="id_precia", how='left' )
            
            fwd_nodes_df = fwd_nodes_df.drop_duplicates(subset='id_precia')
            
            logger.debug(fwd_nodes_df.to_string())
            fwd_nodes_df = fwd_nodes_df.iloc[:, :4].join(fwd_nodes_df.iloc[:, -2:])
            
            order_columns = [0,2,4,3,5]
            
            fwd_nodes_df = fwd_nodes_df.iloc[:, order_columns]
            logger.debug(fwd_nodes_df)
            fwd_nodes_df  = (fwd_nodes_df.sort_values(by='days_fwd_x', ascending=True)).reset_index(drop=True)
            fwd_nodes_df.columns = fwd_nodes_df.columns.str.replace("days_fwd_x","dias")
            fwd_nodes_df.columns = fwd_nodes_df.columns.str.replace("_x","_"+(current_date))
            fwd_nodes_df.columns = fwd_nodes_df.columns.str.replace("_fwd","")

            

            return fwd_nodes_df, confidence_level_percent,daily_variation_percent
        except (Exception,) as bet_exc:
            error_msg = 'Fallo la ejecucion del test "confidence_interval_test"'
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from bet_exc

def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Args:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
    """
    try:
        lambda_client = boto3.client("lambda")
        lambda_response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        logger.info("lambda_response:\n%s", lambda_response["Payload"].read().decode())
    except (Exception,) as lbd_exc:
        raise_msg = f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from lbd_exc


def get_parameter_from_ssm(parameter_name):
    """
    Obtiene un parámetro de Amazon Systems Manager (SSM) Parameter Store.

    Parámetros:
        parameter_name (str): El nombre del parámetro que se desea obtener.

    Retorna:
        str: El valor del parámetro almacenado en SSM Parameter Store.

    Excepciones:
        ssm_client.exceptions.ParameterNotFound: Si el parámetro no existe en SSM Parameter Store.
        Exception: Si ocurre un error inesperado al obtener el parámetro desde SSM Parameter Store.
    """
    ssm_client = boto3.client('ssm')
    try:
        response = ssm_client.get_parameter(
            Name=parameter_name,
            WithDecryption=True
        )
        logger.info("Se obtuvo el parametro correctamente")
        return response['Parameter']['Value']
    except ssm_client.exceptions.ParameterNotFound:
        logger.error(f"El parámetro '{parameter_name}' no existe en Parameter Store.")
    except Exception as e:
        logger.error(f"Error al obtener el parámetro '{parameter_name}' desde r Parameter Store: {e}")


def update_report_process(status, description, technical_description):
    """
    Estructura el reporte de estado del proceso y luego llama la funcion que invoca la lambda
    
    Parameters:
        status (str): Estado del proceso
        description (str): Descripcion del proceso
        technical_description (str): Descripcion técnica del proceso (llegado el caso haya fallado el proceso)
        
    Returns:
        None
    """
    lambda_name = get_parameter_from_ssm(parameter_store_name)
    report_process = {
          "input_id": "Forward_" + str(instrument),
          "output_id":[instrument],
          "process": "Derivados OTC",
          "product": "fwd_inter",
          "stage": "Validacion",
          "status": "",
          "aws_resource": "glue-p-otc-validator-fwi-points",
          "type": "insumo",
          "description": "",
          "technical_description": "",
          "valuation_date": valuation_date
        }
    report_process["status"]=status
    report_process["description"]=description
    report_process["technical_description"]=technical_description
    launch_lambda(lambda_name, report_process)
    logger.info("Se envia el reporte de estado del proceso")


if __name__ == "__main__":
    try:
        valuation_date_str = datetime.strptime(valuation_date, date_format).date()
        if valuation_date_str.weekday() == 5 or valuation_date_str.weekday() == 6:
            logger.info('La fehca de valoración corresponde a un fin de semana, para este día no se realiza el proceso de validacion')
        else:
            yesterday_date = (datetime.strptime(valuation_date, date_format) - timedelta(days=int(last_data))).date()
            yesterday_date = datetime.strftime(yesterday_date, date_format)
            precia_utils_db_manager = DBManager(db_url_precia_utils)
            pub_otc_db_manager = DBManager(db_url_pub)
            fwd_nodes_last_7_df = pub_otc_db_manager.get_fwd_inter_points_nodes(yesterday_date,valuation_date, instrument)
            logger.debug(fwd_nodes_last_7_df.to_string())
            
            utils_otc_db_manager = DBManager(db_url_precia_utils)
            instrument_list = tuple(fwd_nodes_last_7_df['instrument_fwd'].tolist())
            fwd_validation_params = utils_otc_db_manager.get_params_variation(instrument)
            data = validation_forward_nodes(fwd_nodes_last_7_df,fwd_validation_params,valuation_date,instrument)
            update_report_process("Exitoso", "Proceso Finalizado", "")
    except (Exception,) as init_exc:
        error_msg = "Fallo la ejecución del main, por favor revisar:"
        update_report_process("Fallido", error_msg, str(init_exc))
        raise PlataformError() from init_exc  