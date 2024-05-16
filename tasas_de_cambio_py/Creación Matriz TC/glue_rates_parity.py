"""
=============================================================

Nombre: glue-dev-rates-parity.py
Tipo: Glue Job

Autores:
    - Lorena Julieth Torres Hernández
Tecnología - Precia

Ultima modificación: 04/12/2023

En el presente script se realiza el calculo de la paridad de 
las monedas para crear la matriz de tasas de cambio. 
Asi mismo se realiza el envio del reporte de variaciones y 
actualización de bases de datos 
=============================================================
"""


import sqlalchemy as sa
import logging
import pandas as pd
import json
import io
import os
import boto3
		   
from datetime import datetime, timedelta
from common_library_email_report.ReportEmail import ReportEmail
from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError
																						   
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from rates_exchange_report.rates_report import ExchangeRateVariations
from precia_utils import precia_sftp

decimal_format = '{:,.5f}'																 
																			 
date_format = "%Y-%m-%d"
logger = setup_logging(logging.INFO)
parameter_store_name = "/ps-otc-lambda-reports"

try:
    params_key = [
        "DB_SECRET",
        "LIMITE_VARIACION",
        "SMTP_SECRET",
        "CURRENCIES",
        "VALUATION_DATE",
        "S3_BUCKET",
        "S3_FILE_PATH_PUBLISH",
        "S3_FILE_PATH_REPORT",
        "DATANFS_SECRET",
        "LAMBDA_PROCESS"
    ]
    params_dict = get_params(params_key)  
    secret_db = params_dict["DB_SECRET"]
    db_url_dict = get_secret(secret_db)
    db_url = db_url_dict["conn_string"]
    schema_src = db_url_dict["schema_src_rates"]
    db_url_src = db_url + schema_src
    schema_pub = db_url_dict["schema_pub_rates"]
    db_url_pub = db_url + schema_pub
    valuation_date = params_dict["VALUATION_DATE"]
    valuation_date_list = list(valuation_date.split(" "))
    secret_smtp = params_dict["SMTP_SECRET"]   
    db_url_utils = db_url_dict["conn_string_aurora"]
    schema_utils = db_url_dict["schema_utils"]
    db_url_precia_utils = db_url_utils + schema_utils
    valuation_date_str = params_dict["VALUATION_DATE"]
    list_currencies = params_dict["CURRENCIES"]
    bucket_name = params_dict["S3_BUCKET"]
    s3_file_path = params_dict["S3_FILE_PATH_PUBLISH"]
    secret_sftp = params_dict["DATANFS_SECRET"]
    lambda_process = params_dict["LAMBDA_PROCESS"]
    key_secret_sftp = get_secret(secret_sftp)
    data_connection_sftp = {
        "host": key_secret_sftp["host"],
        "port":key_secret_sftp["port"],
        "username":key_secret_sftp["user"],
        "password":key_secret_sftp["password"]
    }
    s3_dnfs_path = key_secret_sftp["matriz_path"]
    # client= precia_sftp.connect_to_sftp(data_connection_sftp)
    # sftp_connect = client.open_sftp()
    
except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc

						  
class actions_db():
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
            update_report_process("Fallido", raise_msg, str(conn_exc))
            raise PlataformError() from conn_exc
        
        
    def disable_previous_info(self, info_df:pd.DataFrame,valuation_date) :
        """
        Funcion encargada de deshabilitar la informacion anterior en la
        tabla, en caso de que se vuelva a sobreescribir información
        """
        error_msg = "No se pudo realizar la deshabilidacion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            conn = db_connection.connect()
            logger.info("Conexion exitosa.")
            try:
                id_precia_list = tuple(info_df['id_precia'].values.tolist())
                if len(id_precia_list) == 1:
                    id_precia_list = id_precia_list[0]
                    logger.debug(id_precia_list)
                    status_info = 0
                    update_query = ("UPDATE pub_exchange_rate_parity SET status_info= {} WHERE id_supplier = 'Parity' and valuation_date = '{}' and id_precia = '{}'".format(status_info, valuation_date,id_precia_list))
                else:     
                    status_info = 0
                    update_query = ("UPDATE pub_exchange_rate_parity SET status_info= {} WHERE id_supplier = 'Parity' and valuation_date = '{}' and id_precia in {}".format(status_info, valuation_date,id_precia_list))
                
                conn.execute(update_query)
                logger.debug(update_query)
                
            except sa.exc.SQLAlchemyError as sql_exc:
                logger.error(create_log_msg(error_msg))
                raise_msg = (
                    "La base de datos no acepto los datos. validar tipos de datos")
                update_report_process("Fallido", error_msg, str(sql_exc))
                raise PlataformError(raise_msg) from sql_exc
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            if db_connection != None:
                db_connection.close()
            raise PlataformError() from fpc_exc


    def insert_data(self, info_df:pd.DataFrame):
        """
        Se realiza la insercion de la información consultada
        """
        error_msg = "No se pudo realizar la insercion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            logger.info("Conexion exitosa.")
            try:
                info_df.to_sql('pub_exchange_rate_parity', con=db_connection, if_exists="append", index=False)
                logger.info(actions_db.CONN_CLOSE_ATTEMPT_MSG)
                db_connection.close()
                logger.info("Conexion a BD cerrada con exito")
                logger.info("Insercion de dataframe en BD exitosa")
                
            except (Exception,):
                logger.error(create_log_msg(error_msg))
                raise
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            if db_connection != None:
                db_connection.close()
            raise PlataformError() from fpc_exc
        
        
    def find_rates_data_today(self,valuation_date):
        """
        Se realiza la busqueda de la información de los ultimos 7 días
        """
        try:
            db_connection = self.create_connection()
            select_query = sa.sql.text(
                f"""SELECT id_precia,value_rates, valuation_date, effective_date  FROM src_exchange_rates WHERE valuation_date = '{valuation_date}' AND status_info =1 and id_supplier not like 'Parity'""")
            data_today = pd.read_sql(select_query, db_connection)
            data_today = data_today.sort_values(by=['effective_date'])
            logger.debug(data_today.to_string())
            db_connection.close()
            return data_today
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  de la información de los ultimos 7 días segun la fecha de hoy"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc
            
            
    def get_rates_today(self,valuation_date):
        """
        Obtiene las tasas calculadas del día actual
        """
        try:
            db_connection = self.create_connection()
            select_query = sa.sql.text(
                f"""SELECT id_precia, value_rates, valuation_date 
                FROM pub_exchange_rate_parity 
                WHERE (id_precia LIKE 'USD%' OR id_precia LIKE '%USD')
                AND status_info = :status_info
                AND id_supplier = :id_supplier
                AND valuation_date = :valuation_date""")
            query_params = {
                "status_info": 1,
                "id_supplier": 'Parity',
                "valuation_date": valuation_date
            }
            
            rates_today = pd.read_sql(select_query, db_connection, params=query_params)
            db_connection.close()
            return rates_today
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  de la información de los ultimos 7 días segun la fecha de hoy"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc
   
   
    def find_rates_data(self,valuation_date,val_date):
        """
        Se realiza la busqueda de la información de los ultimos 7 días
        """
        try:
            db_connection = self.create_connection()
            select_query = sa.sql.text(
                f"""SELECT t1.id_precia,t1.value_rates, t1.valuation_date, t1.effective_date FROM src_exchange_rates t1
                INNER JOIN (
	                SELECT id_precia, MAX(valuation_date) AS max_valuation_date
	                FROM src_exchange_rates 
                    WHERE status_info = 1 AND valuation_date <= '{valuation_date}'  AND valuation_date >= '{val_date}'
                    GROUP BY id_precia
                    ) t2
                ON t1.valuation_date = t2.max_valuation_date AND t1.id_precia = t2.id_precia 
                WHERE t1.status_info = 1""" )
            historic_data = pd.read_sql(select_query, db_connection)
            historic_data = historic_data.sort_values(by=['effective_date'])
            logger.debug(historic_data.to_string())
            db_connection.close()
            return historic_data
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  de la información de los ultimos 7 días"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc
        
        
    def find_rates_data_uvr(self,valuation_date):
        """
        Se realiza la busqueda de la información de los ultimos 7 días
        """
        try:
            if len(valuation_date) == 1:
                valuation_date = valuation_date[0]
                valuation_date = "('"+valuation_date+"')"
            else:
                valuation_date = (tuple(valuation_date))
            db_connection = self.create_connection()
            select_query = sa.sql.text(
            f"""SELECT id_precia,value_rates, valuation_date, effective_date  FROM src_exchange_rates WHERE valuation_date in {valuation_date} AND status_info =1 and id_precia = 'UVRCOP'""")
            historic_data = pd.read_sql(select_query, db_connection)
            historic_data = historic_data.sort_values(by=['effective_date'])
            logger.debug(historic_data.to_string())
            db_connection.close()
            return historic_data
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  de la información de los ultimos 7 días"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc     


    def find_rate_parity(self,valuation_date):
        """
        Data de paridad de tasas 
        """
        try:
            db_connection = self.create_connection()
            select_query = sa.sql.text(
            f"""SELECT id_precia, valuation_date, effective_date,value_rates  FROM pub_exchange_rate_parity WHERE valuation_date = '{valuation_date}' AND status_info =1 """)
            rate_parity_information = pd.read_sql(select_query, db_connection)
            logger.debug(rate_parity_information.to_string())
            db_connection.close()
            return rate_parity_information
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  de la información de la paridad de tasas"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc
        
    def find_business_day(self,valuation_date):
        """
        Se realiza la consulta de los dias hábiles segun la fecha actual
        """
        try:
            if isinstance(valuation_date,str):
                valuation_date = datetime.strptime(valuation_date,date_format)
            
            db_connection = self.create_connection()
            next_day = valuation_date + timedelta(days=1)
            select_query = sa.sql.text(
            f"""SELECT dates_calendar, bvc_calendar, Target_calendar, Chilean_calendar, Russian_calendar, Peruvian_calendar FROM precia_utils_calendars WHERE dates_calendar BETWEEN '{valuation_date}' AND 
            (SELECT MIN(dates_calendar) FROM precia_utils_calendars WHERE dates_calendar >= '{next_day}' AND bvc_calendar =1)""")
            business_days_df = pd.read_sql(select_query, db_connection)
            last_bussines_day_query = sa.sql.text(
            f"""SELECT dates_calendar, bvc_calendar, Target_calendar, Chilean_calendar, Russian_calendar, Peruvian_calendar FROM precia_utils_calendars WHERE bvc_calendar = 1 AND dates_calendar < '{valuation_date}'
            ORDER BY dates_calendar DESC LIMIT 1""")
            last_bussines_day_df = pd.read_sql(last_bussines_day_query, db_connection)
            business_days_df = business_days_df.iloc[:-1]
            dates = business_days_df['dates_calendar'].tolist()
            datesstr = [date.strftime("%Y-%m-%d") for date in dates]
            
            db_connection.close()
            return business_days_df, last_bussines_day_df,datesstr
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  a la BD"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc         
        
        
class exchange_rates():
    def calculate_eur_rates(self,df_exchange_rates:pd.DataFrame,valuation_date):
        """
        Función encargada de calcular el valor  de USDEUR y la paridad de las tasas del Banco central Europeo
        """
        try:
            
            logger.info('Se intenta calcular las tasas para EUR...')
            df_usd_ecb_rates = df_exchange_rates[df_exchange_rates['id_precia'].str.startswith('EUR')]
            df_usd_ecb_rates.set_index("id_precia", inplace = True)
            eurusd_value = df_usd_ecb_rates.loc['EURUSD']['value_rates']
            df_usd_ecb_rates = df_usd_ecb_rates.reset_index()
            df_usd_ecb_rates.drop(df_usd_ecb_rates[(df_usd_ecb_rates["id_precia"]=='EURUSD')].index,inplace=True,)
            df_usd_ecb_rates['id_precia'] = df_usd_ecb_rates['id_precia'].replace({'EUR': 'USD'}, regex=True)
            usdeur_value = 1/eurusd_value
            data_usdeur = ({'id_precia': ['USDEUR'],'value_rates': [usdeur_value], 'valuation_date':valuation_date})
            df_usdeur = pd.DataFrame(data_usdeur)
            df_usd_ecb_rates['value_rates'] = (df_usd_ecb_rates['value_rates']/eurusd_value)
            df_exchange_rates = pd.concat([df_usdeur,df_usd_ecb_rates])
            df_exchange_rates = df_exchange_rates[['id_precia','value_rates','valuation_date','effective_date']]
            logger.debug(df_exchange_rates)
            logger.info('Se calculo las tasas para EUR')
            return df_exchange_rates, eurusd_value
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro calcular el valor de las tasas EUR"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc
        
        
    def calculate_usdudi(self,df_exchange_rates:pd.DataFrame,eurusd_value:float,valuation_date):
	  
        """
        Función encargada de calcular el valor de USDUDI 
        """
        try:
            logger.info('Se intenta calcular las tasas para USDUDI...')
            df_mxn_rates = df_exchange_rates[df_exchange_rates['id_precia'].str.endswith('MXN')]
            logger.debug(df_mxn_rates)
            df_mxn_rates.set_index("id_precia", inplace = True)
            udimxn_value = df_mxn_rates.loc['UDIMXN']['value_rates']
            eurmxn_value = df_mxn_rates.loc['EURMXN']['value_rates']
            df_mxn_rates = df_mxn_rates.reset_index()
            usdmxn_value = eurmxn_value/eurusd_value
            usdudi_value = usdmxn_value/udimxn_value
            df_mxn_rates = df_mxn_rates.reset_index()
            data_udimxn = ({'id_precia': ['USDUDI'],'value_rates': [usdudi_value], 'valuation_date': valuation_date})
            df_mxn_rates = pd.DataFrame(data_udimxn)
            logger.debug(df_mxn_rates)
            logger.info('Se calculo las tasas para USDUDI')
            return df_mxn_rates
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro calcular el valor de las tasas USDUDI"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc


    def calculate_usdclf(self, df_clf_rates: pd.DataFrame, valuation_date):
        """
        Función encargada de calcular el valor de USDCLF
        """
        try:
            df_clf_rates = df_clf_rates[(df_clf_rates['id_precia'].str.contains('CLP', case=False)) | (df_clf_rates['id_precia'].str.contains('CLF', case=False))]
            logger.info("Se intenta recalcular el valor de la tasa USDCLF...")
            df_clf_rates.set_index("id_precia", inplace=True)
            clfclp_value = df_clf_rates.loc["CLFCLP"]["value_rates"]
            usdclp_value = df_clf_rates.loc["USDCLP"]["value_rates"]
            usdclf_value = usdclp_value/clfclp_value
            
            data_usdclf = {
                "id_precia": ["USDCLF"],
                "value_rates": [usdclf_value],
                "valuation_date": valuation_date,
            }
            df_clf_rates = pd.DataFrame(data_usdclf)
            
            
            logger.info("Se calculo las tasas para USDCLF")
            return df_clf_rates
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro calcular el valor de las tasas USDUDI"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc
        
        
    def calculate_usduvr(self,df_exchange_rates:pd.DataFrame,valuation_date):
        
        """
        Funcion encargada de calcular el  valor de  USDUVR 
        """
        try:
            logger.info('Se intenta calcular las tasas para USDUVR...')
            df_cop_rates = df_exchange_rates[df_exchange_rates['id_precia'].str.endswith('COP')]
            logger.debug(df_cop_rates)
            df_cop_rates = df_cop_rates.reset_index()
            df_cop_rates.set_index("id_precia", inplace = True)
            usdcop_value = df_cop_rates.loc['USDCOP']['value_rates']
            uvrcop_value = df_cop_rates.loc['UVRCOP']['value_rates']
            df_cop_rates = df_cop_rates.reset_index()
            usduvr_value = usdcop_value/uvrcop_value
            data_usdvr = [{"id_precia": "USDUVR", "value_rates": usduvr_value, "valuation_date": valuation_date},
                          {"id_precia": "USDCOP", "value_rates": usdcop_value, "valuation_date": valuation_date}]
            df_usdvr_rates = pd.DataFrame(data_usdvr)
            logger.debug(df_usdvr_rates)
            return df_usdvr_rates
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro calcular el valor de las tasas USDUVR"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc    
        
        
    def calculate_central_bank_rates(self,df_exchange_rates:pd.DataFrame):
        """
        Funcion encargada de traer la informacion de los bancos centrales y adjuntar en el dataframe para 
        la paridad  
        """
        try:
            logger.info('Se intenta consultar el valor de las tasas de cambio de los bancos centrales')
            df_usd_central_bank = df_exchange_rates[df_exchange_rates['id_precia'].str.startswith('USD')]
            df_usd_central_bank.set_index("id_precia", inplace = True)
            df_usd_central_bank = df_usd_central_bank.reset_index()
            df_usd_central_bank.drop(df_usd_central_bank[(df_usd_central_bank["id_precia"]=='USDCOP')].index,inplace=True,)
            df_usd_central_bank = df_usd_central_bank[['id_precia','value_rates','valuation_date','effective_date']]
            logger.info('Se calculo las tasas para bancos centrales')
            return df_usd_central_bank
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro calcular el valor de las tasas de los bancos centrales"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc               

    
    def calculate_all_rates(self,data_exchange_rates:pd.DataFrame):
        """
        Funcion encargada de realizar la paridad de tasas de acuerdo a la informacion recolectada 
        de los vendors y bancos centrales 
        """
        
        try:
            results = []
            id_precia = []
            data_usd_rates = data_exchange_rates[["id_precia", "value_rates"]]
            for index1, row1 in data_exchange_rates.iterrows():
                #logger.debug(index1)
                for index2, row2 in data_exchange_rates.iterrows():
                    #logger.debug(index2)
                    results.append(row1["value_rates"] / row2["value_rates"])
                    id_precia.append(row2["id_precia"] + str(row1["id_precia"]))
            df_exchange_rates = pd.DataFrame()
            df_exchange_rates["id_precia"] = id_precia
            df_exchange_rates["id_precia"] = df_exchange_rates["id_precia"].replace(
                {"USD": ""}, regex=True
            )
            df_exchange_rates["value_rates"] = results
																			   
            df_exchange_rates = pd.concat([df_exchange_rates, data_usd_rates])
            results = []
            id_precia = []
            for index, row1 in data_exchange_rates.iterrows():
                #logger.debug(index)
                for index2, row2 in data_exchange_rates.iterrows():
                    #logger.debug(index2)
                    id_precia.append(row1["id_precia"][3:] + row1["id_precia"][:3])
                    results.append(round(1 / row1["value_rates"], 5))
            df_usd_rates = pd.DataFrame()
            df_usd_rates["id_precia"] = id_precia
            df_usd_rates["value_rates"] = results
            df_exchange_rates = pd.concat([df_usd_rates, df_exchange_rates])
            df_exchange_rates['id_supplier'] = 'Parity'
            #logger.info(df_exchange_rates.dtypes)
            logger.info(df_exchange_rates)
            df_exchange_rates = df_exchange_rates.drop_duplicates()
            return df_exchange_rates
                
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro calcular el valor de todas las tasas"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc     
        

    def remove_similar_pairs(self, df_all_usd_rates:pd.DataFrame):    
        """
        Función encargada de eliminas las tasas como COPCOP, EUREUR, USDUSD... 
        """
        try:
        
            df_all_usd_rates['rate_1'] = df_all_usd_rates['id_precia'].str[:3]
            df_all_usd_rates['rate_2'] = df_all_usd_rates['id_precia'].str[3:6]
            df_all_usd_rates.loc[df_all_usd_rates["rate_1"] == df_all_usd_rates["rate_2"], "rates"] = 'igual'
            df_all_usd_rates['rates'] = df_all_usd_rates['rates'].fillna('diferente')
            df_all_usd_rates = df_all_usd_rates[(df_all_usd_rates["rates"] == 'diferente')]
            df_all_usd_rates = df_all_usd_rates.drop(
                [
                    "rates",
                    "rate_2",
                    "rate_1",
                ],
                axis=1,
            )
            return  df_all_usd_rates
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro eliminar las tasas que se cruzaron entre ellas mismas"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc   
            
    def trigger_rates(self, lambda_name: str, payload: list):
    
        """Lanza una ejecucion de lambda indicada
        Args:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
        """
        Payload=json.dumps(payload)
        logger.info(Payload)
        logger.info(lambda_name)
        lambda_client = boto3.client("lambda") 
        lambda_response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),)
        logger.info("lambda_response:\n%s", lambda_response["Payload"].read().decode())

class generate_matrix_tc:
    def select_parity_currencies(self, df_exchange_rates: pd.DataFrame):
        """
        De acuerdo a la informacion que encontro en paridad se va a extraer las monedas que alli se encuentran
        """
        try:
            parity_currencies = df_exchange_rates["id_precia"].str[:-3]
            parity_currencies = list(set(parity_currencies))
            return parity_currencies
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro extraer las monedas de la informacion de paridad"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def compare_currencies(self, list_parity: list, df_exchange_rates: pd.DataFrame):
        """
        Se comapara las monedas que se encuentran en paridad con las necesarias para generar la matriz y solo utilizar estas
        """
        try:
            difference = []
            for i in list_parity:
                if i not in list_currencies:
                    difference.append(i)
            logger.debug(difference)
            data_delete = df_exchange_rates["id_precia"].str.contains(
                "|".join(difference)
            )
            df_currencies_tc = df_exchange_rates.loc[~data_delete]
            return df_currencies_tc

        except (Exception,) as fpc_exc:
            raise_msg = "No se logro validar si existen las monedas en paridad"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def validate_full_currencies(self, df_currencies_tc: pd.DataFrame, val_date):
        """
        Se valida que las monedas para realizar la creacion de la matriz esten completas
        """
        try:
            df_currencies_tc = df_currencies_tc[
                df_currencies_tc["valuation_date"] == val_date
            ]
            curencies = df_currencies_tc["id_precia"].str[:-3].values.tolist()
            curencies = list(set(curencies))
            list_currencies_tc = list(list_currencies.split(", "))
            if sorted(curencies) == sorted(list_currencies_tc):
                all_currencies = "SI"
            else:
                all_currencies = "NO"

            difference = list(set(list_currencies_tc) - set(curencies))

            return all_currencies, difference
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro validar si las monedas estan completas para la creacion de la matriz"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def create_df_rate_vs_same_rate(
        self, currencies_to_tc: pd.DataFrame, valuation_date
    ):
        """
        Se crea un nuevo dataframe con las monedas que se cruzan con ellas mismas ej EUREUR, USDUSD
        """
        try:
            df_rate_vs_same_rate = currencies_to_tc.copy()
            df_rate_vs_same_rate["id_precia"] = df_rate_vs_same_rate["id_precia"].str[
                :3
            ]
            df_rate_vs_same_rate["id_precia"] = (
                df_rate_vs_same_rate["id_precia"] + df_rate_vs_same_rate["id_precia"]
            )
            df_rate_vs_same_rate["value_rates"] = 1
            df_rate_vs_same_rate["valuation_date"] = valuation_date
            df_rate_vs_same_rate = df_rate_vs_same_rate.drop_duplicates()

            return df_rate_vs_same_rate
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro crear el dataframe para las tasas que se cruzan con ellas mismas"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def create_df_to_tc_matrix(
        self, df_rate_vs_same_rate: pd.DataFrame, df_currencies_tc: pd.DataFrame
    ):
        """
        Se crea el dataframe informacion de las tasas recolectadas, agregando las monedas que se cruzan con ellas mismas
        """
        try:
            df_to_tc_matrix = pd.concat([df_currencies_tc, df_rate_vs_same_rate])
            df_to_tc_matrix["value_rates"] = df_to_tc_matrix["value_rates"].astype(
                "string"
            )
            df_to_tc_matrix["value_rates"] = df_to_tc_matrix["value_rates"].replace(
                "1.0", "1"
            )
            df_to_tc_matrix["value_rates"] = df_to_tc_matrix["value_rates"].astype(
                "float"
            )
            df_to_tc_matrix["Mid"] = df_to_tc_matrix["id_precia"].str[:3]
            df_to_tc_matrix["id_precia2"] = df_to_tc_matrix["id_precia"].str[3:]

            logger.debug(df_to_tc_matrix)
            return df_to_tc_matrix
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro crear el dataframe con la informacion necesaria para la matriz TC"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def create_matrix(self, df_to_tc_matrix: pd.DataFrame, list_currencies: list):
        """
        Se Crea la matriz con la informacion recolectada
        """
        try:
            matrix = pd.pivot_table(
                df_to_tc_matrix, values="value_rates", index="Mid", columns="id_precia2"
            )
            matrix.columns = [col if col != "Mid" else "" for col in matrix.columns]
            list_currencies = list(list_currencies.split(", "))
            matrix = matrix.reindex(index=list_currencies, columns=list_currencies)
            return matrix
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro realizar la creacion de la matriz"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def create_matrix_file(self, matrix: pd.DataFrame, date_to_file: pd.to_datetime):
        """
        Se crea al archivo de la matriz
        """
        try:
            logger.info("Se intenta crear el archivo matriz...")
            if isinstance(date_to_file, str):
                date_to_file = datetime.strptime(date_to_file, date_format)
            date_to_file = datetime.strftime(date_to_file, "%Y%m%d")
            logger.debug(date_to_file)
            file_name = f"Matriz_TC_{date_to_file}.txt"
            decimal = (
                lambda x: decimal_format.format(x).rstrip("0").rstrip(".")
                if isinstance(x, float)
                else x
            )
            matrix = matrix.applymap(decimal)
            matrix = matrix.astype("string")
            matrix = matrix.replace(",", "", regex=True)
            matrix.to_csv(file_name, sep=" ", line_terminator='\r\n', index=True)
            self.put_in_s3(s3_file_path,bucket_name, file_name, matrix)
            self.load_files_to_sftp(matrix, s3_dnfs_path, file_name, data_connection_sftp)
            logger.info(f"Se crea el archivo matriz para la fecha {date_to_file}")
            return file_name
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro realizar la creacion del archivo de la matriz"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def put_in_s3(
        self, s3_file_path: str, bucket_name: str, filename: str, matrix: pd.DataFrame
    ):
        """
        Funcion que dispone los datos obtenidos de la API DDS de Refinitiv como
        un archivo .json en el bucket 'bucket_name', ruta 's3_file_path'.
        """
        log_msg = f"No se pudo cargar el archivo {filename} al bucket"
        try:
            logger.info("Cargando archivo %s en bucket S3 ...", filename)
            s3_resourse = boto3.client("s3")

            full_s3_file_path = os.path.join(s3_file_path, filename)

            matrix2 = matrix.to_csv(sep=" ", header=True, line_terminator='\r\n', index=True)
            logger.info("Creando objeto en %s ...", bucket_name)

            logger.info("Cargando bytes en %s ...", full_s3_file_path)
            s3_resourse.put_object(
                Body=matrix2, Bucket=bucket_name, Key=full_s3_file_path
            )
            logger.info("Archivo insumo cargado con exito en el bucket")
        except (Exception,) as put_exc:
            logger.error(create_log_msg(log_msg))
            update_report_process("Fallido", log_msg, str(put_exc))
            raise PlataformError(log_msg) from put_exc

    def validate_effective_date(self, business_days_df):
        """
        Se valida si la fecha consultada es habil o no segun el calendario de colombia
        """
        try:
            effective_date = business_days_df["bvc_calendar"].iloc[0]
            logger.debug(effective_date)
            return effective_date
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro validar la fecha de vigencia"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def validate_weekday(self, business_days_df):
        """
        Se valida a que dia de la semana corresponde la fecha para la creacion de la matriz
        """
        try:
            weekday = business_days_df["dates_calendar"].iloc[0]
            weekday = datetime.strftime(weekday, "%Y-%m-%d")
            date = datetime.strptime(weekday, "%Y-%m-%d")
            weekday = date.strftime("%A")
            logger.debug(weekday)
            return weekday, date
        except (Exception,) as fpc_exc:
            raise_msg = (
                "No se logro validar el dia de la semana para la creación de la matriz"
            )
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def get_last_bussines_day(self, last_bussines_day_df):
        """
        Se busca el dia anterior habil
        """
        try:
            last_bussines_day = last_bussines_day_df["dates_calendar"].iloc[0]
            logger.debug(last_bussines_day)
            return last_bussines_day
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro encontrar el ultimo día hábil"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def get_uvr_data(self, df_exchange_rates):
        """
        Se busca la información para uvr que se debe actualizar cuando no es día hábil en Colombia
        """

        try:
            df_uvr = df_exchange_rates[
                df_exchange_rates["id_precia"].str.contains("UVR")
            ]
            logger.debug(df_uvr.to_string())
            return df_uvr
        except (Exception,) as fpc_exc:
            raise_msg = "No se encontro información para uvr"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def validate_holiday_col(self, holiday_col, bussines_day_valuation, valuation_date):
        """
        Si es festivo en Colombia pero en otro calendario no se cambia la fecha de la creacion de la matriz
        """
        try:
            if bussines_day_valuation != valuation_date:
                date = bussines_day_valuation
                logger.info("Es festivo en Colombia pero en algun otro calendario no")
                holiday_col = "SI"
            else:
                date = valuation_date
            create_matrix = "SI"

            return create_matrix, date, holiday_col
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro cambiar la fecha para la generacion de la matriz en dia festivo Colombia"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def send_error_mail(self, missing_currencies):
        try:
            logger.info('Se intenta realizar el envio del correo...')
            logger.debug(missing_currencies)
            missing_currencies_list = ", ".join(missing_currencies)
            logger.debug(len(missing_currencies))
            if len(missing_currencies) == 1:
                insumo = 'falto el insumo'
            else:
                insumo = 'faltaron los insumos'
						   
            msg = MIMEMultipart('alternative')
            msg['Subject'] = "Megatron - Insumos incompletos para la creación de la Matriz TC"
            key_secret = get_secret(secret_smtp)
            mail_from =  key_secret["mail_from"]
            mail_to = key_secret["mail_to"]
            body = f"""<html>
                </head>
                <body>
                    <h4>Cordial saludo, </h4>
                    <p>No se logro realizar la creación de la matriz {insumo}</p>
                    <br>
                    {missing_currencies_list}
                    <br>
                    <br>
                    <br>
                    <b>Megatron</b>
                    <br>
                    <img src="https://www.precia.co/wp-content/uploads/2018/02/P_logo_naranja.png" width="320"height="100" style="float :right" />
                    <br>
                    PBX: <FONT COLOR="blue"><b>(57-601)6070071</b></FONT>
                    <br>
                    Cra 7 No 71-21 Torre B Of 403 Bogotá
                <p>
            </body>
            </html>"""
			
            email = ReportEmail(msg['Subject'], body)
            key_secret = get_secret(secret_smtp)
            data_connection_smtp = {
                "server": key_secret["smtp_server"],
                "port": key_secret["smtp_port"],
                "user": key_secret["smtp_user"],
                "password": key_secret["smtp_password"]}

            email = email.connect_to_smtp(data_connection_smtp)
            msg.attach(MIMEText(body, 'html'))
            email.sendmail(mail_from, mail_to, msg.as_string())
            logger.info('Se logro realizar el envio del correo')
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro enviar el correo de error"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def create_matrix_run(
        self,
        business_days,
        business_days_df,
        effective_date,
        last_bussines_day_df,
        valuation_date,
        df_uvrcop,
        df_exchange_rates,
    ):
        try:
            create_matrix = "SI"
            holiday_col = "NO"
            weekday, date = self.validate_weekday(business_days_df)
            if (business_days == business_days_df.shape[1] - 1).all():
                logger.info("Hoy es dia habil para todos los calendarios")
                create_matrix = "SI"
            elif (business_days == 0).all():
                logger.info("Hoy NO es dia habil para todos los calendarios")
                create_matrix = "SI"
            else:
                if effective_date == 0:
                    bussines_day_valuation = self.get_last_bussines_day(
                        last_bussines_day_df
                    )
                    create_matrix, date, holiday_col = self.validate_holiday_col(
                        holiday_col, bussines_day_valuation, valuation_date
                    )

            if create_matrix == "SI":
                rates_tc = self.select_parity_currencies(df_exchange_rates)
                df_currencies_tc = self.compare_currencies(rates_tc, df_exchange_rates)
                self.create_matrix_all_currencies(
                    df_currencies_tc, date, holiday_col, valuation_date, df_uvrcop
                )
            return df_exchange_rates
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro continuar la ejecucion desde el run"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def create_matrix_all_currencies(
        self, df_currencies_tc, date, holiday_col, valuation_date, df_uvrcop
    ):
        try:
            all_currencies, difference = self.validate_full_currencies(
                df_currencies_tc, valuation_date
            )

            if all_currencies == "SI":
                df_rate_vs_same_rate = self.create_df_rate_vs_same_rate(
                    df_currencies_tc, date
                )
                df_to_tc_matrix = self.create_df_to_tc_matrix(
                    df_rate_vs_same_rate, df_currencies_tc
                )

                if holiday_col == "SI":
                    df_to_tc_matrix.drop(
                        df_to_tc_matrix[
                            (df_to_tc_matrix["id_precia"] == "UVRUVR")
                        ].index,
                        inplace=True,
                    )
                    pub_rates_db_manager = actions_db(db_url_pub)
                    df_exchange_rates_uvr = pub_rates_db_manager.find_rate_parity(
                        valuation_date
                    )
                    df_uvr = self.get_uvr_data(df_exchange_rates_uvr)

                    df_to_tc_matrix = df_to_tc_matrix.drop_duplicates()

                    df_to_tc_matrix.update(df_uvr)
                    df_uvruvr = pd.DataFrame(
                        {
                            "id_precia": ["UVRUVR"],
                            "value_rates": [1.0],
                            "Mid": "UVR",
                            "id_precia2": "UVR",
                            "valuation_date": date,
                        }
                    )
                    df_to_tc_matrix = pd.concat(
                        [df_to_tc_matrix, df_uvruvr, df_uvrcop],
                        axis=0,
                        ignore_index=True,
                    )
                matrix = self.create_matrix(df_to_tc_matrix, list_currencies)

                self.create_matrix_file(matrix, valuation_date)

            else:
                logger.info("Las monedas no se encuentran completas")
                self.send_error_mail(difference)
                
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro crear la matriz con todas las tasas"
            logger.error(create_log_msg(raise_msg))
            update_report_process("Fallido", raise_msg, str(fpc_exc))
            raise PlataformError() from fpc_exc

    def load_files_to_sftp(self, df_file, route_sftp, file_name, data_connection_sftp):
        """Genera y carga los archivos en el SFTP de MATRIZ"""
        client = precia_sftp.connect_to_sftp(data_connection_sftp, 10)
        sftp_connect = client.open_sftp()
        logger.info("Comenzando a generar el archivo %s en el sftp", file_name)
        error_message = f"No se pudo generar el archivo en el SFTP: {file_name}"
        try:
            with sftp_connect.open(route_sftp + "/" + file_name, "w") as f:
                try:
                    f.write(df_file.to_csv(index=True, sep=" ", line_terminator='\r\n', header=True))
                except (Exception,):
                    logger.error(
                        create_log_msg("Fallo la escritura del archivo: %s", file_name)
                    )

        except (Exception,) as e:
            logger.error(
                create_log_msg(error_message)
            )
            update_report_process("Fallido", error_message, str(e))
        finally:
            sftp_connect.close()


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
    valuation_date_param = get_params(["VALUATION_DATE"])
    valuation_date = valuation_date_param["VALUATION_DATE"]
    report_process = {
          "input_id": "MatrizTC",
          "output_id": ["MatrizTC"],
          "process": "rates",
          "product": "rates",
          "stage": "Metodologia-Validacion",
          "status": "",
          "aws_resource": "glue-p-rates-parity",
          "type": "archivo",
          "description": "",
          "technical_description": "",
          "valuation_date": valuation_date
        }
    report_process["status"]=status
    report_process["description"]=description
    report_process["technical_description"]=technical_description
    launch_lambda(lambda_name, report_process)
    logger.info("Se envia el reporte de estado del proces")
        
        
def run():
    precia_utils_db_manager = actions_db(db_url_precia_utils)
    val_date = valuation_date_list[0]
    (
        business_days_df,
        last_bussines_day_df,
        datesstr,
    ) = precia_utils_db_manager.find_business_day(valuation_date_list[0])
    data_matrix_tc = generate_matrix_tc()
    is_holiday = data_matrix_tc.validate_effective_date(business_days_df)
    all_currencies = 'NO'
    for valuation_date in datesstr:
            data_exchange_rates = exchange_rates()
            src_otc_db_manager = actions_db(db_url_src)
            if valuation_date == val_date:
                df_exchange_rates = src_otc_db_manager.find_rates_data_today(valuation_date)
            else:
                df_exchange_rates = src_otc_db_manager.find_rates_data(valuation_date,val_date)
            df_usd_rates = pd.DataFrame()
            if 'EURUSD' in df_exchange_rates['id_precia'].values:
                df_usd_ecb_rates,eurusd_value = data_exchange_rates.calculate_eur_rates(df_exchange_rates,valuation_date)
                df_usd_rates = pd.concat([df_usd_ecb_rates, df_usd_rates])
            if ("EURMXN" in df_exchange_rates['id_precia'].values
                    )and("UDIMXN" in df_exchange_rates['id_precia'].values):
                    df_usdudi_rate = data_exchange_rates.calculate_usdudi(df_exchange_rates,eurusd_value,valuation_date)
                    df_usd_rates = pd.concat([df_usd_rates, df_usdudi_rate])
            if ("USDCLP" in df_exchange_rates["id_precia"].values) and (
                "CLFCLP" in df_exchange_rates["id_precia"].values
            ):
                df_clf_rates = data_exchange_rates.calculate_usdclf(
                    df_exchange_rates, valuation_date
                )
                logger.debug(df_usd_rates.to_string())
                """df_usd_rates.drop(
                    df_usd_rates[
                        (df_usd_rates["id_precia"] == "USDCLF")
                    ].index,
                    inplace=True,
                )"""
                logger.debug(df_usd_rates.to_string())
                
                df_usd_rates = pd.concat([df_usd_rates, df_clf_rates])

            if ("USDCOP" in df_exchange_rates['id_precia'].values
                )and("UVRCOP" in df_exchange_rates['id_precia'].values):
                df_usduvr_rate = data_exchange_rates.calculate_usduvr (df_exchange_rates,valuation_date)
										   
                df_usd_rates = pd.concat([df_usd_rates,df_usduvr_rate])
            df_uvrcop = df_exchange_rates[df_exchange_rates['id_precia']=='UVRCOP']
            
            df_usd_central_bank = data_exchange_rates.calculate_central_bank_rates(df_exchange_rates)
            df_usd_rates = pd.concat([df_usd_rates,df_usd_central_bank])
            df_all_usd_rates = data_exchange_rates.calculate_all_rates(df_usd_rates)
            df_all_usd_rates['valuation_date'] = valuation_date
            # ================================================================================================
            # REPORTE DE TASAS PROCESADAS EN LA BASE DE DATOS PARA EL TRIGGER
            pub_rates_db_manager_trigger = actions_db(db_url_pub)
            logger.debug(df_exchange_rates.to_string())
            start_usd = df_all_usd_rates[df_all_usd_rates['id_precia'].str.startswith('USD')]
            end_usd = df_all_usd_rates[df_all_usd_rates['id_precia'].str.endswith('USD')]
            clfclp_rate = df_exchange_rates[df_exchange_rates['id_precia'] == 'CLFCLP']
            usdmxn_rate = df_all_usd_rates[df_all_usd_rates['id_precia'] == 'USDMXN']
            logger.debug(df_exchange_rates.to_string())
            logger.debug(clfclp_rate)
            logger.debug(usdmxn_rate)
            pre_usd_rates = pd.concat([start_usd,end_usd, clfclp_rate, usdmxn_rate])
            logger.debug("Tasas del proceso")
            logger.debug(pre_usd_rates.to_string())
            
            df_rates_today = pub_rates_db_manager_trigger.get_rates_today(valuation_date)
            logger.debug(df_rates_today.to_string())
            
            post_usd_rates = pd.merge(pre_usd_rates, df_rates_today, on='id_precia', how='left')
            logger.debug(f"Tasas de bd y calculadas: {post_usd_rates.to_string()}")
            selected_data = post_usd_rates.loc[post_usd_rates['value_rates_x'] != post_usd_rates['value_rates_y'], 'id_precia']
            logger.debug(selected_data)
            reports_rates = pd.DataFrame(post_usd_rates, columns=['id_precia'])
            logger.debug(reports_rates)
            if not reports_rates.empty:
                logger.info(f"Tasas que se reportaran en la tabla: {reports_rates.to_string()}")
                usd_rates_list = reports_rates['id_precia'].tolist()
                payload = {"product": "Rates parity", "input_name":usd_rates_list, "valuation_date":datesstr}
                data_exchange_rates.trigger_rates(lambda_process, payload)
                logger.info("Se han reportado las tasas para el proceso")
            else:
                logger.info("Las tasas calculadas son la mismas que en bd, no se reportaran")
            
            #=====================================================================
            pub_rates_db_manager = actions_db(db_url_pub)
            if df_all_usd_rates.empty:
                logger.info(f'No hay información para el día {valuation_date}')
                update_report_process("Fallido", "No hay informacion", "Empty Data")
                raise PlataformError (f'No hay información para el día {valuation_date}')
            pub_rates_db_manager.disable_previous_info(df_all_usd_rates,valuation_date)
            df_all_usd_rates_to_db = data_exchange_rates.remove_similar_pairs(df_all_usd_rates)
            pub_rates_db_manager.insert_data(df_all_usd_rates_to_db)
            data_matrix_tc = generate_matrix_tc()
            effective_date = data_matrix_tc.validate_effective_date(business_days_df)
            business_days = business_days_df.iloc[:,1:].sum(axis=1)
            df_exchange_rates = data_matrix_tc.create_matrix_run(business_days,business_days_df,effective_date,last_bussines_day_df,valuation_date,df_uvrcop,df_all_usd_rates_to_db)
            rates_tc = data_matrix_tc.select_parity_currencies(df_exchange_rates)
            df_currencies_tc = data_matrix_tc.compare_currencies(
                rates_tc, df_exchange_rates
            )
            all_currencies, difference = data_matrix_tc.validate_full_currencies(
                df_currencies_tc, valuation_date
            )
            logger.info('all_currencies (%s): %s',valuation_date,all_currencies)
    logger.info('all_currencies pos for ' + str(all_currencies))

    logger.info('all_currencies pos for ' + str(all_currencies))
    dates = len(valuation_date_list)
    if dates == 1:
        valuation_date_list2 = [date.strip() for date in valuation_date_list]
        valuation_date_list2 = tuple(valuation_date_list2)
        valuation_date_list2 = ''.join(valuation_date_list2) 
        valuation_date_list2 = "('" + valuation_date_list2 + "')"
        logger.info(valuation_date_list2)
    else:
        valuation_date_list2 = tuple(valuation_date_list)
    payload = {"valuation_date": datesstr}
    payload = json.dumps(payload)
    src_otc_db_manager = actions_db(db_url_src)
    df_exchange_rates_uvrcop = src_otc_db_manager.find_rates_data_uvr(tuple(datesstr))
    logger.info(df_exchange_rates_uvrcop)
    df_exchange_rates_uvrcop = df_exchange_rates_uvrcop[['id_precia','value_rates','valuation_date']]
    df_exchange_rates_uvrcop = (df_exchange_rates_uvrcop.rename(index=str, columns={"id_precia": "Moneda", "value_rates": "Valor", "valuation_date":"Fecha"})).to_string(index=False)
    mail_subject = "Reporte de variaciones - Tasas de Cambio"
    body = f"""
Cordial Saludo.

Se adjunta el reporte de las varaciones de las tasas del día de {'fecha'}
				 
Ya se realizo la creación de la matriz, esta disponible para su publicación 
Para lanzar el proceso de publicación, utilice el siguiente json:
{payload}
        
El valor de la UVRCOP que se utilizo en la creación de la matriz fue
{df_exchange_rates_uvrcop}


Megatron.

Enviado por el servicio automático de notificaciones de Precia PPV S.A. en AWS
"""
						 
        #----Variaciones Tasas de Cambio----
    params = [
        "DB_SECRET",
        "LIMITE_VARIACION",
        "SMTP_SECRET",
        "S3_BUCKET",
        "S3_FILE_PATH_REPORT"
    ]
    params_glue = get_params(params)
    secret_db = params_glue["DB_SECRET"]
    key_secret_db = get_secret(secret_db)
    params_glue = get_params(params_key)
    url_aurora = key_secret_db["conn_string_aurora"]
    schema_utils = key_secret_db["schema_utils"]
    url_connection_utils = url_aurora + schema_utils
    url_aurora = key_secret_db["conn_string"]
    schema_rates = key_secret_db["schema_pub_rates"]
    url_connection_rates = url_aurora + schema_rates
    secret_smtp = params_glue["SMTP_SECRET"]
    key_secret_smtp = get_secret(secret_smtp)
    bucket_s3_reports = params_glue["S3_BUCKET"]
    s3_file_path_report = params_glue["S3_FILE_PATH_REPORT"]
    param_variation = int(params_glue["LIMITE_VARIACION"])
    etl = ExchangeRateVariations(url_connection_utils, url_connection_rates, val_date)
    etl.extract_data()
    etl.transform_data()
    etl.load_info_pdf(bucket_s3_reports, s3_file_path_report, param_variation)
    logger.info('all_currencies  ' + str(all_currencies))
    if all_currencies == 'SI':
        etl.send_report_email(key_secret_smtp, mail_subject, body)
    update_report_process("Exitoso", "Proceso Finalizado", "")
        
        
if __name__ == "__main__":
    run()
