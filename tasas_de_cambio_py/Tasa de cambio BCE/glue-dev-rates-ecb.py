import sqlalchemy as sa
import logging
import io
import json
import requests
import boto3
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta, timezone

from common_library_email_report.ReportEmail import ReportEmail
from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError


date_format = "%Y-%m-%d"
logger = setup_logging(logging.INFO)
try:
    params_key = [
        "DB_SECRET",
        "SMTP_SECRET",
        "URL",
        "VALUATION_DATE",
        "RATE_PARITY_LAMBDA"
    ]
    params_dict = get_params(params_key)
    secret_smtp = params_dict["SMTP_SECRET"]   
    secret_db = params_dict["DB_SECRET"]
    db_url_dict = get_secret(secret_db)
    db_url = db_url_dict["conn_string"]
    schema_src = db_url_dict["schema_src_rates"]
    db_url_src = db_url + schema_src
    db_url_utils = db_url_dict["conn_string_aurora"]
    schema_utils = db_url_dict["schema_utils"]
    db_url_precia_utils = db_url_utils + schema_utils
    url = params_dict["URL"]
    valuation_date_str = params_dict["VALUATION_DATE"]
    parity_lambda = params_dict["RATE_PARITY_LAMBDA"]
    if valuation_date_str == "TODAY":
        colombia_offset = timedelta(hours=-5)
        colombia_timezone = timezone(colombia_offset)
        valuation_date = datetime.now(colombia_timezone).date()
    else:
        valuation_date = datetime.strptime(valuation_date_str, date_format)
    valuation_date = valuation_date.date() 
    logger.info(valuation_date)

    
except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc


class actions_db():
    CONN_CLOSE_ATTEMPT_MSG = "Intentando cerrar la conexion a BD"
    def __init__(self, db_url_src : str) -> None:
        """Gestiona la conexion y las transacciones a base de datos

        Args:
            db_url_src  (str): url necesario para crear la conexion a base de datos

        Raises:
            PlataformError: Cuando falla la creacion del objeto
        """
        try:
            self.db_url_src  = db_url_src 
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
                self.db_url_src , connect_args={"connect_timeout": 2}
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
        
    
    def find_business_day(self):
        """
        Se realiza la consulta de los dias hábiles segun la fecha actual
        Args:
            next_day: siguiente día (hábil o no) de la fecha de valoración 
        Returns:
            business_days_df: dataframe con la información del día habil y los posteriores dias no habiles hasta el siquiente dia hábil
        """        
        
        try:
            db_connection = self.create_connection()
            next_day = valuation_date + timedelta(days=1)
            select_query = sa.sql.text(
            f"""SELECT dates_calendar, target_calendar FROM precia_utils_calendars WHERE dates_calendar BETWEEN '{valuation_date}' AND 
            (SELECT MIN(dates_calendar) FROM precia_utils_calendars WHERE dates_calendar >= '{next_day}' AND target_calendar =1)""")
            business_days_df = pd.read_sql(select_query, db_connection)
            business_days_df = business_days_df.iloc[:-1]
            logger.debug(business_days_df)
            db_connection.close()
            return business_days_df
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  a la BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc
        
        
    def disable_previous_info(self, info_df: pd.DataFrame):
        """
        Funcion encargada de deshabilitar la información anterior en la
        tabla, solo si se encuentra en la necesidad de volver a cargar la información
        """        
        error_msg = "No se pudo realizar la deshabilitacion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            conn = db_connection.connect()
            
            try:
                info_df = info_df.reset_index()
                
                id_precia_value_list = (info_df['id_precia'].tolist())
                id_precia_list = []
                for id_supplier in id_precia_value_list:
                    if id_supplier not in id_precia_list:
                        id_precia_list.append(id_supplier)
                id_precia_list = tuple(id_precia_list)
                info_df.set_index("id_precia", inplace=True)
                valuation_date_str = info_df.at[id_precia_value_list[0], "valuation_date"]
                status_info = 0
                update_query = ("UPDATE src_exchange_rates SET status_info= {} WHERE id_precia in {} and valuation_date = '{}'".format(status_info,id_precia_list, valuation_date_str))
                conn.execute(update_query)
                
            except sa.exc.SQLAlchemyError as sql_exc:
                logger.error(create_log_msg(error_msg))
                raise_msg = (
                    "La base de datos no acepto los datos. validar tipos de datos"
                )
                raise PlataformError(raise_msg) from sql_exc
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            if db_connection != None:
                db_connection.close()
            raise PlataformError() from fpc_exc
        

    def insert_data(self, info_df: pd.DataFrame):
        """
        Función encargada de realizar la inserción de la información a la base de datos  
        """
        error_msg = "No se pudo realizar la insercion de la informacion"
        try:
            logger.info("Se intenta conectarse a la base de datos...")
            db_connection = self.create_connection()
            logger.info("Conexion exitosa.")
            try:
                logger.debug(info_df)
                if "index" in info_df.columns.values:
                    info_df = info_df.drop(["index"],axis=1,)
                info_df.to_sql('src_exchange_rates', con=db_connection, if_exists="append", index=False)
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
            if db_connection != None:
                db_connection.close()
            raise PlataformError() from fpc_exc

    
    def find_data(self, df_ecb_currencies: pd.DataFrame):
        """
        Se realiza la busqueda de las monedas entregadas por el BCE en la tabla donde se almacenan las monedas esperadas   
        """
        error_msg = 'No se pudo obtener la data de las monedas del BCE'
        try:
            logger.info('Se intenta conectarse a la base de datos...')
            db_connection = self.create_connection()
            logger.info("Conexion exitosa.") 
            currency_list_to_find = df_ecb_currencies['id_supplier'].values.tolist()
            currency_list = []
            for currency in currency_list_to_find:
                if currency not in currency_list:
                    currency_list.append(currency)
            currency_list = tuple(currency_list)
            query = "select currency from src_rates_european_central_bank where currency IN {}".format(currency_list)
            df_currencies_bce = pd.read_sql(query,db_connection)
            return df_currencies_bce
        except (Exception,):
                logger.error(create_log_msg(error_msg))
                raise
            
            
class get_data_ect():
    
    def get_xml_file(self,url:str):
        """
        Se realiza la peticion a la API del BCE  
        """
        error_msg = 'No se pudo descargar el archivo'
        try: 
            response = requests.get(url)
            if response.status_code == 200:
                xml_bytesio = io.BytesIO(response.content)
                logger.info('Archivo XML convertido exitosamente en objeto BytesIO')
            return xml_bytesio
            
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise  
    

    def get_data_xml(self, xml_bytesio:str):
        """
        Se extrae la información que se descargo de la API y se convierte esta información en una tabla con tres columns fecha, moneda y valor de la tasa    
        """
        error_msg = 'No se logro traer la data del archivo XML'
        try:
            tree = ET.parse(xml_bytesio)
            root = tree.getroot()
            for child in root:
                logger.debug(child.tag)
            data = []
            for cube in root.iter(child.tag):
                time = cube.attrib.get('time')
                currency = cube.attrib.get('currency')
                rate = cube.attrib.get('rate')
                data.append((time, currency, rate))
            return data
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise  


    def xml_to_pandas(self, data_xml:str):
        """
        La información que se encuentra en la tabla se convierte en dataframe  
        """
        error_msg = 'No se pudo convertir la iformacion a dataframe'
        try: 
            df_bce = pd.DataFrame(data_xml, columns=['valuation_date','id_supplier','value_rates'])
            return df_bce
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise  


    def modify_data(self, df_bce,next_business_date:str):
        """
        Se modifica la informacion para obtener la fecha de valoracion en la misma columna, crear el id_precia y asignar la fecha de día hábil   
        """
        error_msg = 'No se logro establecer la fecha en el archivo'
        try:
            logger.debug((df_bce['valuation_date'].unique().tolist())[1])
            df_bce['valuation_date'] = (df_bce['valuation_date'].unique().tolist())[1]
            df_bce['BCE'] = 'EUR'
            df_bce['id_precia'] = df_bce['BCE'] + df_bce['id_supplier']
            df_bce['effective_date'] = next_business_date
            return df_bce
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise    
    
    
    def drop_extra_data(self, df_bce:pd.DataFrame):
        """
        Se elimina los campos nulos 
        """
        error_msg = 'No se logro eliminar la informacion innecesaria'
        try: 
            df_bce = df_bce.loc[2:]
            return df_bce
        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise  
    
    
    def validate_currencies(self,df_bce:pd.DataFrame,df_currencies_bce:pd.DataFrame):
        """
        Se realiza la validacion de las monedas que se espera y las que se consultaron en el BCE sean las mismas, en caso contrario notificar si falto la recolección del alguna  
        """
        error_msg = 'No se logro validar la informacion de las monedas'
        try:
            list_currency = df_currencies_bce['currency'].values.tolist()
            logger.info('Se intenta validar que las monedas se encuentran completas')
            df_bce['validate'] = df_bce['id_supplier'].isin(list_currency)
            missing_values = df_bce.loc[df_bce['validate']==False]
            if missing_values.empty:
                df_bce = df_bce.drop(["validate","BCE",],axis=1,)
                return df_bce 
            else:
                missing_values = missing_values['id_supplier'].tolist()
                logger.debug(missing_values)
                missing_values = ', '.join(missing_values)
                subject = "Megatron - Insumos incompletos del Banco central europeo"
                body = f"Buen día \n No se lograron consultar todas las monedas del Banco central europeo, se realizo la consulta correctamente pero falto: \n {missing_values}  \n Megatron\n PBX:(57-601)6070071\n Cra 7 No 71-21 Torre B Of 403 Bogotá"
                key_secret = get_secret(secret_smtp)
                mail_from = key_secret["mail_from"]
                mail_to = key_secret["mail_to"]
                email = ReportEmail(subject, body)
                data_connection_smtp = {
                    "server": key_secret["smtp_server"],
                    "port": key_secret["smtp_port"],
                    "user": key_secret["smtp_user"],
                    "password": key_secret["smtp_password"]}

                smtp_connection = email.connect_to_smtp(data_connection_smtp)
                message = email.create_mail_base(mail_from, mail_to)
                email.send_email(smtp_connection, message)             
                df_bce = df_bce.drop(["validate","BCE",],axis=1,)
                return df_bce 

        except (Exception,):
            logger.error(create_log_msg(error_msg))
            raise          

  
    def validate_effective_date(self,business_days_df):
        """
        Se realiza la validacion de la fecha actual sea día hábil
        """
        try:
            effective_date = business_days_df['target_calendar'].iloc[0]
            logger.debug(effective_date)
            return effective_date
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro validar la vigencia de la información"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc  
        

    def validate_business_date(self,business_days_df):
        """
        Esta función se encarga de obtener el primer dia 
        """
        try:
            business_days_df['dates_calendar'] = business_days_df['dates_calendar'].astype('string')
            valuation_date = business_days_df['dates_calendar'].iloc[0]
            list_dates = business_days_df['dates_calendar'].tolist() 
            return list_dates,valuation_date
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro obtener el día de vigencia"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc  

 
    def find_effective_date(self, business_days_df):
        """
        Busca la siguiente fecha de vigencia segun la fecha de valoracion  
        """
        try:
            logger.debug(business_days_df)
            next_date = business_days_df['dates_calendar'].iloc[-1]
            return next_date
        except (Exception,) as fpc_exc:
            raise_msg = "No se logro agregar la fecha de vigencia"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc
        

    def launch_lambda(self, lambda_name: str, payload: list):
        """Lanza una ejecucion de lambda indicada
        Args:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
        """
        lambda_client = boto3.client("lambda") 
        lambda_response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),)
        logger.info("lambda_response:\n%s", lambda_response["Payload"].read().decode())
 
          
def run():
        data_bce = get_data_ect()
        precia_utils_db_manager = actions_db(db_url_precia_utils)
        business_days_df = precia_utils_db_manager.find_business_day()
        effective_date = data_bce.validate_effective_date(business_days_df)
        list_business_date,valuation_date = data_bce.validate_business_date(business_days_df)
        today = datetime.now().date()
        today = datetime.strftime(today, date_format)
        logger.info(valuation_date)
        logger.info(today)
        logger.info(type(valuation_date))
        logger.info(type(today))
        if valuation_date == today:
            xml_bytesio = data_bce.get_xml_file(url)
            data_xml = data_bce.get_data_xml(xml_bytesio)
            next_business_date = data_bce.find_effective_date(business_days_df)
            df_bce = data_bce.xml_to_pandas(data_xml)
            df_bce = data_bce.modify_data(df_bce,next_business_date)
            clean_df_bce = data_bce.drop_extra_data(df_bce)
            src_action_db = actions_db(db_url_src)
            df_currencies_bce = src_action_db.find_data(clean_df_bce)
            df_currencies_bce = data_bce.validate_currencies(clean_df_bce,df_currencies_bce)
            for date in list_business_date:
                if effective_date == 1:
                    df_currencies_bce['valuation_date'] = date
                    df_currencies_bce['effective_date'] = date
                    src_action_db.disable_previous_info(df_currencies_bce)
                    src_action_db.insert_data(df_currencies_bce)
                    logger.info(df_currencies_bce)
                else:
                    logger.info('Hoy no es un día habil segun el calendario Target')
            list_business_date = ' '.join([str(dates) for dates in list_business_date])
            valuation_date = {'valuation_date_list': list_business_date}
            data_bce.launch_lambda(parity_lambda, valuation_date )
        else:
            logger.info('La informacion no esta actualizada')

if __name__ == "__main__":
    run()