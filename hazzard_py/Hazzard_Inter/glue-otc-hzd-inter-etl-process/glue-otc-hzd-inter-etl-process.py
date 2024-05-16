"""Módulo que se encarga de los Hazzard Internacionales y Probabilidades de Default Y Supervivencia
para ser insertados en base de datos
Este proceso tiene dependencias de las generación de curvas Swaps Inter y CDS"""

# Nativas
from base64 import b64decode
from dateutil import relativedelta as rd
import datetime as dt
import json
import logging
from sys import argv, exc_info, stdout

# AWS
from awsglue.utils import getResolvedOptions
from boto3 import client as aws_client

# De terceros
import pandas as pd
import numpy as np
import sqlalchemy as sa
from scipy import  interpolate
from scipy import optimize

# Personalizadas
from DateUtils import DateUtils

ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)
PARAMETER_STORE = "ps-otc-lambda-reports"

#-------------------------------------------------------------------------------------------------------------------
# CONFIGURACIÓN DEL SISTEMA DE LOGS
_bootstrap_error_message = 'Fallo el calculo del error bootstrap'
_bootstrap_error_raise_message ="No se pudo calcular el error bootstrap para la curva con los insumos dados"
def setup_logging(log_level):
    """
    Configura el sistema de registro de logs.
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
        file_handler = logging.StreamHandler(stdout)
    file_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(file_handler)
    logger.setLevel(log_level)
    return logger


def create_log_msg(log_msg: str) -> str:
    """
    Aplica el formato adecuado al mensaje log_msg, incluyendo información sobre excepciones.
    """
    exception_type, exception_value, exception_traceback = exc_info()
    if not exception_type:
        return f"{log_msg}."
    error_line = exception_traceback.tb_lineno
    return ERROR_MSG_LOG_FORMAT.format(
        log_msg, error_line, exception_type.__name__, exception_value
    )
    
class BaseError(Exception):
    """Exception personalizada para la capa precia_utils"""


class UserError(BaseError):
    """
    Clase heredada de BaseError que permite etiquetar los errores causados
    por la informacion suministrada en el event
    """

    def __init__(
        self,
        error_message="El event no tienen la estructura y/o valores esperados",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)


class PlataformError(BaseError):
    """
    Clase heredada de BaseError que permite etiquetar los errores causados por
    errores del sistema identificados
    """

    def __init__(
        self,
        error_message="La plataforma presenta un error, ver el log para mas detalles",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)
        

#-------------------------------------------------------------------------------------------------------------------
# OBTENER SECRETOS Y PARÁMETROS
def get_params(parameter_list) -> dict:
    """Obtiene los parametros de entrada del glue

    Parameters:
        parameter_list (list): Lista de parametros
    
    Returns:
        dict: Valor de los parametros
    """
    try:
        logger.info("Obteniendo parametros del glue job ...")
        params = getResolvedOptions(argv, parameter_list)
        logger.info("Todos los parametros fueron encontrados")
        return params
    except Exception as e:
        error_msg = (f"No se encontraron todos los parametros solicitados: {parameter_list}")
        logger.error(create_log_msg(error_msg))
        raise Exception(error_msg) from sec_exc
        
def get_secret(secret_name: str) -> dict:
    """
    Obtiene secretos almacenados en el servicio Secrets Manager de AWS.

    Parameters:
        secret_name (str): Nombre del secreto en el servicio AWS.
        
    Returns:
        dict: Secreto con la informacion desplegada en Secrets Manager AWS.
    """
    try:
        logger.info('Intentando obtener secreto: "%s" ...', secret_name)
        cliente_secrets_manager = aws_client("secretsmanager")
        secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
        if "SecretString" in secret_data:
            secret_str = secret_data["SecretString"]
        else:
            secret_str = b64decode(secret_data["SecretBinary"])
        logger.info("Se obtuvo el secreto.")
        return json.loads(secret_str)
    except (Exception,) as sec_exc:
        error_msg = f'Fallo al obtener el secreto "{secret_name}"'
        logger.error(create_log_msg(error_msg))
        raise Exception(error_msg) from sec_exc
    

def get_parameter_store(parameter_name):
    """
    Obtiene el valor del parameter store
    
    Parameters:
        parameter_name (str): Nombre del parámetro
    
    Returns:
        str: valor del parametro obtenido
    """
    logger.info("Intentando leer el parametro: "+parameter_name)
    ssm_client = aws_client('ssm')
    response = ssm_client.get_parameter(
        Name=parameter_name, WithDecryption=True)
    logger.info("El parametro tiene el valor: " +
                str(response['Parameter']['Value']))
    return response['Parameter']['Value']

#--------------------------------------------------------------------------------
# EJECUTAR LAMBDA

def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Parameters:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
    """
    try:
        lambda_client = aws_client("lambda")
        lambda_response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        logger.info("lambda_response:\n%s", lambda_response["Payload"].read().decode())
    except (Exception,) as lbd_exc:
        raise_msg = f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
        logger.error(create_log_msg(raise_msg))
        raise
    
#---------------------------------------------------------------------------------
# REPORTE DE ESTADOS
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

    lambda_name = get_parameter_store(PARAMETER_STORE)
    curve_param = get_params(["HAZZARD"])
    curve = curve_param["HAZZARD"]
    valuation_date_param = get_params(["VALUATION_DATE"])
    valuation_date = valuation_date_param["VALUATION_DATE"]
    report_process = {
          "input_id": curve,
          "output_id":[curve],
          "process": "Derivados OTC",
          "product": "Hazzard_Inter",
          "stage": "Metodologia",
          "status": "",
          "aws_resource": "glue-p-otc-hzd-inter-etl-process",
          "type": "archivo",
          "description": "",
          "technical_description": "",
          "valuation_date": valuation_date
        }
    report_process["status"]=status
    report_process["description"]=description
    report_process["technical_description"]=technical_description
    launch_lambda(lambda_name, report_process)
    logger.info("Se envia el reporte de estado del proceso")

#---------------------------------------------------------------------------------
# METODOLOGÍA HAZZARD INTERNACIONAL Y PROBABILIDADES DE DEFAULT

def hazard_cum(tao, hazard_rates_curve):
    
    """     
    Calcula los Hazard Rates acumulados. Esta función es necesaria para la funcion 'sp_probability'.
      
    Params
    -------
        tao (numpy.ndarray): Numero de dias para los que se calcula la intensidad acumulada
        hazard_rates_curve (numpy.ndarray): riesgo "Hazard" asociadas a cada dia (tao)

    Returns
    -------
        (numpy.ndarray): contine los Hazard Rates acumulados a cada dia ingresado.        
    """ 
    try:
        lt = len(tao)-1
        hazard_rates_cum = np.cumsum((tao - np.append(0,tao[0:lt])) * hazard_rates_curve)
        return(hazard_rates_cum)
    except(Exception,):
        logger.error(create_log_msg(''))
        raise PlataformError("")
    
def cds_pr(hazards, date_ini, cds_curve, cc_curve, rec=0.4, base=360):
    """
    Calcula el error bootstrap para la curva de cds dados los hazards ingresados
    Params
    -------
        hazards (numpy.array) : Hazards asociados al cds
        date_ini (datetime.date): Fecha del calculo
        cds_curve (Dataframe): Dataframe con la curva de cds
                                   days:  Dias de la curva.
                                   rate: Spread.
        cc_curve (numpy.array): Curva de descuento de los flujos del cds
        rec (float): Tasa de recuperación del cds
        base (float): Base del cds

    Returns
    -------
        (float): Suma de errores cuadrados

    """
    try:
        #Se generan las fechas de pago de los cds
        possible_first_cds_dates = [dt.date(date_ini.year,m,20) for m in [3,6,9,12]]+[dt.date(date_ini.year+1,3,20)]
        first_cds_date = [d for d in possible_first_cds_dates if d > date_ini][0]
        last_cds_date = date_ini+dt.timedelta(days=int(cds_curve["days"][len(cds_curve["days"])-1]))
        year_diff = last_cds_date.year - first_cds_date.year
        month_diff = last_cds_date.month - first_cds_date.month
        cds_payments = int((year_diff * 12 + month_diff)/3)
        pay_cds_dates = [first_cds_date + rd.relativedelta(months=3*i) for i in range(0,cds_payments+1)]
        days_pay_cds = np.array([(pay_cds_date-date_ini).days for pay_cds_date in pay_cds_dates])
        #Calculo del factor de descuento para los dias de pago
        cc_day_freq = cc_curve[days_pay_cds-1]
        df = 1/(1+cc_day_freq*days_pay_cds/base)
        #Se crea la curva hazard para los dias de pago
        hazard_pay_freq = interpolate.interp1d(cds_curve["days"], hazards, kind = 'next', fill_value = "extrapolate")(days_pay_cds)
        dt_freq = (days_pay_cds-np.append([0],days_pay_cds[:-1]))/base
        t_pay_freq = days_pay_cds/base
        haz = hazard_cum(t_pay_freq,hazard_pay_freq)
        #Se calcula el valor presente de las patas del cds
        prem_leg = cds_curve["rate"] * np.cumsum(np.exp(-haz) * dt_freq * df)[np.where(np.isin(days_pay_cds,cds_curve["days"]))]
        def_leg = (1 - rec) * np.cumsum(hazard_pay_freq * np.exp(-haz) * dt_freq * df)[np.where(np.isin(days_pay_cds,cds_curve["days"]))]
        error = prem_leg-def_leg
        return np.dot(error,error)
    except(Exception,):
        logger.error(create_log_msg(_bootstrap_error_message))
        raise PlataformError(_bootstrap_error_raise_message)


def hazard_objective(date_ini, cds_curve, cc_curve, rec = 0.4, base = 360):
    """
    Construye la curva de hazard rates para una contraparte extranjera dados los cds

    Params
    ----------
    date_ini (datetime.date): Fecha de generacion.
    cds_curve (pd.dataframe): Dataframe de dos columnas
                               days:  Dias de la curva.
                               rate: Spread.                                             
    cc_curve (numpy.ndarray) : Curva de descuento de los flujos de los cds.
    rec (float): Tasa de recuperación del cds
    base (float): Base del cds
    
    Returns
    ------------
    (pd.dataframe): Curva hazarrd
                        days:  Dias de la curva.
                        hazard: Hazard asociado a la contraparte.
    """
    try:
        logger.info(cds_curve.rate)
        h0=cds_curve["rate"]/(1-rec)
        bnds=[(1e-5,100)]*len(h0)
        optimization = optimize.minimize(cds_pr,h0,bounds=bnds,args=(date_ini, cds_curve, cc_curve),tol=1e-16)
        if not optimization.success:
            logger.error(create_log_msg('Se genero un error en la optimizacion de la curva hazard'))
            logger.error(create_log_msg(optimization.message))
            raise PlataformError("Hubo un error en la optimizacion de la curva hazard")
        hazards = optimization.x
        return pd.DataFrame({"days":cds_curve["days"],"hazard":hazards})
    except(Exception,):
        logger.error(create_log_msg('Se genero un error en la generación de la curva hazzard'))
        raise PlataformError("Hubo un error en la generación de la curva hazzard")


def sp_probability(haz_rate_info, days):
    
    """     
    Calcula la probabilidad de supervivencia de la contraparte a la cual pertene las tasas de riesgo 'Hazard' ingresadas.
      
    Params
    -------
        haz_rate_info (DataFrame): Contiene dos columnas:
                                    days: Dias de la curva de hazards
                                    hazard: riesgo "Hazard" asociadas a cada dia
        days (numpy.ndarray): Numero de dias a calcular la probabilidad de supervivencia, default y default condicionada

    Returns
    -------
        (DataFrame): Contiene cuatro columnas:
                        days: Dias de la curva de probabilidades
                        sp: Probabilidad de supervicencia para el dia
                        dp: Probabilidad de default para el dia
                        dp_cond: Probabilidad de default condicionada para el dia
    """ 
    try:
        days_haz = np.arange(1,days[-1]+1)
        hazard_rates_curve = interpolate.interp1d(haz_rate_info["days"],  haz_rate_info["hazard"], kind = 'next', fill_value = "extrapolate")(days_haz)
        if len(np.where(np.isnan(hazard_rates_curve))[0])>0:
            hazard_rates_curve[np.where(np.isnan(hazard_rates_curve))[0]]=hazard_rates_curve[min(np.where(np.isnan(hazard_rates_curve))[0])-1]
        hazard_rates_cum = hazard_cum(days_haz/360, hazard_rates_curve)
        logger.info(hazard_rates_cum)
        sp = np.exp(-hazard_rates_cum[days-1])
        dt = (days-np.append([0],days[:-1]))/360
        dp = 1 - sp
        dp_cond = hazard_rates_curve[days-1] * dt
        return(pd.DataFrame({"days":days, "sp":sp,"dp": dp, "dp_cond":dp_cond}))
    except(Exception,):
        logger.error(create_log_msg('Se genero un error en el calculo de las probabilidades de supervivencia y default'))
        raise PlataformError("Hubo un error en el calculo de las probabilidades de supervivencia y default")

#---------------------------------------------------------------------------------

class DbHandler:
    """Representa la extracción e inserción de información de las bases de datos"""
    def __init__(self, url_db: str, valuation_date: str) -> None:
        self.connection = None
        self.valuation_date = valuation_date
        self.url_db = url_db
        
    
    def connect_db(self):
        """Genera la conexión a base de datos"""
        try:
            self.connection = sa.create_engine(self.url_db).connect()
            logger.info("Se conecto correctamente a la base de datos")
        except Exception as e:
            logger.error(create_log_msg("Fallo la conexión a la base de datos"))
            raise PlataformError("Hubo un error en la conexión a base de datos: " + str(e))


    def disconnect_db(self):
        """Cierra la conexión a la base de datos"""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def get_data_calendars(self, calendar_swap: list):
        """Trae la información de calendarios"""
        query_calendars = (
            "SELECT dates_calendar "
            + "FROM precia_utils_calendars "
            + "WHERE "+calendar_swap+" = 1 "
        )
        error_message = "No se pudo traer la info de los calendarios"
        try:
            df_calendars = pd.read_sql(query_calendars, self.connection)
            logger.debug(df_calendars)
            logger.info("Se obtuvo la información de los calendarios")
            return df_calendars
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible extraer la informacion de los calendarios: "+ str(e))
    

    def get_data_cds(self, cds: str):
        """Trae el valor del CDS"""
        query_exchange_rate = sa.sql.text("""
            SELECT days, mid_price as rate FROM prc_otc_cds
            WHERE counterparty = :cds
            AND valuation_date = :valuation_date
            AND status_info = :status_info
        """)
        query_params = {
            "cds": cds,
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No fue posible extraer la información del CDS"
        try:
            df_exchange_rate = pd.read_sql(query_exchange_rate, self.connection, params=query_params)
            logger.debug(df_exchange_rate)
            logger.info(f"Se obtuvo la información del CDS: {cds}")
            if df_exchange_rate.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos del CDS: {self.valuation_date}")
            return df_exchange_rate
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+cds))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible extraer la información del CDS {cds}: "+ str(e))
        

    def get_data_swapcc_inter_daily(self, curve: str):
        """Trae la información de los Swap Inter diaria"""
        query_exchange_rate = sa.sql.text("""
            SELECT days, rate as rate FROM pub_otc_inter_swap_cc_daily
            WHERE curve = :curve
            AND valuation_date = :valuation_date
            AND status_info = :status_info
        """)
        query_params = {
            "curve": curve,
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No fue posible extraer la información del Swap"
        try:
            df_exchange_rate = pd.read_sql(query_exchange_rate, self.connection, params=query_params)
            logger.debug(df_exchange_rate)
            logger.info(f"Se obtuvo la información del Swap: {curve}")
            if df_exchange_rate.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos del Swap: {self.valuation_date}")
            return df_exchange_rate
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible extraer la información de la curva {curve}: "+ str(e))
        
    
    def disable_previous_info(self, pub_table: str, valuation_date: str, id_precia: str):
        """Actualiza el status de la información se encuentra en la base de datos
        pasando de 1 a 0
        """
        error_message = "Falló la actualización del estado de la información"
        try:
            update_query = (
                "UPDATE "+ pub_table + ""
                + " SET status_info= 0"
                + " WHERE id_precia = '"+ id_precia + "'"
                + " AND valuation_date = '"+ valuation_date + "'"
            )
            self.connection.execute(update_query)
            logger.info("Se ha actualizado el estado de la informacion correctamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible actualizar el estado de la informacion en base de datos: ", str(e))
        

    def insert_data_db(self, df_insert: pd.DataFrame, pub_table: str):
        """Inserta la informacion calculada durante el proceso"""
        error_message = "Falló la inserción de la información en la base de datos"
        try:
            df_insert.to_sql(pub_table, con=self.connection, if_exists="append", index=False)
            logger.info("Finaliza la inserción a la base de datos exitosamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible insertar la información en la base de datos: "+ str(e))


class ETL():
    """Representa la orquestación de la ETL"""
    def __init__(self, url_publish_otc: str, url_process_otc: str, url_precia_utils: str) -> None:
        self.url_publish_otc = url_publish_otc
        self.url_process_otc = url_process_otc
        self.url_precia_utils = url_precia_utils
        valuation_date_param = get_params(["VALUATION_DATE"])
        self.valuation_date = valuation_date_param["VALUATION_DATE"]
        cds_param = get_params(["HAZZARD"])
        self.cds = cds_param["HAZZARD"]

    
    def extract_data(self):
        """Orquesta la extracción de la informacion de la clase DbHandler"""
        usdois = ['CO', 'BA', 'CB', 'GS', 'JP', 'MS', 'WEF', 'BR', 'RBOC']
        eurois = ['BCS', 'BBV', 'BNP', 'COM', 'DB', 'HBC', 'HS', 'INGB', 'SAN', 'SCB', 'SCP', 'UBS', 'NS']
        jpyois = ['SUM', 'MTFC']
        logger.info("Comienza la extraccion de informacion de base de datos...")
        try:
            self.db_handler_utils = DbHandler(self.url_precia_utils, self.valuation_date)
            self.db_handler_utils.connect_db()
            self.df_calendar = self.db_handler_utils.get_data_calendars("federal_reserve_calendar")
            self.db_handler_utils.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", e.error_message, str(e))
            raise PlataformError(e.error_message)
        
        try:
            self.db_handler_prc = DbHandler(self.url_process_otc, self.valuation_date)
            self.db_handler_prc.connect_db()
            self.df_cds = self.db_handler_prc.get_data_cds(self.cds)
            logger.info(self.df_cds.to_string())
            logger.info(self.df_cds.dtypes)
            self.db_handler_utils.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", e.error_message, str(e))
            raise PlataformError(e.error_message)
        
        try:
            self.db_handler_pub = DbHandler(self.url_publish_otc, self.valuation_date)
            self.db_handler_pub.connect_db()
            if self.cds in eurois:
                self.df_swapcc_daily = self.db_handler_pub.get_data_swapcc_inter_daily("SwapCC_EUROIS")
            elif self.cds in usdois:
                self.df_swapcc_daily = self.db_handler_pub.get_data_swapcc_inter_daily("SwapCC_USDOIS")
            elif self.cds in jpyois:
                self.df_swapcc_daily = self.db_handler_pub.get_data_swapcc_inter_daily("SwapCC_JPYOIS")
            self.db_handler_pub.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", e.error_message, str(e))
            raise PlataformError(e.error_message)

        
    def transform_data(self):
        """Orquesta la construcción de los Hazzard, Probabilidades de Default y Supervivencia"""
        try:
            date_format = '%Y-%m-%d'
            date_ini = dt.datetime.strptime(self.valuation_date,"%Y-%m-%d").date()
            business_day_calendar=[dt.datetime.strptime(x,date_format).date() for x in  self.df_calendar["dates_calendar"].astype(str)]
            tri_dates = DateUtils().tenor_sequence_dates(date_ini,"10Y", "3M", business_day_calendar, "spot_starting", "modified_following")
            cds_days_prob = np.array([x.days for x in (tri_dates-date_ini)])
            days_prob = np.append([1],(cds_days_prob-cds_days_prob[0])[1:])
            if self.cds == 'CO':
                self.hazards = hazard_objective(date_ini, self.df_cds, self.df_swapcc_daily.iloc[:,1].values, 0.5)
            else:
                self.hazards = hazard_objective(date_ini, self.df_cds, self.df_swapcc_daily.iloc[:,1].values)
            self.dp = sp_probability(self.hazards, days_prob)[["days","dp"]]# dataframe probabilidades default
            self.sp = sp_probability(self.hazards, days_prob)[["days","sp"]]# dataframe probabilidades supervivencia
            
            self.df_probabilities = pd.merge(self.dp, self.sp, on='days', how='inner')
    
            # Columnas adicionales para inserción en base de datos
            self.hazards.columns = ['days', 'rate']
            self.df_probabilities.columns = ['days', 'pd_value', 'ps_value']
            self.hazards["valuation_date"] = self.valuation_date
            self.hazards["id_precia"] = self.cds
            self.df_probabilities["id_precia"] = self.cds
            self.df_probabilities["valuation_date"] = self.valuation_date
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", e.error_message, str(e))
            raise PlataformError(e.error_message)


    def load_info(self):
        try:
            """Carga la información generado por la metología a base de datos"""
            self.db_loader_pub = DbHandler(self.url_publish_otc, self.valuation_date)
            self.db_loader_pub.connect_db()
            self.db_loader_pub.disable_previous_info("pub_otc_hazzard_rates", self.valuation_date, self.cds)
            self.db_loader_pub.insert_data_db(self.hazards, "pub_otc_hazzard_rates")
            self.db_loader_pub.disable_previous_info("pub_otc_probabilities", self.valuation_date, self.cds)
            self.db_loader_pub.insert_data_db(self.df_probabilities, "pub_otc_probabilities")
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", e.error_message, str(e))
            raise PlataformError(e.error_message)
            
            
    def run_process_lambdas(self):
        """Orquesta la ejecucion de las lambdas de Hazzard Local y generación de archivos"""
        lbd_file_param = get_params(["LAMBDA_FILE_GENERATOR"])
        lbd_file = lbd_file_param["LAMBDA_FILE_GENERATOR"]
        
        lbd_hzd_local_param = get_params(["LAMBDA_HZD_LOCAL"])
        lbd_hzd_local = lbd_hzd_local_param["LAMBDA_HZD_LOCAL"]
        
        payload = {
            "VALUATION_DATE": self.valuation_date,
            "HAZZARD": [self.cds]
        }
        payload_hzd_local = {
            "Records": [
                {
                  "Sns": {
                    "Message": "{'VALUATION_DATE': '%s', 'FILES_ID': 'SC'}" % self.valuation_date  
                  }
                }
              ]
            }
        if self.cds == 'CO':
            launch_lambda(lbd_hzd_local, payload_hzd_local)
        launch_lambda(lbd_file, payload)



def main():
    params_key = [
        "DB_SECRET",
        "HAZZARD",
        "VALUATION_DATE"
    ]

    params_glue = get_params(params_key)
    secret_db = params_glue["DB_SECRET"]
    key_secret_db = get_secret(secret_db)

    url_sources_otc = key_secret_db["conn_string_aurora_sources"]
    schema_precia_utils_otc = key_secret_db["schema_aurora_precia_utils"]
    url_db_precia_utils_otc = url_sources_otc+schema_precia_utils_otc

    url_process_otc = key_secret_db["conn_string_aurora_process"]
    schema_process_otc = key_secret_db["schema_aurora_process"]
    url_db_process_otc = url_process_otc+schema_process_otc

    url_publish_otc = key_secret_db["conn_string_aurora_publish"]
    schema_publish_otc = key_secret_db["schema_aurora_publish"]
    url_db_publish_aurora = url_publish_otc+schema_publish_otc

    etl =  ETL(url_db_publish_aurora, url_db_process_otc, url_db_precia_utils_otc)
    etl.extract_data()
    etl.transform_data()
    etl.load_info()
    etl.run_process_lambdas()
    update_report_process("Exitoso", "Proceso Finalizado", "")



if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    main() 