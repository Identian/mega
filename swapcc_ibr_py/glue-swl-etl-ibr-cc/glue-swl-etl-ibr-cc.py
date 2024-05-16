"""Módulo que se encarga de la curva SwapCC IBR, trae la información necesaria para la construccion de la curva,
se procesa la informacion con la metodologia, se genera la curva, se almacena en la base de datos y se crean los
archivos"""

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
from scipy import optimize
from sqlalchemy.pool import NullPool
import sqlalchemy as sa

# Librería para la Metodología
from Interpolation import Interpol
from DateUtils import DateUtils

pd.options.mode.chained_assignment = None

ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)

PARAMETER_STORE_REPORTS = "ps-otc-lambda-reports"
PARAMETER_STORE_S3 = "ps-s3-datanfs-write"


#---------------------------------------------------------------------------------
# CONFIGURACION DE LOGS
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


#---------------------------------------------------------------------------------
# OBTENER SECRETOS Y PARÁMETROS
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
        raise PlataformError(error_msg)
        
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
        raise PlataformError(error_msg) from sec_exc
    
#---------------------------------------------------------------------------------
# EJECUCION DE LAMBDAS
def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Args:
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
        raise PlataformError(raise_msg) from lbd_exc

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

    lambda_name = get_parameter_store(PARAMETER_STORE_REPORTS)
    curve_param = get_params(["SWAP_CURVE"])
    curve = curve_param["SWAP_CURVE"]
    valuation_date_param = get_params(["VALUATION_DATE"])
    valuation_date = valuation_date_param["VALUATION_DATE"]
    report_process = {
          "input_id": curve,
          "output_id":[curve],
          "process": "Derivados OTC",
          "product": "Swap_Local",
          "stage": "Metodologia",
          "status": "",
          "aws_resource": "glue-p-swl-etl-ibr-cc",
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
# CARGUE DE INFORMACION A S3
def upload_s3(bucket_name: str, df_file: pd.DataFrame, file_name: str, columns_file: list):
    """
    Sube el archivo al bucket de S3
    """
    route_datanfs = get_params(["ROUTE_DATANFS"])
    path_s3 = route_datanfs["ROUTE_DATANFS"]
    df_curve = df_file.to_csv(index=False, sep=" ", header=False, line_terminator='\r\n', columns=columns_file)
    s3 = aws_client('s3')
    try:
        s3.put_object(Body=df_curve, Bucket=bucket_name, Key=path_s3+file_name)
        logger.info(f"Se ha cargado el archivo {file_name} en el bucket {bucket_name} correctamente")
    except Exception as e:
        logger.error(f"Ocurrio un error en la carga del archivo del bucket {bucket_name}. ERROR: {e}")
        raise PlataformError("Hubo un error cargando el archivo en el S3") from e



#---------------------------------------------------------------------------------
# METODOLOGIA PARA LA CURVA SWAPCC IBR

class curve_function():
    """
    Clase que contiene las funciones relacionadas a la construcción de curvas interpoladas
    """
    def __init__(self, curve_nodes_tenors, curve_nodes_df, curve_tenors, interpolation_method):
        self.curve_nodes_tenors = curve_nodes_tenors
        self.curve_nodes_df = curve_nodes_df
        self.curve_tenors = curve_tenors
        self.interpolation_method = interpolation_method
    
    def curve_from_nominal_rates(self):
        """
        Curva interpolada sobre tasas nominales.
        Params:
            curve_nodes_tenors (numpy.ndarray): días de los nodos para interpolar
            curve_nodes_df (numpy.ndarray): factores de descuento de los tenores para interpolar
            curve_tenors (numpy.ndarray): valor de los días a interpolar
            interpolation_method: metodo de interpolación
        Return:
            curve (numpy.ndarray): Curva interpolada
        """
        
        curve_nodes_rates = (1/self.curve_nodes_df-1)*360/self.curve_nodes_tenors
        rates_curve = Interpol(self.curve_nodes_tenors, curve_nodes_rates, self.curve_tenors).method(self.interpolation_method)
        curve = 1/(1+rates_curve*self.curve_tenors/360)
        return(curve)
    
    def curve_from_discount_factors(self):
        """
        Curva interpolada sobre factores de descuento.
        Params:
            curve_nodes_tenors (numpy.ndarray): días de los nodos para interpolar
            curve_nodes_df (numpy.ndarray): factores de descuento de los tenores para interpolar
            curve_tenors (numpy.ndarray): valor de los días a interpolar
            interpolation_method: metodo de interpolación
        Return:
            curve (numpy.ndarray): Curva interpolada
        """
        x_nodes = np.append(np.array([0]),self.curve_nodes_tenors)
        y_nodes = np.append(np.array([1]),self.curve_nodes_df)
        df_curve = Interpol(x_nodes,y_nodes, self.curve_tenors).method(self.interpolation_method)
        curve = df_curve
        return(curve)
    def curve_from_log_discount_factors(self):
        """
        Curva interpolada sobre log factores de descuento.
        Params:
            curve_nodes_tenors (numpy.ndarray): días de los nodos para interpolar
            curve_nodes_df (numpy.ndarray): factores de descuento de los tenores para interpolar
            curve_tenors (numpy.ndarray): valor de los días a interpolar
            interpolation_method: metodo de interpolación
        Return:
            curve (numpy.ndarray): Curva interpolada
        """
        curve_nodes_logdf = np.log(self.curve_nodes_df)
        x_nodes = np.append(np.array([0]),self.curve_nodes_tenors)
        y_nodes = np.append(np.array([0]),curve_nodes_logdf)
        log_df_curve = Interpol(x_nodes,y_nodes, self.curve_tenors).method(self.interpolation_method)
        curve = np.exp(log_df_curve)
        return(curve)
    
    def get_curve(self, interpolation_nodes):
        """
        Calcula la curva interpolada dado el tipo de nodo ha interpolar
        Params:
        interpolation_nodes (str): nominal_rates, discount_factors, log_discount_factors 
        Returns
        case : Función de interpolación sobre los nodos respectivos

        """
        switcher = {
            'nominal_rates': self.curve_from_nominal_rates,
            'discount_factors': self.curve_from_discount_factors,
            'log_discount_factors': self.curve_from_log_discount_factors,
        }
        case = switcher.get(interpolation_nodes, 'error')
        if (case == 'error'):
            msg = ' interpolation_nodes invalido %s' % interpolation_nodes
            raise ValueError(msg)
        else:
            return case()
        
class swap_functions():
    """
    Clase que contiene las funciones relacionadas a la construcción de curvas swaps
    """
    def __init__(self, swaps_characteristics, curves_characteristics, interest_rates_characteristics,calendar,logger):
        self.swaps_characteristics = swaps_characteristics
        self.curves_characteristics=curves_characteristics
        self.interest_rates_characteristics = interest_rates_characteristics
        self.calendar = calendar
        self.logger = logger
    
    # -----Attributes-----

    _date_format = '%Y-%m-%d'
    _bootstrap_error_message = 'Fallo el calculo del error bootstrap'
    _bootstrap_error_raise_message ="No se pudo calcular el error bootstrap para la curva con los insumos dados"
    _flows_log_message = 'Generacion de fechas y flujos de los swaps empleados'
    
    #--------------------
    
    def ibr_bootstrap(self, variable_nodes_values, nodes_days, fixed_nodes_values, swap_table_bullet, swap_table, efective_days, interpolation_method, interpolation_nodes):
        """
        Calcula el error bootstrap para la curva swap ois apartir de los swap plain vanilla  
        Params:
            variable_nodes_values (numpy.ndarray): Factores de descuento de cada nodo
            nodes_days (numpy.ndarray): Días correspondientes a los nodos de la curva
            fixed_nodes_values (numpy.ndarray): Factor de descuento ON
            swap_table (pd.DataFrame): Tabla que contiene la información relevante de cada swap (tenor, tasa par, dias del cupon, tiempo entre cupones) 
            interpolation_method (str): Metodo de interpolación utilizado (linear_interpol, cubic_splines_interpol)
            interpolation_nodes (str): Valores sobre los que se interpola (nominal_rates, discount_factors, log_discount_factors)
        Return:
            error (float): Suma de errores al cuadrado
        """
        try:
            nodes_values = np.append(np.array([fixed_nodes_values]),variable_nodes_values)
            swap_table_bullet["ois_curve"] = curve_function(nodes_days, nodes_values, swap_table_bullet["swap_days"].values, interpolation_method).get_curve(interpolation_nodes)
            swap_table["ois_curve"] = curve_function(nodes_days, nodes_values, swap_table["swap_days"].values, interpolation_method).get_curve(interpolation_nodes)
            
            swap_table_bullet["fra_rate_value"]=(swap_table_bullet["ois_curve"].shift(1).replace(np.nan,1)/swap_table_bullet["ois_curve"])-1
            float_leg_bullet=(swap_table_bullet["fra_rate_value"]*swap_table_bullet["ois_curve"])[swap_table_bullet["last_payment"]]
            fix_leg_bullet=(swap_table_bullet["coupon_time"]*swap_table_bullet["ois_curve"]*swap_table_bullet["swap_rate"])[swap_table_bullet["last_payment"]]
            swap_out_bullet=pd.DataFrame({"tenor":swap_table_bullet["swap_tenor"][swap_table_bullet["last_payment"]],"fix_leg":fix_leg_bullet,"float_leg":float_leg_bullet}).set_index("tenor")
                        
            swap_table["fra_rate_value"]=(swap_table["ois_curve"].shift(1).replace(np.nan,1)/swap_table["ois_curve"])-1
            float_leg=(swap_table["fra_rate_value"]*swap_table["ois_curve"]).cumsum()[swap_table["last_payment"]]
            fix_leg=((swap_table["coupon_time"]*swap_table["ois_curve"]).cumsum()[swap_table["last_payment"]])*swap_table["swap_rate"][swap_table["last_payment"]]
            swap_out=pd.DataFrame({"tenor":swap_table["swap_tenor"][swap_table["last_payment"]],"fix_leg":fix_leg,"float_leg":float_leg}).set_index("tenor")
            swaps = pd.concat([swap_out_bullet,swap_out])
            bootstrap_error = swaps["fix_leg"] - swaps["float_leg"]
            return(np.dot(bootstrap_error,bootstrap_error))
        except(Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)    
    

    def ibr_curve(self, swap_curve, trade_date, swaps_info, discount_curve=None, curve_in_t2=False):
        """
        Construye la curva ois cero cupon a partir de información de swaps plain vanilla

        Params
        ----------
        swap_curve (str): Nombre de la curva swap ha construir.
        trade_date (datetime.date): Fecha de valoración.
        swaps_info (pd.dataframe): Información de los swaps utiizados en la construcción de la curva cero
                                   tenor:  Tenores tenidos en cuenta de los swaps, incluido el tenor overnight.
                                   rate: Tasas par de los swaps, incluida la tasa overnight.
        discount_curve (numpy.ndarray) : Curva de factores de descuento para los flujos del swap (Opcional), None.
        curve_in_t2 (Boolean): La curva esta en t+2 (Opcional) False.

        Returns
        ----------
        swap_curve_nodes (pd.dataframe): Curva swap cero cupon nodos
                                   days:  Dias de la curva.
                                   rate: Tasas cero cupon.
        swap_curve_daily (pd.dataframe): Curva swap cero cupon diaria
                                   days:  Dias de la curva.
                                   rate: Tasas cero cupon.

        """
        try:
            self.logger.info(create_log_msg(f'Inicia la construccion de la curva cero cupon {swap_curve}'))
            #Curve Characteristics
            curve_characteristics = self.curves_characteristics.loc[np.where(self.curves_characteristics["curve_name"]==swap_curve)].reset_index(drop=True)
            interpolation_method = curve_characteristics["interpolation_method"][0]
            interpolation_nodes = curve_characteristics["interpolation_nodes"][0]
            #Swap Characteristics
            swap_characteristics = self.swaps_characteristics.loc[np.where(self.swaps_characteristics["swap_curve"]==swap_curve)].reset_index(drop=True)
            swap_business_day_convention = swap_characteristics["business_day_convention"][0]
            swap_starting_type_convention = swap_characteristics["start_type"][0]        
            swap_business_day_calendar=[dt.datetime.strptime(x,self._date_format).date() for x in self.calendar]
            swap_daycount_convention = swap_characteristics["daycount_convention"][0]
            bullet_tenors = swap_characteristics["bullet_tenors"][0].split(',')
            frequency_payment = swap_characteristics["buyer_leg_frequency_payment"][0]
    
            #ON Rate Characteristics
            interest_rate_characteristics = self.interest_rates_characteristics.loc[np.where(self.interest_rates_characteristics["interest_rate_name"]==swap_characteristics["buyer_leg_rate"][0])].reset_index(drop=True)
            on_business_day_convention = interest_rate_characteristics["business_day_convention"][0]
            on_tenor = interest_rate_characteristics["tenor"][0]
            on_starting_type_convention = interest_rate_characteristics["start_type"][0]
            on_business_day_calendar=[dt.datetime.strptime(x,self._date_format).date() for x in self.calendar]
            on_daycount_convention = interest_rate_characteristics["daycount_convention"][0]
    
            #ON handling
            on_rate = swaps_info.loc[swaps_info["tenor"]=='ON',"rate"][0]
            logger.info(swaps_info.to_string())
            on_rate = float(on_rate)
            on_date = DateUtils().tenor_date(trade_date, on_tenor, on_business_day_calendar, on_starting_type_convention, on_business_day_convention)
            if curve_in_t2: on_date = trade_date + 1 # Adjustment to curve in t+2, on_day is always 1 day
            on_day = 1
            on_value = 1/(1+on_rate/360)
    
            #Swap handling
            swap_tenors = swaps_info.loc[np.where(np.logical_not(swaps_info["tenor"]=='ON')),"tenor"].values
            swap_rates = swaps_info.loc[np.where(np.logical_not(swaps_info["tenor"]=='ON')),"rate"].values
            effective_date = DateUtils().starting_date(trade_date, swap_business_day_calendar,swap_starting_type_convention)
            effective_days = (effective_date-trade_date).days
            swap_tenor_dates=[DateUtils().tenor_date(
                            trade_date, x, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention) for x in swap_tenors]
    
            swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-trade_date)]
            if curve_in_t2: swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-effective_date)] # Adjustment to curve in t+2
            
            self.logger.info(create_log_msg(self._flows_log_message))
            #Cashflow handling
            swap_table = pd.DataFrame()
            swap_table_bullet = pd.DataFrame()
            swap_last_dates=np.array([])
            last_payment_date=None
    
            for swap_tenor in swap_tenors[np.where(np.isin(swap_tenors,bullet_tenors))[0]]:
                #Looping though swap tenors
                swap_rate = swap_rates[np.where(swap_tenors==swap_tenor)][0]
                
                frequency = swap_tenor
                swap_dates = DateUtils().tenor_sequence_dates(trade_date, swap_tenor, frequency, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention)
                swap_dates = swap_dates[np.where(np.logical_not(pd.Series(swap_dates).isin(swap_last_dates)))]
                swap_days = [x.days for x in (swap_dates-trade_date)]
                coupon_time = coupon_time = [DateUtils().day_count(trade_date,swap_dates[0],swap_daycount_convention)]+[DateUtils().day_count(swap_dates[i-1],swap_dates[i],swap_daycount_convention) for i in range(1,len(swap_dates))]
                last_payment=np.append(np.repeat(False, len(coupon_time)-1),True)
                
                swap_table_bullet = pd.concat([swap_table_bullet, pd.DataFrame({"swap_tenor":swap_tenor, "swap_rate":swap_rate, "swap_days":pd.Series(swap_days), "coupon_time":pd.Series(coupon_time),"last_payment":last_payment})])
            swap_table_bullet.reset_index(drop=True,inplace=True)
            swap_last_dates=np.array([effective_date])
            swaps_dates = DateUtils().tenor_sequence_dates(trade_date, swap_tenors[-1], frequency_payment, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention)
            for swap_tenor in swap_tenors[np.where(np.logical_not(np.isin(swap_tenors,bullet_tenors)))[0]]:
                frequency = frequency_payment
                tenor_specs = DateUtils().tenor_specs(swap_tenor)
                frequency_specs = DateUtils().tenor_specs(frequency)
                if (tenor_specs["period"]== frequency_specs["period"]):
                    tenor_count =  int(tenor_specs["length"]/frequency_specs["length"])
                elif (tenor_specs["period"]== "Y" and frequency_specs["period"] == "M"):
                    tenor_count =  int(tenor_specs["length"]*12/frequency_specs["length"])
                swap_dates = swaps_dates[:tenor_count+1]
                swap_dates = swap_dates[np.where(np.logical_not(pd.Series(swap_dates).isin(swap_last_dates)))]
                swap_rate = swap_rates[np.where(swap_tenors==swap_tenor)][0]
                swap_last_dates=np.append(swap_last_dates,swap_dates)
                swap_days = [x.days for x in (swap_dates-trade_date)]
                if swap_table.empty:
                    default_date=trade_date
                else:
                    default_date=last_payment_date
                coupon_time = [DateUtils().day_count(default_date,swap_dates[0],swap_daycount_convention)]+[DateUtils().day_count(swap_dates[i-1],swap_dates[i],swap_daycount_convention) for i in range(1,len(swap_dates))]
                last_payment_date=swap_dates[-1]
                last_payment=np.append(np.repeat(False, len(coupon_time)-1),True)
                swap_table = pd.concat([swap_table, pd.DataFrame({"swap_tenor":swap_tenor, "swap_rate":swap_rate, "swap_days":pd.Series(swap_days), "coupon_time":pd.Series(coupon_time),"last_payment":last_payment})])
                
            swap_table.reset_index(drop=True,inplace=True)

            self.logger.info(create_log_msg(f'Inicia el proceso de optimizacion de la curva {swap_curve}'))
            nodes_days = [on_day]+ swap_tenor_days
            fixed_nodes_values = on_value
            variable_nodes_values = 1/(1+swap_rates*swap_tenor_days/360)
            bnds=[(0,None)]*len(variable_nodes_values)
            swap_nodes_values = optimize.minimize(self.ibr_bootstrap,variable_nodes_values,bounds=bnds,args=(nodes_days, fixed_nodes_values, swap_table_bullet, swap_table, effective_days, interpolation_method, interpolation_nodes),tol=1e-16).x
            self.logger.info(create_log_msg(f'Finalizo el proceso de optimizacion de la curva {swap_curve}'))
            nodes_values = np.append(np.array(on_value), swap_nodes_values)
            if curve_in_t2:
                nodes_values = np.append(np.array([on_value]),swap_nodes_values * on_value**(effective_date-trade_date).days)
                nodes_days = np.append(np.array(nodes_days[0]), np.array(nodes_days[1:]) + (effective_date-trade_date).days)
    
            ois_curve = curve_function(nodes_days, nodes_values, np.arange(1,max(nodes_days)+1), interpolation_method).get_curve(interpolation_nodes)
            swap_curve_daily=pd.DataFrame({"days":np.arange(1,len(ois_curve)+1),"rate":(1 / ois_curve - 1) * 360 / np.arange(1,len(ois_curve)+1)})
            swap_curve_nodes=swap_curve_daily.iloc[np.array(nodes_days)-1,:]
            self.logger.info(create_log_msg(f'Finalizo la construccion de la curva cero cupon {swap_curve}'))
            return swap_curve_nodes,swap_curve_daily
        except(Exception,):
            self.logger.error(create_log_msg(f'Se genero un error en la construcción de la curva cero cupon {swap_curve}'))
            raise PlataformError(f"Hubo un error en en la construcción de la curva cero cupon {swap_curve}")
        
#---------------------------------------------------------------------------------
# EXTRACCIÓN DE INFORMACIÓN DE BASES DE DATOS
class DbHandler:
    """Representa la extracción e inserción de información de las bases de datos"""
    def __init__(self, url_db: str, valuation_date: str) -> None:
        self.sql_engine = sa.create_engine(url_db, poolclass=NullPool)
        self.valuation_date = valuation_date
        self.url_db = url_db


    def get_data_calendars(self, calendar_swap: list):
        """Trae la información de calendarios"""
        query_calendars = (
            "SELECT dates_calendar "
            + "FROM precia_utils_calendars "
            + "WHERE "+calendar_swap+" = 1 "
        )
        error_message = "No se pudo traer la info de los calendarios"
        try:
            with self.sql_engine.connect() as conn:
                df_calendars = pd.read_sql(query_calendars, conn)
            logger.info("Se obtuvo la información de los calendarios")
            return df_calendars
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible extraer la informacion de los calendarios: "+ str(e))
    

    def get_data_local_sample(self, id_instrument: str):
        """Trae la información de la muestra IBR"""
        query_local_sample = sa.sql.text("""
            SELECT tenor, days, mid FROM prc_otc_swp_local
            WHERE id_instrument = :id_instrument
            AND valuation_date = :valuation_date
        """)
        query_params = {
            "id_instrument": id_instrument,
            "valuation_date": self.valuation_date
        }
        error_message = "No fue posible extraer la información del la muestra local"
        try:
            with self.sql_engine.connect() as conn:
                df_local_sample = pd.read_sql(query_local_sample, conn, params=query_params)
            logger.info("Se obtuvo la información de la muestra local")
            if df_local_sample.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos de la muestra IBR: {self.valuation_date}")
            return df_local_sample
        except Exception as e:
            logger.error(create_log_msg(error_message+":IBR"))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible extraer la información de la muestra IBR: "+ str(e))


    def get_data_ibron_rate(self, name_rate: str):
        """Trae la información de la IBRON"""
        query_ibron = sa.sql.text("""
            SELECT value_rates as mid FROM src_indicators_republic_bank
            WHERE name_rate = :name_rate
            AND valuation_date = :valuation_date
            AND rate_type = :rate_type
            AND status_info = :status_info
        """)
        query_params = {
            "name_rate": name_rate,
            "rate_type": "Nominal",
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No fue posible extraer la información del Swap"
        try:
            with self.sql_engine.connect() as conn:
                df_ibron = pd.read_sql(query_ibron, conn, params=query_params)
            logger.info("Se obtuvo la información de la informacion de la IBRON")
            if df_ibron.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos de la IBRON: {self.valuation_date}")
            return df_ibron
        except Exception as e:
            logger.error(create_log_msg(error_message+": IBRON"))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible extraer la información de la IBRON: "+ str(e))
        

    def get_data_curves_characteristics(self, swap_curve):
        """Trae las características de interpolación de la curva"""

        query_curves_characteristics = sa.sql.text(
            """
            SELECT curve_name, interpolation_method, interpolation_nodes 
            FROM precia_utils_swi_curves_characteristics
            WHERE curve_name = :curve_name
            """
        )
        query_params = {"curve_name": swap_curve}

        try:
            with self.sql_engine.connect() as conn:
                df_curve_charac = pd.read_sql(query_curves_characteristics, conn, params=query_params)
            logger.info(f"Se obtuvo la info de las caracterísiticas de interpolación para curva {swap_curve}")
            return df_curve_charac
        except (Exception,) as e:
            logger.error(create_log_msg("Fallo en obtener la información de interpolacion"))
            update_report_process("Fallido", "Error en obtener data", str(e))
            raise PlataformError("No fue posible extraer la información de la IBRON: "+ str(e))


    def get_data_swap_characteristics(self, swap_curve):
        """Trae las características de la curva swap"""

        query_swp_charac = sa.sql.text(
            """
            SELECT swap_curve, swap_name, daycount_convention, buyer_leg_rate, 
            seller_leg_rate, buyer_leg_currency, seller_leg_currency, 
            buyer_leg_frequency_payment, seller_leg_frequency_payment,
            start_type, on_the_run_tenors, bullet_tenors, business_day_convention, 
            business_day_calendar, buyer_leg_projection_curve, 
            seller_leg_projection_curve, buyer_leg_discount_curve, 
            seller_leg_discount_curve, colateral
            FROM precia_utils_swi_swap_characteristics
            WHERE swap_curve = :swap_curve
            """
        )
        query_params = {"swap_curve": swap_curve}

        try:
            with self.sql_engine.connect() as conn:
                df_swap_charac = pd.read_sql(query_swp_charac, conn, params=query_params)
            logger.info(f"Se obtuvo la info de las caracterísiticas de la curva {swap_curve}")
            return df_swap_charac            
        except (Exception,) as e:
            logger.error(create_log_msg("Hubo un error en la obtencion de informacion"))
            update_report_process("Fallido", "Fallo la obtencion de las caracteristicas de la curva", str(e))
            raise


    def get_data_interest_rates_characteristics(self):
        """Trae las caracteristicas generales de las tasas de interes"""

        query_rates_charac = sa.sql.text(
            """
            SELECT interest_rate_name, daycount_convention, currency,
            tenor, start_type, business_day_convention, business_day_calendar 
            FROM precia_utils_swi_interest_rates_characteristics
            """
        )
        error_message = "Fallo la extracción de información de las caracteristicas generales de las tasas de interes"
        try:
            with self.sql_engine.connect() as conn:
                df_interest_rates = pd.read_sql(query_rates_charac, conn)
            logger.info("Se obtuvo la información de las caracteristicas generales de las tasas de interes en bd")
            return df_interest_rates     
        except (Exception,) as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise


    def disable_previous_info(self, pub_table: str, valuation_date: str, curve: str):
        """Actualiza el status de la información se encuentra en la base de datos
        pasando de 1 a 0
        """
        error_message = "Falló la actualización del estado de la información"
        try:
            update_query = (
                "UPDATE "+ pub_table + ""
                + " SET status_info= 0"
                + " WHERE curve = '"+ curve + "'"
                + " AND valuation_date = '"+ valuation_date + "'"
            )
            with self.sql_engine.connect() as conn:
                conn.execute(update_query)

            logger.info("Se ha actualizado el estado de la informacion correctamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible actualizar el estado de la informacion en base de datos: ", str(e))
        

    def insert_data_db(self, df_insert: pd.DataFrame, pub_table: str):
        """Inserta la informacion calculada durante el proceso"""
        error_message = "Falló la inserción de la información en la base de datos"
        try:
            with self.sql_engine.connect() as conn:
                df_insert.to_sql(pub_table, con=conn, if_exists="append", index=False)
            logger.info("Finaliza la inserción a la base de datos exitosamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible insertar la información en la base de datos: "+ str(e))
        

class ETL:
    """Representa la orquestación de la ETL"""
    def __init__(self, url_db_process, url_db_utils, url_db_sirius, url_db_publish, valuation_date, swap_curve) -> None:
        self.url_db_utils = url_db_utils
        self.url_db_process = url_db_process
        self.url_db_sirius = url_db_sirius
        self.url_db_publish = url_db_publish
        self.swap_curve = swap_curve
        self.valuation_date = valuation_date

    def extract_data(self):
        """Orquesta la extracción de la informacion de la clase ExtratorDataDb"""
        logger.info("Comienza la extraccion de informacion de base de datos...")
        try:
            self.db_handler_utils = DbHandler(self.url_db_utils, self.valuation_date)
            self.general_calendar_df = self.db_handler_utils.get_data_calendars("bvc_calendar")
            self.df_curve_charac = self.db_handler_utils.get_data_curves_characteristics(self.swap_curve)
            self.df_interest_rates = self.db_handler_utils.get_data_interest_rates_characteristics()
            self.df_swp_charac = self.db_handler_utils.get_data_swap_characteristics(self.swap_curve)
            self.db_handler_process = DbHandler(self.url_db_process, self.valuation_date)
            self.df_swp = self.db_handler_process.get_data_local_sample("IBR")
            self.db_handler_sirius = DbHandler(self.url_db_sirius, self.valuation_date)
            self.df_ibron_rate = self.db_handler_sirius.get_data_ibron_rate("IBRON")
            logger.info("Finaliza la extraccion de informacion")
        except Exception as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", "Fallo la extracción de los insumos de las bases de datos", str(e))
            raise PlataformError(e.error_message)
        

    def transform_data(self):
        """Orquesta la construccion de las curvas"""
        logger.info(self.df_ibron_rate.to_string())
        self.df_ibron_rate['tenor'] = ["ON"]
        self.df_ibron_rate['days'] = [1]
        self.df_ibron_rate['mid'] = self.df_ibron_rate["mid"]/100

        # Agrega la informacion de la IBRON a la muestra
        self.df_swp = pd.concat([self.df_swp, self.df_ibron_rate], ignore_index=True)
        self.df_swp = self.df_swp.sort_values(by=['days'])

        logger.info(f"Resultado de la muestra con la ibron {self.df_swp.to_string()}")
        
        # Ejecucion de la metodologia
        self.general_calendar_df["dates_calendar"] = self.general_calendar_df["dates_calendar"].astype(str)
        logger.info(self.general_calendar_df["dates_calendar"])
        logger.info(self.general_calendar_df["dates_calendar"].dtype)
        trade_date = dt.datetime.strptime(self.valuation_date, '%Y-%m-%d').date()
        self.df_swp=pd.concat([pd.DataFrame({"tenor":["ON"],"days_par":[self.df_ibron_rate.iloc[0,0]],"mid":[self.df_ibron_rate.iloc[0,1]]}),self.df_swp])
        self.df_swp.rename(columns={"mid": "rate"}, inplace=True)
        self.df_swp.reset_index(inplace=True,drop=True)
        swap_class=swap_functions(self.df_swp_charac, self.df_curve_charac, self.df_interest_rates, self.general_calendar_df["dates_calendar"], logger)
        self.df_swapcc_nodes, self.df_swapcc_daily = swap_class.ibr_curve(self.swap_curve, trade_date, self.df_swp[["tenor", "rate"]])

        # Completa la informacion de la curva para la base de datos
        self.df_swapcc_daily["valuation_date"] = self.valuation_date
        self.df_swapcc_daily["curve"] = self.swap_curve
        self.df_swapcc_nodes["valuation_date"] = self.valuation_date
        self.df_swapcc_nodes["curve"] = self.swap_curve
        self.df_swapcc_daily.at[0, 'rate'] = self.df_ibron_rate.at[0, 'mid']
        self.df_swapcc_nodes.at[0, 'rate'] = self.df_ibron_rate.at[0, 'mid']


    def load_data(self):
        """Orquesta la carga de la informacion a la base de datos y la generación de los archivos"""
        error_message = "Fallo la carga de la informacion"
        nodos_file_name = self.swap_curve+"_Nodos_fecha.txt".replace("fecha", self.valuation_date.replace("-", ""))
        daily_file_name = self.swap_curve+"_Diaria_fecha.txt".replace("fecha", self.valuation_date.replace("-", ""))
        bucket_name_datanfs = get_parameter_store(PARAMETER_STORE_S3)
        
        upload_s3(bucket_name_datanfs, self.df_swapcc_nodes, nodos_file_name, ["days", "rate"])
        upload_s3(bucket_name_datanfs, self.df_swapcc_daily, daily_file_name, ["days", "rate"])

        
        try:
            logger.info("Comienza la carga de informacion a la base de datos...")
            self.db_handler_publish = DbHandler(self.url_db_publish, self.valuation_date)
            self.db_handler_publish.disable_previous_info("pub_otc_swl_cc_daily", self.valuation_date, self.swap_curve)
            self.db_handler_publish.insert_data_db(self.df_swapcc_daily, "pub_otc_swl_cc_daily")
            self.db_handler_publish.disable_previous_info("pub_otc_swl_cc_nodes", self.valuation_date, self.swap_curve)
            self.db_handler_publish.insert_data_db(self.df_swapcc_nodes, "pub_otc_swl_cc_nodes")
        except Exception as e:
            logger.error(create_log_msg(e.error_message))
            update_report_process("Fallido", "Fallo la extracción de los insumos de las bases de datos", str(e))
            raise PlataformError(e.error_message)
        update_report_process("Exitoso", "Proceso Finalizado", "")


def main():
    params_key = [
        "DB_SECRET",
        "SWAP_CURVE",
        "VALUATION_DATE"
    ]

    params_glue = get_params(params_key)
    valuation_date = params_glue["VALUATION_DATE"]
    
    secret_db = params_glue["DB_SECRET"]
    key_secret_db = get_secret(secret_db)

    url_sources = key_secret_db["conn_string_sources"]
    schema_utils = key_secret_db["schema_utils"]
    url_db_utils = url_sources+schema_utils

    url_process = key_secret_db["conn_string_process"]
    schema_process = key_secret_db["schema_process"]
    url_db_process = url_process+schema_process

    url_publsih = key_secret_db["conn_string_publish"]
    schema_publish = key_secret_db["schema_publish"]
    url_db_publish = url_publsih+schema_publish

    url_publish_rates = key_secret_db["conn_string_sirius"]
    schema_publish_indicators = key_secret_db["schema_sirius_indicators"]
    url_db_publish_sirius = url_publish_rates+schema_publish_indicators

    swap_curve = params_glue["SWAP_CURVE"]

    etl = ETL(url_db_process, url_db_utils, url_db_publish_sirius, url_db_publish, valuation_date, swap_curve)
    etl.extract_data()
    etl.transform_data()
    etl.load_data()


if __name__ == "__main__":
    logger = setup_logging(logging.INFO)
    main() 