"""
=============================================================

Nombre: glue-job-dev-process-otc-opt-inter.py
Tipo: Glue Job

Autores:
    - Lorena Julieth Torres Hernández
Tecnología - Precia

    --Metodologia
    - Sebastian Velez Hernandez
Investigacion y desarrollo - Precia



Ultima modificación: 16/11/2023

En el presente script se realiza la creacion de la curva de 
SwapCC_UFcamara, la generación de archivos con la información 
de diaria y nodos y su almacenamiento en el SFTP de Precia y 
asi mismo su almacenamiento en la base de datos de publish.
=============================================================
"""
#Nativas de Python
import logging
import json
from datetime import datetime
import datetime as dt
import sys
from dateutil import relativedelta as rd
from sys import exc_info, stdout
import base64

#AWS
import boto3
from awsglue.utils import getResolvedOptions 

#De terceros
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
import paramiko
import numpy as np
from scipy import optimize
import warnings

#Personalizadas
from Interpolation import Interpol
from DateUtils import DateUtils

pd.options.mode.chained_assignment = None

##### Inicio de código Metodologia

"""
Created on Fri Oct 27 15:25:40 2023

@author: SebastianVelezHernan
"""

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


class forward_functions():
    """
    Clase que contiene las funciones relacionadas a futuros y forwards
    """    
    # -----Attributes-----
    # Class attributes
    _future_letter_conventions = {
        1:'F',
        2:'G',
        3:'H',
        4:'J',
        5:'K',
        6:'M',
        7:'N',
        8:'Q',
        9:'U',
        10:'V',
        11:'X',
        12:'Z'}
    #--------------------
    def __init__(self, logger):
        self.logger=logger
        
    def calculata_future_letters(self, future_date):
        month_future=future_date.month
        letter=self._future_letter_conventions[month_future]
        year=str(future_date.year)[-1]
        return letter+year
    
    def fra_rate(self,df_curve,coupon_time):
        """
        Calcula las tasas FRA dados unos factores de descuento y dias anualizados entre estos

        Parameters
        ----------
        df_curve (np.array): Vector con los factores de descuento a utilizar para el calculo de las tasas FRA
        coupon_time (np.array): vector con los días anualizados entre cupones para el calculo de las tasas FRA

        Returns
        fra (np.array): Vector con las tasas FRA calculadas

        """
        try:
            df_coupon = df_curve[np.where(np.logical_not(np.isnan(df_curve)))]
            delta_coupon = 1/ coupon_time
            fra = ((np.append(np.array([1]), df_coupon[:-1])/df_coupon)-1) * delta_coupon
            return fra[np.where(np.logical_not(np.isnan(fra)))]
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en el calculo de las tasas FRA'))
            raise PlataformError("Hubo un error en el calculo de las tasas FRA")
    def fra_rate_on(self, df_curve, coupon_time, tenor = "xx", first_coupon = "zz", 
                         index_acum = None):
        """
        Calcula la tasa FRA dado unos factores de descuento, haciendo uso de la composición de la 
        tasa overnigth al tenor aplicable

        Parameters
        ----------
        df_curve (np.array): Vector factores de descuento a utilizar en el calculo de la tasa FRA
        coupon_time (np.array): Vector días anualizados a utilizar en el calculo de la tasa FRA
        tenor (str): Tenor para el cual se calcula la tasa FRA
        first_coupon (str):Primer tenor en el cual se utiliza la información de la tasa overnight
        index_acum (float): Valor acumulado de la tasa overnight

        Returns
        -------
        fra (float): Tasa FRA al plazo determinado teniendo en cuenta la tasa overnigth
        """
        try:
                
            df_coupon = df_curve
            delta_coupon = 1/ coupon_time[np.where(np.logical_not(np.isnan(coupon_time)))]
            if tenor == first_coupon:
                on_acum = index_acum
                df_curve[0] = 1
            else:
                on_acum = 1
            fra = (on_acum * df_coupon[0]/df_coupon[1] - 1) * delta_coupon
            return fra[0]
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en el calculo de la tasa FRA, dada la tasa on acumulada'))
            raise PlataformError("Hubo un error en el calculo de la tasa FRA, dada la tasa on acumulada")
            
    def calculate_future_dates_vto(self, trade_date, fut_months_term, business_day_convention, 
                                       fut_number, calendar, prior_day_adjust = None):
        try:        
            fut_months_term = int(fut_months_term.replace("M",""))
            first_fut_date_year = dt.datetime(trade_date.year, fut_months_term*int(trade_date.month/fut_months_term) ,1).date()
            posible_fut_dates = [first_fut_date_year + rd.relativedelta(months=3*i) for i in range(fut_number+1)]
            fut_dates = [DateUtils().month_third_wednesday(x) for x in posible_fut_dates]
            fut_dates = np.array([DateUtils().following_business_day_convention(fut_date,calendar) for fut_date in fut_dates])
            next_fut_dates = fut_dates[np.where(fut_dates>trade_date)]
            return next_fut_dates
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la creacion de las fechas de vencimiento de los futuros SOFR 3M'))
            raise PlataformError("Hubo un error en la creacion de las fechas de vencimiento de los futuros SOFR 3M")
 

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
    
    def ufcamara_bootstrap(self, variable_nodes_values, nodes_days, fixed_nodes_values, swap_table, interpolation_method, interpolation_nodes):
        """
        Calcula el error bootstrap para la curva UFCAMARA a partir de los swap tasa fija UF-CAMARA   
        Params:
            variable_nodes_values (numpy.ndarray): Factores de descuento de cada nodo
            nodes_days (numpy.ndarray): Días correspondientes a los nodos de la curva
            fixed_nodes_values (numpy.ndarray): Factores de descuento provenientes de los forward
            swap_table (pd.DataFrame): Tabla que contiene la información relevante de cada swap (tenor, tasa par, dias del cupon, tiempo entre cupones) 
            interpolation_method (str): Metodo de interpolación utilizado (linear_interpol, cubic_splines_interpol)
            interpolation_nodes (str): Valores sobre los que se interpola (nominal_rates, discount_factors, log_discount_factors)
        Return:
            error (float): Suma de errores al cuadrado
        """
        try:
            nodes_values = np.append(fixed_nodes_values,variable_nodes_values)
            swap_table["local_discount_curve"] = curve_function(nodes_days, nodes_values, swap_table["swap_days"].values, interpolation_method).get_curve(interpolation_nodes)
            swap_table["fra_rate_value"]=(swap_table["foreing_projection_curve"].shift(1).replace(np.nan,1)/swap_table["foreing_projection_curve"])-1
            float_leg=(swap_table["fra_rate_value"]*swap_table["foreing_discount_curve"]).cumsum()[swap_table["last_payment"]]+swap_table["foreing_discount_curve"][swap_table["last_payment"]]
            fix_leg=((swap_table["coupon_time_seller"]*swap_table["local_discount_curve"]).cumsum()[swap_table["last_payment"]])*swap_table["swap_rate"][swap_table["last_payment"]]+swap_table["local_discount_curve"][swap_table["last_payment"]]
            swap_out=pd.DataFrame({"tenor":swap_table["swap_tenor"][swap_table["last_payment"]],"fix_leg":fix_leg,"float_leg":float_leg}).set_index("tenor")
                
            bootstrap_error = swap_out["fix_leg"] - swap_out["float_leg"]
            return(np.dot(bootstrap_error,bootstrap_error))
        except(Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)

    def ufcamara_curve(self, trade_date, swaps_info, fwds_info, uf_publish, spot, camara_curve, nodos_fwd):
        """
        Construye la curva cero cupon a partir de información de cross currency swaps y puntos forward

        Params
        ----------
        swap_curve (str): Nombre de la curva swap ha construir.
        trade_date (datetime.date): Fecha de valoración.
        swaps_info (pd.dataframe): Dataframe de dos columnas
                                   tenor:  Tenores tenidos en cuenta de los swaps.
                                   rate: Tasas par de los swaps.
        fwds_info (pd.dataframe): Dataframe con la curva diaria de tasas forward
                                   days:  Dias de la curva.
                                   rate: Tasa forward.
        uf_publish (pd.dataframe): Dataframe con la información de la UF publicada por el banco central de chile
                                   fecha:  fecha de la UF.
                                   uf: UF correspondiente a la fecha.
        spot (float): Tasa de cambio spot                                                                   
        camara_curve (numpy.ndarray) : Curva cero cupon CAMARA.
        nodos_fwd (numpy.ndarray): Nodos de la curva diaria de tasas fwd forward
        
        Returns
        ------------
        swap_curve_nodes (pd.dataframe): Curva swap cero cupon nodos
                                   days:  Dias de la curva.
                                   rate: Tasas cero cupon.
        swap_curve_daily (pd.dataframe): Curva swap cero cupon diaria
                                   days:  Dias de la curva.
                                   rate: Tasas cero cupon.


        """
        try:
            swap_curve="SwapCC_UFCAMARA"
            self.logger.info(create_log_msg('Inicia la construccion de la curva cero cupon Swap_UFCAMARA'))
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
            on_the_run_tenors=swap_characteristics["on_the_run_tenors"][0].split(',')
            frequency_payment_buyer = swap_characteristics["buyer_leg_frequency_payment"][0]
            frequency_payment_seller = swap_characteristics["seller_leg_frequency_payment"][0]
            eq_freq= frequency_payment_buyer==frequency_payment_seller
            
            #Forward handling
            fwds_info["fecha"]=fwds_info["tenor"].apply(lambda x: DateUtils().uf_forward_date(trade_date,x,swap_business_day_calendar)).values
            fwds_info["dias"]=(fwds_info["fecha"]-trade_date).dt.days
            uf_publish["valuation_date"]=pd.to_datetime(uf_publish["valuation_date"],format="%Y/%m/%d").dt.date
            uf_publish["dias"]=(uf_publish["valuation_date"]-trade_date).dt.days
            uf_publish_valid=uf_publish[uf_publish["dias"]>0]
            uf_forward=pd.DataFrame({"days":np.concatenate([uf_publish_valid["dias"],fwds_info["dias"]]),"rate":np.concatenate([uf_publish_valid.iloc[:,1],fwds_info.iloc[:,1]])})
            
            
            
            fwds_diarios=Interpol(uf_forward["days"].values,uf_forward["rate"].values,np.arange(1,np.max(fwds_info["dias"])+1)).method("linear_interpol")
            fwds_info_daily=pd.DataFrame({"days":np.arange(1,np.max(fwds_info["dias"])+1),"rate":fwds_diarios})
            #Swap handling
            swap_tenors = swaps_info.loc[np.isin(swaps_info["tenor"],on_the_run_tenors),"tenor"].values
            swap_rates = swaps_info.loc[np.isin(swaps_info["tenor"],on_the_run_tenors),"mid_price"].values
            swap_tenor_dates=[DateUtils().tenor_date(
                            trade_date, x, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention) for x in swap_tenors]

            swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-trade_date)]
            discount_curve_for=curve_function(np.arange(1,len(camara_curve)+1), camara_curve, np.arange(1,max(swap_tenor_days)+1), interpolation_method).get_curve(interpolation_nodes)
            projection_curve_for=curve_function(np.arange(1,len(camara_curve)+1), camara_curve, np.arange(1,max(swap_tenor_days)+1), interpolation_method).get_curve(interpolation_nodes)
             
            self.logger.info(create_log_msg(self._flows_log_message))
            #Cashflow handling
            swap_table = pd.DataFrame()
            swap_last_dates_buyer=np.array([])
            swap_last_dates_seller=np.array([])
            last_payment_date_buyer=None
            last_payment_date_seller=None

            for swap_tenor in swap_tenors:
                #Looping though swap tenors
                swap_rate = swap_rates[np.where(swap_tenors==swap_tenor)][0]
                
                frequency_buyer = swap_tenor if swap_tenor in bullet_tenors else frequency_payment_buyer
                frequency_seller = swap_tenor if swap_tenor in bullet_tenors else frequency_payment_seller
                if eq_freq:
                    swap_dates_buyer= swap_dates_seller = DateUtils().tenor_sequence_dates(trade_date, swap_tenor, frequency_buyer, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention)    
                else:
                    swap_dates_buyer = DateUtils().tenor_sequence_dates(trade_date, swap_tenor, frequency_buyer, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention)
                    swap_dates_seller = DateUtils().tenor_sequence_dates(trade_date, swap_tenor, frequency_seller, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention)
                if not swap_table.empty:
                    swap_dates_buyer=swap_dates_buyer[np.where(np.logical_not(pd.Series(swap_dates_buyer).isin(swap_last_dates_buyer)))]
                    swap_dates_seller=swap_dates_seller[np.where(np.logical_not(pd.Series(swap_dates_seller).isin(swap_last_dates_seller)))]

                swap_last_dates_buyer=np.append(swap_last_dates_buyer,swap_dates_buyer)
                swap_last_dates_seller=np.append(swap_last_dates_seller,swap_dates_seller)
                
            
                swap_days_seller = [x.days for x in (swap_dates_seller-trade_date)]
                
                if swap_table.empty:
                    default_date_buyer=trade_date
                    default_date_seller=trade_date
                else:
                    default_date_buyer=last_payment_date_buyer
                    default_date_seller=last_payment_date_seller
                coupon_time_buyer = [DateUtils().day_count(default_date_buyer,swap_dates_buyer[0],swap_daycount_convention)]+[DateUtils().day_count(swap_dates_buyer[i-1],swap_dates_buyer[i],swap_daycount_convention) for i in range(1,len(swap_dates_buyer))]
                coupon_time_seller = [DateUtils().day_count(default_date_seller,swap_dates_seller[0],swap_daycount_convention)]+[DateUtils().day_count(swap_dates_seller[i-1],swap_dates_seller[i],swap_daycount_convention) for i in range(1,len(swap_dates_seller))]
                last_payment_date_buyer=swap_dates_buyer[-1]
                last_payment_date_seller=swap_dates_seller[-1]
                last_payment=np.append(np.repeat(False, len(coupon_time_buyer)-1),True)
                
                swap_table = pd.concat([swap_table, pd.DataFrame({"swap_tenor":swap_tenor, "swap_rate":swap_rate, "swap_days":pd.Series(swap_days_seller), "coupon_time_buyer":pd.Series(coupon_time_buyer), "coupon_time_seller":pd.Series(coupon_time_seller),"last_payment":last_payment})])
            swap_table.reset_index(drop=True,inplace=True)
            swap_table["foreing_discount_curve"] = 1/(1+discount_curve_for[swap_table["swap_days"].values-1]*swap_table["swap_days"].values/360)
            swap_table["foreing_projection_curve"] = 1/(1+projection_curve_for[swap_table["swap_days"].values-1]*swap_table["swap_days"].values/360)
            
            self.logger.info(create_log_msg('Inicia el proceso de optimizacion de la curva Swap_UFCAMARA'))
            fwd_days=fwds_info_daily.iloc[:swap_tenor_days[0],0].values
            nodes_days = np.append(fwd_days,swap_tenor_days)
            fixed_nodes_values = (1/(1+discount_curve_for[fwd_days-1]*fwd_days/360))*(fwds_info_daily.iloc[:swap_tenor_days[0],1]/spot)
            variable_nodes_values = 1/(1+swap_rates*swap_tenor_days/360)
            optimizacion = optimize.minimize(self.ufcamara_bootstrap,variable_nodes_values,args=(nodes_days, fixed_nodes_values, swap_table, interpolation_method, interpolation_nodes))
            if not optimizacion.success:
                self.logger.error(create_log_msg(f'Se genero un error en la optimizacion de la curva cero cupon {swap_curve}'))
                self.logger.error(create_log_msg(optimizacion.message))
                raise PlataformError("Hubo un error en la optimizacion de la curva cero cupon {swap_curve}")
            swap_nodes_values = optimizacion.x
            self.logger.info(create_log_msg(optimizacion.message))
            self.logger.info(create_log_msg('Finalizo el proceso de optimizacion de la curva Swap_UFCAMARA'))
            
            nodes_values = np.append(fixed_nodes_values, swap_nodes_values)
            ccs_curve = curve_function(nodes_days, nodes_values, np.arange(1,max(nodes_days)+1), interpolation_method).get_curve(interpolation_nodes)
            
            swap_curve_daily=pd.DataFrame({"days":np.arange(1,len(ccs_curve)+1),"rate":(1 / ccs_curve - 1) * 360 / np.arange(1,len(ccs_curve)+1)})
            swap_curve_nodes=pd.DataFrame({"days":np.append(uf_forward["days"].values,swap_tenor_days),"rate":(1 / ccs_curve[np.append(uf_forward["days"].values,swap_tenor_days)-1] - 1) * 360 / np.append(uf_forward["days"].values,swap_tenor_days)})
            
            self.logger.info(create_log_msg('Finalizo la construccion de la curva cero cupon Swap_UFCAMARA'))

            return swap_curve_nodes, swap_curve_daily
        
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la construcción de la curva cero cupon Swap_UFCAMARA'))
            raise PlataformError("Hubo un error en en la construcción de la curva cero cupon Swap_UFCAMARA")

##### Fin de código Metodologia


ERROR_MSG_LOG_FORMAT = "{} (linea: {}, {}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)


def setup_logging(log_level):
    """
    formatea todos los logs que invocan la libreria logging
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    precia_handler = logging.StreamHandler(stdout)
    precia_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(precia_handler)
    logger.setLevel(log_level)
    return logger


def create_log_msg(log_msg: str) -> str:
    """
    Aplica el formato de la variable ERROR_MSG_LOG_FORMAT al mensaje log_msg.
    Valida antes de crear el mensaje si existe una excepcion, y de ser el caso
    asocia el mensaje log_msg a los atributos de la excepcion.

    Args:
        log_msg (str): Mensaje de error personalizado que se integrara al log

    Returns:
        str: Mensaje para el log, si hay una excepcion responde con el
        formato ERROR_MSG_LOG_FORMAT
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


#PRECIA_UTILS_AWS
def get_secret(secret_name: str) -> dict:
    """
    Obtiene secretos almacenados en el servicio Secrets Manager de AWS.

    Args:
        secret_name (str): Nombre del secreto en el servicio AWS.

    Raises:
        PlataformError: Cuando ocurre algun error al obtener el secreto.

    Returns:
        dict: Secreto con la informacion desplegada en Secrets Manager AWS.
    """
    try:
        logger.info('Intentando obtener secreto: "%s" ...', secret_name)
        cliente_secrets_manager = boto3.client("secretsmanager")
        secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
        if "SecretString" in secret_data:
            secret_str = secret_data["SecretString"]
        else:
            secret_str = base64.b64decode(secret_data["SecretBinary"])
        logger.info("Se obtuvo el secreto.")
        return json.loads(secret_str)
    except (Exception,) as sec_exc:
        error_msg = f'Fallo al obtener el secreto "{secret_name}"'
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc


def get_params(parameter_list: list) -> dict:
    """
    Obtiene los parametros configurados en 'Job parameters' en el Glue Job

    Args:
        parameter_list (list): Lista de parametros a recuperar

    Returns:
        dict: diccionario de los parametros solicitados
    """
    try:
        logger.info("Obteniendo parametros ...")
        params = getResolvedOptions(sys.argv, parameter_list)
        logger.info("Todos los parametros fueron encontrados")
        return params
    except (Exception,) as sec_exc:
        error_msg = (
            f"No se encontraron todos los parametros solicitados: {parameter_list}"
        )
        logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc


logger = setup_logging(logging.INFO)

try:
    logger.info("Inicia la lectura de parametros del glue...")
    params_key = [
        "DB_SECRET",
        "DATANFS_SECRET",
        "VALUATION_DATE",
        "LAMBDA_PROCESS"    
    ]
    params_dict = get_params(params_key)

    secret_db = params_dict["DB_SECRET"]
    db_url_publish = secret_db["conn_string_aurora_publish"]
    schema_publish_otc = secret_db["schema_aurora_publish"]
    db_url_publish = db_url_publish+schema_publish_otc
    db_url_sirius_pub = secret_db["conn_string_sirius"]
    schema_publish_rates = secret_db["schema_sirius_publish"]
    db_url_sirius_pub = db_url_sirius_pub+schema_publish_rates
    db_url_process = secret_db["conn_string_aurora_process"]
    schema_process_otc = secret_db["schema_aurora_process"]
    db_url_process = db_url_process+schema_process_otc
    db_url_sources = secret_db["conn_string_aurora_sources"]
    schema_precia_utils_otc = secret_db["schema_aurora_precia_utils"]
    db_url_utils = db_url_sources+schema_precia_utils_otc    
    db_schema_src = secret_db["schema_sources"]
    db_url_src = db_url_sources + db_schema_src
    secret_sftp = params_dict["DATANFS_SECRET"]
    parameter_store_name = 'ps-otc-lambda-reports'
    valuation_date = params_dict["VALUATION_DATE"]
    lambda_trigger = params_dict["LAMBDA_PROCESS"]


except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc


class filemanager():
    def download_sftp(self, valuation_date):
        """Funcion encargada de realizar la descarga del archivo 
        SwapCC_Camara_Diaria del sftp

        Args:
            valuation_date (str): Fecha de valoracion

        Raises:
            PlataformError: Erro si la estructura del secreto no es la esperada
            PlataformError: Error si la conexión al SFTP falla
            PlataformError: Error si no se logro realizar la descarga del archivo

        Returns:
            file_data: Archivo descargado del SFTP
        """
        try:
            secret = get_secret(secret_sftp)
            timeout = int(40)
            logger.info("Conectandose al SFTP ...")
            logger.info("Validando secreto del SFTP ...")
            sftp_host = secret["sftp_host"]
            sftp_port = secret["sftp_port"]
            sftp_username = secret["sftp_user"]
            sftp_password = secret["sftp_password"]

            logger.info("Secreto del SFTP tienen el formato esperado.")
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            logger.info("Intentando conectarse a : %s, %s ...", sftp_host, sftp_port)
            client.connect(
                sftp_host,
                port=sftp_port,
                username=sftp_username,
                password=sftp_password,
                timeout=timeout,
            )
            sftp_client = client.open_sftp()
            route_path = secret["route_fwd"]
            logger.info("Conexion al SFTP exitosa.")    
        except KeyError as key_exc:
            error_msg = "El secreto del SFTP no tiene el formato esperado"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from key_exc
        except (Exception,) as unknown_exc:
            error_msg = "Fallo el intento de conexion al SFTP"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from unknown_exc   
        name_file = "SwapCC_Camara_Diaria_yyyymmdd.txt"
        valuation_dates = valuation_date.replace("-","")
        name_file = name_file.replace("yyyymmdd", valuation_dates)
        route_path = route_path + name_file
        logger.debug(route_path)
        try:
            with sftp_client.open(route_path, 'rb') as remote_file:
                file_data = remote_file.read()
            logger.info(f"Se ha descargado el archivo {name_file} correctamente del SFTP")
            return file_data
        except Exception as e:
            error_msg = f"Ocurrio un error en la descarga del SFTP del archivo: {name_file}"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from e


    def load_files_to_sftp(self, file_name,file_content):
        """_summary_

        Args:
            file_name (str): nombre del archivo a cargar en el SFTP
            file_content (pd.dataframe): contenido del archivo que se va a cargar en el SFTP

        Raises:
            PlataformError: Erro si la estructura del secreto no es la esperada
            PlataformError: Error si la conexión al SFTP falla
            PlataformError: Error si no se logro realizar la carga del archivo
        """
        try:
            secret = get_secret(secret_sftp)
            timeout = int(2)
            logger.info("Conectandose al FTP ...")
            logger.info("Validando secreto del FTP ...")
            sftp_host = secret["sftp_host"]
            sftp_port = secret["sftp_port"]
            sftp_username = secret["sftp_user"]
            sftp_password = secret["sftp_password"]
            logger.info("Secreto del FTP tienen el formato esperado.")
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            logger.info("Intentando conectarse a : %s, %s ...", sftp_host, sftp_port)
            client.connect(
                sftp_host,
                port=sftp_port,
                username=sftp_username,
                password=sftp_password,
                timeout=timeout,
            )
            sftp_client = client.open_sftp()
            route_sftp = secret["route_fwd"]
            logger.info("Conexion al SFTP exitosa.")    
        except KeyError as key_exc:
            error_msg = "El secreto del SFTP no tiene el formato esperado"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from key_exc
        except (Exception,) as unknown_exc:
            error_msg = "Fallo el intento de conexion al SFTP"
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from unknown_exc      
        logger.info("Comenzando a cargar el archivo %s en el sftp", file_name)
        error_message = f"No se pudo cargar el archivo en el SFTP: {file_name}"
        try:
            with sftp_client.open(route_sftp + "/" + file_name, "w") as f:
                try:
                    if file_name.lower().endswith(".txt"):
                        f.write(file_content.to_csv(index=False, sep=" ", line_terminator='\r\n', header=True))
                    else:
                        f.write(file_content.decode)
                except (Exception,):
                    error_msg = "Fallo la escritura del archivo:" + str(file_name)
                    logger.error(create_log_msg(error_msg))
                    raise
        except (Exception,) as e:
            logger.error(create_log_msg(error_message))
            raise PlataformError(error_msg) from e
        finally:
            sftp_client.close()  


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
        
    
    def get_uf_data(self):
        """Funcion encargada de consultar el historico de UF en la 
        base de datos de process la tabla prc_otc_historic_uf

        Returns:
            pd.dataframe: Informacion consultada en base de datos 
        """

        select_query = ("SELECT distinct valuation_date, value_uf FROM prc_otc_historic_uf" +
                        " WHERE status_info = 1;"
        )
        db_connection = self.create_connection()
        ud_data_df = pd.read_sql(
            select_query, db_connection
        )
        db_connection.close()
        return ud_data_df 
    

    def get_swi_data(self):
        """Funcion encargada de consultar la informacion de swap en la 
        base de datos de process la tabla prc_otc_inter_swap

        Returns:
            pd.dataframe: Informacion consultada en base de datos 
        """
        select_query = ("SELECT tenor, days, mid_price"+
                        " FROM prc_otc_inter_swap WHERE curve = 'Swap_UFCAMARA' "+
                        "AND  instrument_swap = 'ois_camara_fixuf' AND status_info = 1"
        )
        db_connection = self.create_connection()
        swi_data_df = pd.read_sql(
            select_query, db_connection
        )
        db_connection.close()
        return swi_data_df 
    

    def get_fwd_data(self):
        """Funcion encargada de consultar la informacion de camara en la 
        base de datos de process la tabla prc_otc_inter_swap

        Returns:
            pd.dataframe: Informacion consultada en base de datos 
        """
        select_query = ("SELECT tenor, mid_price FROM prc_otc_inter_swap WHERE curve = 'Swap_UFCAMARA' "+
                        "AND  instrument_swap = 'forward_clfclp' AND status_info = 1"
        )
        db_connection = self.create_connection()
        fwd_data_df = pd.read_sql(
            select_query, db_connection
        )
        db_connection.close()
        return fwd_data_df


    def get_rate_data(self):
        """Funcion encargada de consultar la informacion de la tasa CLFCLP 
        base de datos de pub_rates la tabla pub_exchange_rate_parity

        Returns:
            pd.dataframe: Informacion consultada en base de datos 
        """
        select_query = ("SELECT id_precia, value_rates, valuation_date"+
                        " FROM pub_exchange_rate_parity WHERE id_precia = 'CLFCLP' "+
                        f"AND valuation_date = '{valuation_date}' AND status_info = 1"
        )
        db_connection = self.create_connection()
        fwd_params_df = pd.read_sql(
            select_query, db_connection
        )
        db_connection.close()
        return fwd_params_df


    def get_curves_characteristics(self):
        """Funcion encargada de consultar la informacion caracteristicas de 
        las curvas para swap internacional en la base de datos de sources
        en la tabla precia_utils_swi_curves_characteristics

        Returns:
            pd.dataframe: Informacion consultada en base de datos 
        """
        select_query = ("SELECT curve_name, interpolation_method, interpolation_nodes "+
                        "FROM precia_utils_swi_curves_characteristics"
        )
        db_connection = self.create_connection()
        curves_characteristics = pd.read_sql(
            select_query, db_connection
        )
        db_connection.close()
        return curves_characteristics
    
    
    def get_interest_rates_characteristics(self):
        """Funcion encargada de consultar la informacion la tasa de interes de 
        las curvas para swap internacional en la base de datos de sources
        en la tabla precia_utils_swi_interest_rates_characteristics

        Returns:
            pd.dataframe: Informacion consultada en base de datos 
        """
        select_query = ("SELECT interest_rate_name, daycount_convention, currency, tenor, start_type, business_day_convention, business_day_calendar"+
                        " FROM precia_utils_swi_interest_rates_characteristics"
        )
        db_connection = self.create_connection()
        interest_rates_characteristics = pd.read_sql(
            select_query, db_connection
        )
        db_connection.close()
        return interest_rates_characteristics    


    def get_calendar_cl(self):
        """Funcion encargada de consultar la informacion calendario de 
        los dias habiles de chile en la base de datos de sources
        en la tabla precia_utils_calendars

        Returns:
            pd.dataframe: Informacion consultada en base de datos 
        """
        select_query = ("SELECT dates_calendar FROM precia_utils_calendars  WHERE chilean_calendar = 1")
        db_connection = self.create_connection()
        calendar_cl = pd.read_sql(
            select_query, db_connection
        )
        calendar_cl['dates_calendar'] = pd.to_datetime(calendar_cl['dates_calendar'], format='%Y-%m-%d')
        calendar_cl['dates_calendar'] = calendar_cl['dates_calendar'].dt.strftime("%Y-%m-%d")
        calendar_cl =calendar_cl['dates_calendar'].tolist()

        db_connection.close()
        return calendar_cl    
    
    def get_swap_charactetistics(self):
        """Funcion encargada de consultar la informacion caracteristicas de 
        los swap internacional en la base de datos de sources
        en la tabla precia_utils_swi_swap_characteristics

        Returns:
            pd.dataframe: Informacion consultada en base de datos 
        """
        select_query = ("SELECT swap_curve, swap_name, daycount_convention, buyer_leg_rate, seller_leg_rate, "+
                        "buyer_leg_currency, seller_leg_currency, buyer_leg_frequency_payment, "+
                        "seller_leg_frequency_payment, start_type, on_the_run_tenors, bullet_tenors, "+
                        "business_day_convention, business_day_calendar, buyer_leg_projection_curve, "+
                        "seller_leg_projection_curve, buyer_leg_discount_curve, seller_leg_discount_curve,"+
                        "colateral FROM precia_utils_swi_swap_characteristics")
        
        db_connection = self.create_connection()
        swi_char_df = pd.read_sql(
            select_query, db_connection
        )
        db_connection.close()
        return swi_char_df    

    def disable_previous_info(self, curve, info_df: pd.DataFrame, db_table: str):
        """Deshabilita la informacion anterior que coincida con la curva y la
        fecha de valoracion del dataframe dado

        Args:
            info_df (pd.DataFrame): dataframe de referencia
            db_table (str): tabla en base de datos

        Raises:
            PlataformError: cuando la deshabilitacion de informacion falla
        """
        try:
            logger.info("Construyendo query de actualizacion...")
            info_df = info_df.reset_index(drop=True)
            logger.info(info_df)
            valuation_date_str = info_df.loc[0, "valuation_date"]
            status_info = 0
            update_query = sa.sql.text(
                f"""UPDATE {db_table}
                SET status_info= {status_info} WHERE curve = '{curve}' 
                and valuation_date = '{valuation_date_str}'
                """
            )
            logger.info("Query de actualizacion construido con exito")
            logger.info("Ejecutando query de actualizacion...")
            db_connection = self.create_connection()
            db_connection.execute(update_query)
            logger.info("Query de actualizacion ejecutada con exito")
            db_connection.close()
            logger.info("Conexion a BD cerrada con exito")
        except (Exception,) as ins_exc:
            raise_msg = "Fallo la deshabilitacion de informacion en BD"
            logger.error(create_log_msg(raise_msg))
            
            try:
                db_connection.close()
            except (Exception,):
                error_msg = "Fallo el intento de cierre de conexion a BD"
                logger.error(create_log_msg(error_msg))
            raise PlataformError() from ins_exc

class generate_files():
    
    def __init__(self, uf_curve_node,uf_curve_daily, valuation_date) -> None:
        self.uf_curve_node = uf_curve_node 
        self.uf_curve_daily = uf_curve_daily 
        self.valuation_date = valuation_date
        self.generate_file_nodes()
        self.generate_daily_file()
        payload = {"product": "Swap Inter", "input_name":["SwapCC_UFCAMARA"], "valuation_date":[valuation_date]}
        self.trigger_swapp_cc(lambda_trigger, payload)
        
        
        
    
    def generate_file_nodes(self):
        """Función encargada  de generar el archivo de nodos de UFCamara

        Raises:
            PlataformError: Error si no se logra generar el archivo
        """
        try:
            curve = 'UFCAMARA'
            logger.info(f'Se intenta generar el archivo de nodos para la curva {curve}...')
            self.uf_curve_node.columns = ['days','rate']
            self.uf_curve_node['valuation_date'] = self.valuation_date
            actiondb = actions_db(db_url_publish)
            actiondb.disable_previous_info('SwapCC_UFCAMARA',self.uf_curve_node, "pub_otc_inter_swap_cross_points_nodes")
            engine = create_engine(db_url_publish)
            self.uf_curve_node["curve"] = 'SwapCC_UFCAMARA'
            self.uf_curve_node.to_sql("pub_otc_inter_swap_cross_points_nodes", engine, if_exists="append", index=False)
            valuation_date = self.valuation_date.replace('-', '')
            filename = 'SwapCC_'+curve +'_Nodos_'+ valuation_date
            filename = filename + '.txt'
            self.uf_curve_node = self.uf_curve_node[["days", "rate"]]
            self.uf_curve_node.to_csv(filename, sep=" ", line_terminator='\r\n', index=False, header=None)
            file_manager = filemanager()
            file_manager.load_files_to_sftp(filename, self.uf_curve_node)
            logger.info(f'Se logra generar el archivo de la curva diaria para la curva {curve}')
            
        except (Exception,) as conn_exc:
            raise_msg = "No se logro generar el archivo para la curva diaria de {curve}"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from conn_exc    
     
        
    def generate_daily_file(self):
        """Función encargada  de generar el archivo de curva diaria de UFCamara

        Raises:
            PlataformError: Error si no se logra generar el archivo
        """
        try:
            curve = 'UFCAMARA'
            logger.info(f'Se intenta generar el archivo de nodos para la curva {curve}...')
            self.uf_curve_daily.columns = ['days','rate']
            self.uf_curve_daily['valuation_date'] = self.valuation_date
            actiondb = actions_db(db_url_publish)
            actiondb.disable_previous_info('SwapCC_UFCAMARA',self.uf_curve_daily, "pub_otc_inter_swap_cross_daily")
            engine = create_engine(db_url_publish)
            self.uf_curve_daily["curve"] = 'SwapCC_UFCAMARA'
            self.uf_curve_daily.to_sql("pub_otc_inter_swap_cross_daily", engine, if_exists="append", index=False)
            valuation_date = self.valuation_date.replace('-', '')
            filename = 'SwapCC_'+curve +'_Diaria_'+ valuation_date
            filename = filename + '.txt'
            self.uf_curve_daily = self.uf_curve_daily[["days", "rate"]]
            self.uf_curve_daily.to_csv(filename, sep=" ", line_terminator='\r\n', index=False, header=None)
            file_manager = filemanager()
            file_manager.load_files_to_sftp(filename, self.uf_curve_daily)
            logger.info(f'Se logra generar el archivo de la curva diaria para la curva {curve}')
            
        except (Exception,) as conn_exc:
            raise_msg = "No se logro generar el archivo para la curva diaria de {curve}"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from conn_exc  

    def trigger_swapp_cc(self, lambda_name: str, payload):
    
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
          "input_id": "Swap_UFCAMARA",
          "output_id":["SwapCC_UFCAMARA"],
          "process": "Derivados OTC",
          "product": "swp_inter",
          "stage": "Metodologia",
          "status": "",
          "aws_resource": "glue-p-swi-process-ufcamara",
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



def run():
    try:
        """file_manager = filemanager()
        curva_camara = file_manager.download_sftp(valuation_date)"""
        actiondb = actions_db(db_url_process)
        uf = actiondb.get_uf_data()
        swap_info = actiondb.get_swi_data()
        curva_camara = pd.read_csv("SwapCC_Camara_Diaria_20230630.txt", delim_whitespace=True,header=None)
        fwd_ufcamara_nodos = actiondb.get_fwd_data()
        actiondb = actions_db(db_url_utils)
        interest_rates_characteristics = actiondb.get_interest_rates_characteristics()
        curves_characteristics = actiondb.get_curves_characteristics()
        swaps_characteristics = actiondb.get_swap_charactetistics()
        actiondb = actions_db(db_url_utils)
        calendars = actiondb.get_calendar_cl()
        actiondb = actions_db(db_url_sirius_pub)
        clfclp_data = actiondb.get_rate_data()
        fx_spot = clfclp_data.loc[0,'value_rates']
        swap_info = swap_info.sort_values(by='days')
        swap_class=swap_functions(swaps_characteristics,curves_characteristics,interest_rates_characteristics,calendars,logger)
        valuation_dates = datetime.strptime(valuation_date,'%Y-%m-%d').date()
        uf_curve_node,uf_curve_daily=swap_class.ufcamara_curve(valuation_dates,swap_info,fwd_ufcamara_nodos,uf,fx_spot,curva_camara.iloc[:,1].values,fwd_ufcamara_nodos.iloc[:,0])
        generate_files(uf_curve_node,uf_curve_daily, valuation_date)
        logger.debug(uf_curve_node)
        logger.debug(uf_curve_daily)
        update_report_process("Exitoso", "Proceso Finalizado", "")
    except (Exception,) as init_exc:
        error_msg = "Fallo la ejecución del main, por favor revisar:"
        logger.error(create_log_msg(error_msg))
        update_report_process("Fallido", error_msg, str(init_exc))
        raise PlataformError() from init_exc      


if __name__ == "__main__":
    run()