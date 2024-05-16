# -*- coding: utf-8 -*-
"""
Fecha: 2023-06-14
Autor: Sebastian Velez <svelez@precia.co>
"""

import datetime as dt
import pandas as pd
import numpy as np
from Interpolation import Interpol
from DateUtils import DateUtils,CalendarHandler
from scipy import optimize
from dateutil import relativedelta as rd

from precia_utils.precia_logger import create_log_msg
from precia_utils.precia_exceptions import PlataformError
pd.options.mode.chained_assignment = None

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
            first_fut_date_year = dt.datetime(trade_date.year, fut_months_term * int(np.ceil(trade_date.month/fut_months_term)) ,1).date()
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
    
    def ois_bootstrap(self, variable_nodes_values, nodes_days, fixed_nodes_values, swap_table, interpolation_method, interpolation_nodes):
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
            swap_table["ois_curve"] = curve_function(nodes_days, nodes_values, swap_table["swap_days"].values, interpolation_method).get_curve(interpolation_nodes)
            if 'discount_curve' in swap_table.columns:
                swap_table_ = swap_table.groupby(['swap_tenor','swap_rate']).apply(
                    lambda x: (((x["ois_curve"].shift(1)/x["ois_curve"])-1)*x["discount_curve"]).sum()/(x["discount_curve"]*x["coupon_time"]).sum()).reset_index(level="swap_rate")
                swap_table_.rename(columns={0:"swap_rate_implied"},inplace=True)
            else:
                swap_table["fra_rate_value"]=(swap_table["ois_curve"].shift(1).replace(np.nan,1)/swap_table["ois_curve"])-1
                float_leg=(swap_table["fra_rate_value"]*swap_table["ois_curve"]).cumsum()[swap_table["last_payment"]]
                fix_leg=((swap_table["coupon_time"]*swap_table["ois_curve"]).cumsum()[swap_table["last_payment"]])*swap_table["swap_rate"][swap_table["last_payment"]]
                swap_out=pd.DataFrame({"tenor":swap_table["swap_tenor"][swap_table["last_payment"]],"fix_leg":fix_leg,"float_leg":float_leg}).set_index("tenor")
                
            bootstrap_error = swap_out["fix_leg"] - swap_out["float_leg"]
            return(np.dot(bootstrap_error,bootstrap_error))
        except(Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)
            
    def ois_curve(self, swap_curve, trade_date, swaps_info, discount_curve=None, curve_in_t2=False):
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
            on_date = DateUtils().tenor_date(trade_date, on_tenor, on_business_day_calendar, on_starting_type_convention, on_business_day_convention)
            if curve_in_t2: on_date = trade_date + 1 # Adjustment to curve in t+2, on_day is always 1 day
            on_day = (on_date-trade_date).days
            on_value = 1/(1+on_rate*DateUtils().day_count(trade_date,on_date,on_daycount_convention))
    
            #Swap handling
            swap_tenors = swaps_info.loc[np.where(np.logical_not(swaps_info["tenor"]=='ON')),"tenor"].values
            swap_rates = swaps_info.loc[np.where(np.logical_not(swaps_info["tenor"]=='ON')),"rate"].values
            effective_date = DateUtils().starting_date(trade_date, swap_business_day_calendar,swap_starting_type_convention)
            swap_tenor_dates=[DateUtils().tenor_date(
                            trade_date, x, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention) for x in swap_tenors]
    
            swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-trade_date)]
            if curve_in_t2: swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-effective_date)] # Adjustment to curve in t+2
            
            self.logger.info(create_log_msg(self._flows_log_message))
            #Cashflow handling
            swap_table = pd.DataFrame()
            swap_last_dates=np.array([])
            last_payment_date=None
    
            for swap_tenor in swap_tenors:
                #Looping though swap tenors
                swap_rate = swap_rates[np.where(swap_tenors==swap_tenor)][0]
                frequency = swap_tenor if swap_tenor in bullet_tenors else frequency_payment
                swap_dates = DateUtils().tenor_sequence_dates(trade_date, swap_tenor, frequency, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention)
                if not swap_table.empty:
                    swap_dates=swap_dates[np.where(np.logical_not(pd.Series(swap_dates).isin(swap_last_dates)))]
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
            if discount_curve is not None:
                swap_table["discount_curve"]=discount_curve[swap_table["swap_days"]-1].reset_index(drop=True)
            self.logger.info(create_log_msg(f'Inicia el proceso de optimizacion de la curva {swap_curve}'))
            nodes_days = [on_day]+ swap_tenor_days
            fixed_nodes_values = on_value
            variable_nodes_values = 1/(1+swap_rates*swap_tenor_days/360)
            swap_nodes_values = optimize.minimize(self.ois_bootstrap,variable_nodes_values,args=(nodes_days, fixed_nodes_values, swap_table, interpolation_method, interpolation_nodes)).x
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
            
    def eonia_curve(self, estr_curve, spread_eonia_estr, nodes_days):
        """
        Construcción de la curva eonia a partir de la curva cero cupon estr y el spread entre la eonia y estr

        Parameters
        ----------
        estr_curve (pd.dataframe): Curva diaria cero cupon estr
                                   days:  Dias de la curva.
                                   rate: Tasas cero cupon.
        spread_eonia_estr (float): Spread existente entre la tasa eonia y la tasa estr 
        nodes_days (numpy.ndarray): Dias correspondientes a los nodos de la curva cero cupon ESTR
        
        Returns
        -------
        eonia_curve_nodes (pd.dataframe): Curva cero cupon Eonia nodos
                                   days:  Dias de la curva.
                                   rate: Tasas cero cupon.
        eonia_curve_daily (pd.dataframe): Curva cero cupon Eonia diaria
                                   days:  Dias de la curva.
                                   rate: Tasas cero cupon.

        """
        try:
            self.logger.info(create_log_msg('Inicia la construccion de la curva cero cupon Eonia'))
            estr_df_curve=(1/(1+estr_curve["rate"]*estr_curve["days"]/360)).values
            coupon_time =  np.diff(np.append(np.array([0]),range(1,len(estr_df_curve)+1)))
            estr_fra = forward_functions(self.logger).fra_rate(estr_df_curve,coupon_time/360)
            eonia_forward = estr_fra + spread_eonia_estr
            eonia_df_forward = (1/(1 + eonia_forward * coupon_time/360))
            eonia_df_zero = np.cumprod(eonia_df_forward)
            eonia_zero = (1/eonia_df_zero - 1) * 360/np.arange(1,len(estr_df_curve)+1)
            eonia_curve_daily=pd.DataFrame({"days":np.arange(1,len(estr_df_curve)+1),"rate":eonia_zero})
            eonia_curve_nodes=eonia_curve_daily.iloc[nodes_days-1,:]
            self.logger.info(create_log_msg('Finalizo la construccion de la curva cero cupon Eonia'))
            return eonia_curve_nodes,eonia_curve_daily
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la construcción de la curva cero cupon Eonia'))
            raise PlataformError("Hubo un error en en la construcción de la curva cero cupon Eonia")
            
    def ccs_bootstrap(self, variable_nodes_values, nodes_days, fixed_nodes_values, swap_table, interpolation_method, interpolation_nodes):
        """
        Calcula el error bootstrap para la curva cross currency swap apartir de los ccs   
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
            nodes_values = np.append(fixed_nodes_values,variable_nodes_values)
            swap_table["local_discount_curve"] = curve_function(nodes_days, nodes_values, swap_table["swap_days"].values, interpolation_method).get_curve(interpolation_nodes)
            if 'discount_curve' in swap_table.columns:
                swap_table_ = swap_table.groupby(['swap_tenor','swap_rate']).apply(
                    lambda x: (((x["ois_curve"].shift(1)/x["ois_curve"])-1)*x["discount_curve"]).sum()/(x["discount_curve"]*x["coupon_time"]).sum()).reset_index(level="swap_rate")
                swap_table_.rename(columns={0:"swap_rate_implied"},inplace=True)
            else:
                swap_table["fra_rate_value"]=(swap_table["foreing_projection_curve"].shift(1).replace(np.nan,1)/swap_table["foreing_projection_curve"])-1
                float_leg=(swap_table["fra_rate_value"]*swap_table["foreing_discount_curve"]).cumsum()[swap_table["last_payment"]]+swap_table["foreing_discount_curve"][swap_table["last_payment"]]
                fix_leg=((swap_table["coupon_time_seller"]*swap_table["local_discount_curve"]).cumsum()[swap_table["last_payment"]])*swap_table["swap_rate"][swap_table["last_payment"]]+swap_table["local_discount_curve"][swap_table["last_payment"]]
                swap_out=pd.DataFrame({"tenor":swap_table["swap_tenor"][swap_table["last_payment"]],"fix_leg":fix_leg,"float_leg":float_leg}).set_index("tenor")
                
            bootstrap_error = swap_out["fix_leg"] - swap_out["float_leg"]
            return(np.dot(bootstrap_error,bootstrap_error))
        except(Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)
    
    def ccs_curve(self, swap_curve, trade_date, swaps_info, fwd_points, spot, discount_curve_for, projection_curve_for,nodos_fwd):
        """
        Construye la curva cero cupon a partir de información de cross currency swaps y puntos forward

        Params
        ----------
        swap_curve (str): Nombre de la curva swap ha construir.
        trade_date (datetime.date): Fecha de valoración.
        swaps_info (pd.dataframe): Dataframe de dos columnas
                                   tenor:  Tenores tenidos en cuenta de los swaps.
                                   rate: Tasas par de los swaps.
        fwd_points (pd.dataframe): Dataframe con la curva diaria de puntos forward
                                   days:  Dias de la curva.
                                   rate: Puntos forward.
        spot (float): Tasa de cambio spot                                                                   
        discount_curve_for (numpy.ndarray) : Curva de descuento foranea.
        projection_curve_for (numpy.ndarray): Curva de projección de la tasa foranea.
        nodos_fwd (numpy.ndarray): Nodos de la curva diaria de puntos forward
        
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
            on_the_run_tenors=swap_characteristics["on_the_run_tenors"][0].split(',')
            frequency_payment_buyer = swap_characteristics["buyer_leg_frequency_payment"][0]
            frequency_payment_seller = swap_characteristics["seller_leg_frequency_payment"][0]
            eq_freq= frequency_payment_buyer==frequency_payment_seller

            #Swap handling
            swap_tenors = swaps_info.loc[np.isin(swaps_info["tenor"],on_the_run_tenors),"tenor"].values
            swap_rates = swaps_info.loc[np.isin(swaps_info["tenor"],on_the_run_tenors),"rate"].values
            swap_tenor_dates=[DateUtils().tenor_date(
                            trade_date, x, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention) for x in swap_tenors]
    
            swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-trade_date)]
            discount_curve_for=curve_function(np.arange(1,len(discount_curve_for)+1), discount_curve_for, np.arange(1,max(swap_tenor_days)+1), interpolation_method).get_curve(interpolation_nodes)
            projection_curve_for=curve_function(np.arange(1,len(projection_curve_for)+1), projection_curve_for, np.arange(1,max(swap_tenor_days)+1), interpolation_method).get_curve(interpolation_nodes)
             
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
            
            self.logger.info(create_log_msg(f'Inicia el proceso de optimizacion de la curva {swap_curve}'))
            try:
                last_fwd_days=nodos_fwd.loc[np.where(nodos_fwd["tenor"]==swap_tenors[0])[0][0]-1,"days"]
            except(Exception,):
                last_fwd_days=nodos_fwd.loc[np.max(np.where(nodos_fwd["days"]<swap_tenor_days[0])[0]),"days"]
            fwd_days=fwd_points.iloc[:last_fwd_days,0].values
            nodes_days = np.append(fwd_days,swap_tenor_days)
            fixed_nodes_values = (1/(1+discount_curve_for[fwd_days-1]*fwd_days/360))/(1+fwd_points.iloc[:fwd_days[-1],1]/spot.iloc[0,0])
            variable_nodes_values = 1/(1+swap_rates*swap_tenor_days/360)
            swap_nodes_values = optimize.minimize(self.ccs_bootstrap,variable_nodes_values,args=(nodes_days, fixed_nodes_values, swap_table, interpolation_method, interpolation_nodes),tol=1e-11).x
            self.logger.info(create_log_msg(f'Finalizo el proceso de optimizacion de la curva {swap_curve}'))
            
            nodes_values = np.append(fixed_nodes_values, swap_nodes_values)
            ccs_curve = curve_function(nodes_days, nodes_values, np.arange(1,max(nodes_days)+1), interpolation_method).get_curve(interpolation_nodes)
            nodos_fwd_curva=nodos_fwd.loc[np.where(nodos_fwd["days"]<=last_fwd_days),"days"].values
            swap_curve_daily=pd.DataFrame({"days":np.arange(1,len(ccs_curve)+1),"rate":(1 / ccs_curve - 1) * 360 / np.arange(1,len(ccs_curve)+1)})
            swap_curve_nodes=pd.DataFrame({"days":np.append(nodos_fwd_curva,swap_tenor_days),"rate":(1 / ccs_curve[np.append(nodos_fwd_curva,swap_tenor_days)-1] - 1) * 360 / np.append(nodos_fwd_curva,swap_tenor_days)})
            
            self.logger.info(create_log_msg(f'Finalizo la construccion de la curva cero cupon {swap_curve}'))
    
            return swap_curve_nodes, swap_curve_daily
        
        except(Exception,):
            self.logger.error(create_log_msg(f'Se genero un error en la construcción de la curva cero cupon ccs {swap_curve}'))
            raise PlataformError(f"Hubo un error en en la construcción de la curva cero cupon ccs {swap_curve}")
    
    def basis_bootstrap(self, variable_nodes_values, nodes_days, fixed_nodes_values, swap_table, interpolation_method, interpolation_nodes):
        """
        Calcula el error bootstrap para la curva cross currency swap apartir de los swap basis  
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
            nodes_values = np.append(fixed_nodes_values,variable_nodes_values)
            swap_table["local_discount_curve"] = curve_function(nodes_days, nodes_values, swap_table["swap_days"].values, interpolation_method).get_curve(interpolation_nodes)

            swap_table["foreign_fra"]=(swap_table["foreing_projection_curve"].shift(1).replace(np.nan,1)/swap_table["foreing_projection_curve"])-1
            swap_table["domestic_fra"]=(swap_table["local_projection_curve"].shift(1).replace(np.nan,1)/swap_table["local_projection_curve"])-1
            foreing_leg=(swap_table["foreign_fra"]*swap_table["foreing_discount_curve"]).cumsum()[swap_table["last_payment"]]+swap_table["foreing_discount_curve"][swap_table["last_payment"]]
            domestic_leg=((swap_table["domestic_fra"]+swap_table["coupon_time_buyer"]*swap_table["swap_basis"])*swap_table["local_discount_curve"]).cumsum()[swap_table["last_payment"]]+swap_table["local_discount_curve"][swap_table["last_payment"]]
            swap_out=pd.DataFrame({"tenor":swap_table["swap_tenor"][swap_table["last_payment"]],"domestic_leg":domestic_leg,"foreing_leg":foreing_leg}).set_index("tenor")
                
            bootstrap_error = swap_out["domestic_leg"] - swap_out["foreing_leg"]
            return(np.dot(bootstrap_error,bootstrap_error))
        except(Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)
    
    def basis_curve(self, swap_curve, trade_date, swaps_info, fwd_points_pair_1, spot_pair_1 , projection_curve_dom, discount_curve_for, projection_curve_for, nodos_fwd, base_cur="USD", fwd_points_pair_2=None, spot_pair_2=None ,curve_in_t2=False):
        """
        Construye la curva cero cupon a partir de la información de swaps basis y puntos forward, se supone que el basis va sumado a la pata domestica

        Params
        ----------
        swap_curve (str): Nombre de la curva swap ha construir.
        trade_date (datetime.date): Fecha de valoración.
        swaps_info (pd.dataframe): Dataframe de dos columnas
                                   tenor:  Tenores tenidos en cuenta de los swaps.
                                   rate: Basis negociado de los swaps.
        fwd_points_pair_1 (pd.dataframe): Dataframe con la curva diaria de puntos forward para el par principal
                                   days:  Dias de la curva.
                                   rate: Puntos forward.
        spot_pair_1 (float): Tasa de cambio spot del par principal                                         
        projection_curve_dom (numpy.ndarray) : Curva de projección de la tasa domestica.                                   
        discount_curve_for (numpy.ndarray) : Curva de descuento foranea.
        projection_curve_for (numpy.ndarray): Curva de projección de la tasa foranea.
        nodos_fwd (numpy.ndarray): Nodos de la curva diaria de puntos forward
        base_cur (str): Moneda base del par (Optional) USD
        fwd_points_pair_2 (pd.dataframe): Dataframe con la curva diaria de puntos forward para el par secundario
                                   days:  Dias de la curva.
                                   rate: Puntos forward.
        spot_pair_2 (float): Tasa de cambio spot del par secundario           
        curve_in_t2 (Boolean): La curva esta en t+2 (Opcional) False.

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
            on_the_run_tenors=swap_characteristics["on_the_run_tenors"][0].split(',')
            frequency_payment_buyer = swap_characteristics["buyer_leg_frequency_payment"][0]
            frequency_payment_seller = swap_characteristics["seller_leg_frequency_payment"][0]
            eq_freq= frequency_payment_buyer==frequency_payment_seller
            
            effective_date = DateUtils().starting_date(trade_date, swap_business_day_calendar,swap_starting_type_convention)
            
            #ON Rate Characteristics
            interest_rate_buyer_characteristics = self.interest_rates_characteristics.loc[np.where(self.interest_rates_characteristics["interest_rate_name"]==swap_characteristics["buyer_leg_rate"][0])].reset_index(drop=True)
            buyer_rate_daycount_convention = interest_rate_buyer_characteristics["daycount_convention"][0]
            
            interest_rate_seller_characteristics = self.interest_rates_characteristics.loc[np.where(self.interest_rates_characteristics["interest_rate_name"]==swap_characteristics["seller_leg_rate"][0])].reset_index(drop=True)
            seller_rate_daycount_convention = interest_rate_seller_characteristics["daycount_convention"][0]
            
            #Swap handling
            swap_tenors = swaps_info.loc[np.isin(swaps_info["tenor"],on_the_run_tenors),"tenor"].values
            swaps_basis = swaps_info.loc[np.isin(swaps_info["tenor"],on_the_run_tenors),"rate"].values
            swap_tenor_dates=[DateUtils().tenor_date(
                            trade_date, x, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention) for x in swap_tenors]
    
            swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-trade_date)]
            discount_curve_for=curve_function(np.arange(1,len(discount_curve_for)+1), discount_curve_for, np.arange(1,max(swap_tenor_days)+1), interpolation_method).get_curve(interpolation_nodes)
            projection_curve_dom=curve_function(np.arange(1,len(projection_curve_dom)+1), projection_curve_dom, np.arange(1,max(swap_tenor_days)+1), interpolation_method).get_curve(interpolation_nodes)
            projection_curve_for=curve_function(np.arange(1,len(projection_curve_for)+1), projection_curve_for, np.arange(1,max(swap_tenor_days)+1), interpolation_method).get_curve(interpolation_nodes)
            
            if curve_in_t2: swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-effective_date)] # Adjustment to curve in t+2
            
            if fwd_points_pair_2 is None:
                try:
                    last_fwd_days=nodos_fwd.loc[np.where(nodos_fwd["tenor"]==swap_tenors[0])[0][0]-1,"days"]
                except(Exception,):
                    last_fwd_days=nodos_fwd.loc[np.max(np.where(nodos_fwd["days"]<swap_tenor_days[0])[0]),"days"]
                fwd_days=fwd_points_pair_1.iloc[:last_fwd_days,0].values
                if base_cur =="USD":
                    implied_rate_curve = ((1+discount_curve_for[fwd_days-1]*fwd_days/360)*(1+fwd_points_pair_1.iloc[:swap_tenor_days[0]-1,1]/spot_pair_1.iloc[0,0])-1)*360/fwd_days
                else:
                    implied_rate_curve = ((1+discount_curve_for[fwd_days-1]*fwd_days/360)/(1+fwd_points_pair_1.iloc[:swap_tenor_days[0]-1,1]/spot_pair_1.iloc[0,0])-1)*360/fwd_days
                
                on_value_buyer_t2 = 1/(1+implied_rate_curve[0]*DateUtils().day_count(trade_date,effective_date,buyer_rate_daycount_convention))
                on_value_seller_t2 = 1/(1+discount_curve_for[0]*DateUtils().day_count(trade_date,effective_date,seller_rate_daycount_convention))
            else:
                max_days = np.min(len(fwd_points_pair_1),len(fwd_points_pair_2))
                fwd_days=fwd_points_pair_1.iloc[:min(max_days,swap_tenor_days[0])-1,0].values   
                fwd_rate = (spot_pair_1+fwd_points_pair_1.iloc[:max_days,1])*(spot_pair_2+fwd_points_pair_2[:max_days])
                fwd_points = fwd_rate-spot_pair_1*spot_pair_2
                implied_rate_curve = ((1+discount_curve_for[fwd_days-1]*fwd_days/360)/(1+fwd_points.iloc[:swap_tenor_days[0]-1,1]/spot_pair_1*spot_pair_2)-1)*360/fwd_days
            self.logger.info(create_log_msg(self._flows_log_message))
            #Cashflow handling
            swap_table = pd.DataFrame()
            swap_last_dates_buyer=np.array([])
            swap_last_dates_seller=np.array([])
            last_payment_date_buyer=None
            last_payment_date_seller=None
            
            for swap_tenor in swap_tenors:
                #Looping though swap tenors
                swap_basis = swaps_basis[np.where(swap_tenors==swap_tenor)][0]
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
                
                swap_days_buyer = [x.days for x in (swap_dates_buyer-trade_date)]
                
                
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
                swap_table = pd.concat([swap_table, pd.DataFrame({"swap_tenor":swap_tenor, "swap_basis":swap_basis, "swap_days":pd.Series(swap_days_buyer), "coupon_time_buyer":pd.Series(coupon_time_buyer), "coupon_time_seller":pd.Series(coupon_time_seller),"last_payment":last_payment})])
     
            projection_curve_for_t2=projection_curve_for*on_value_seller_t2
            projection_curve_dom_t2=projection_curve_dom*on_value_buyer_t2
            swap_table["foreing_discount_curve"] = 1/(1+discount_curve_for[swap_table["swap_days"].values-1]*swap_table["swap_days"].values/360)
            swap_table["foreing_projection_curve"] = 1/(1+projection_curve_for_t2[swap_table["swap_days"].values-1]*swap_table["swap_days"].values/360)
            swap_table["local_projection_curve"] = 1/(1+projection_curve_dom_t2[swap_table["swap_days"].values-1]*swap_table["swap_days"].values/360)
            
            self.logger.info(create_log_msg(f'Inicia el proceso de optimizacion de la curva {swap_curve}'))
            nodes_days = np.append(fwd_days,swap_tenor_days)
            fixed_nodes_values = (1/(1+implied_rate_curve*fwd_days/360)).values#(1/(1+discount_curve_for[fwd_days-1]*fwd_days/360))/(1+fwd_points.iloc[:swap_tenor_days[0]-1,1]/spot).values
            variable_nodes_values = np.repeat(fixed_nodes_values[-1],len(swap_tenors))
            bnds=[(0,None)]*len(variable_nodes_values)
            swap_nodes_values = optimize.minimize(self.basis_bootstrap,variable_nodes_values,bounds=bnds,args=(nodes_days, fixed_nodes_values, swap_table, interpolation_method, interpolation_nodes),tol=1e-11).x
            self.logger.info(create_log_msg(f'Finalizo el proceso de optimizacion de la curva {swap_curve}'))
            
            nodes_values = np.append(fixed_nodes_values, swap_nodes_values)
            ccs_curve = curve_function(nodes_days, nodes_values, np.arange(1,max(nodes_days)+1), interpolation_method).get_curve(interpolation_nodes)
            
            nodos_fwd_curva=nodos_fwd.loc[np.where(nodos_fwd["days"]<=last_fwd_days),"days"].values
            swap_curve_daily=pd.DataFrame({"days":np.arange(1,len(ccs_curve)+1),"rate":(1 / ccs_curve - 1) * 360 / np.arange(1,len(ccs_curve)+1)})
            swap_curve_nodes=pd.DataFrame({"days":np.append(nodos_fwd_curva,swap_tenor_days),"rate":(1 / ccs_curve[np.append(nodos_fwd_curva,swap_tenor_days)-1] - 1) * 360 / np.append(nodos_fwd_curva,swap_tenor_days)})
               
            self.logger.info(create_log_msg(f'Finalizo la construccion de la curva cero cupon {swap_curve}'))
            return swap_curve_nodes,swap_curve_daily
        
        except(Exception,):
            self.logger.error(create_log_msg(f'Se genero un error en la construcción de la curva cero cupon {swap_curve}'))
            raise PlataformError(f"Hubo un error en en la construcción de la curva cero cupon {swap_curve}")
    
    def ccs_par_curve(self, swap_curve, trade_date, swaps_info, swapcc_curve):
        try:
            swap_characteristics = self.swaps_characteristics.loc[np.where(self.swaps_characteristics["swap_curve"]==swap_curve)].reset_index(drop=True)
            swap_business_day_convention = swap_characteristics["business_day_convention"][0]
            swap_starting_type_convention = swap_characteristics["start_type"][0]            
            swap_business_day_calendar=[dt.datetime.strptime(x,self._date_format).date() for x in self.calendar]
            on_the_run_tenors=swap_characteristics["on_the_run_tenors"][0].split(',')
            used_swaps_info=swaps_info.loc[np.isin(swaps_info["tenor"],on_the_run_tenors)].reset_index(drop=True)
            swap_tenor_dates=[DateUtils().tenor_date(
                            trade_date, x, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention) for x in used_swaps_info["tenor"]]
            effective_date = DateUtils().add_business_days(trade_date, 2, swap_business_day_calendar)
            swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-effective_date)]
            par_curve=pd.DataFrame({"days":swap_tenor_days,"mid":used_swaps_info["rate"].values,"bid":used_swaps_info["bid"].values,"ask":used_swaps_info["ask"].values})
            return par_curve
        except(Exception,):
            self.logger.error(create_log_msg(f'Se genero un error en la generacion de la curva par {swap_curve}'))
            raise PlataformError(f"Hubo un error en en la generacion de la curva par {swap_curve}")
    
    def sofr_update(self, trade_date, sofr_hist, sofr_consults):
        """
        Actualización del historico de la tasa SOFR segun sus fechas de publicación y formación

        Params:
        trade_date (datetime.date): Fecha de valoración         
        sofr_hist (pd.dataframe): Dataframe de dos columnas que contiene el historico de la tasa SOFR
                                   Date: Fecha de formación de la tasa SOFR
                                   SOFR: Tasa SOFR.
        sofr_consults (pd.dataframe): Dataframe de dos columnas
                                   valuation_date: Fecha de consulta de la tasa SOFR
                                   mid_price: Tasa SOFR consultada.
           
        Returns
        sofr_updated (pd.dataframe): Dataframe de dos columnas
                                   Date: Fecha de formación de la tasa SOFR conforme a las fechas de publicación
                                   SOFR: Tasa SOFR actualizada.

        """
        try:
            self.logger.info(create_log_msg('Inicio actualización tasa SOFR publicada'))
            sofr_updated=pd.DataFrame()
            us_calendar=np.array([dt.datetime.strptime(x,self._date_format).date() for x in self.calendar])
            sofr_hist["Date"]=sofr_hist["Date"].apply(lambda x: dt.datetime.strptime(x,self._date_format).date())
            sofr_consults["valuation_date"]=sofr_consults["valuation_date"].apply(lambda x: dt.datetime.strptime(x,self._date_format).date())
            next_business_day=DateUtils().add_business_days(sofr_hist.iloc[-1,0], 1, us_calendar)
            self.logger.info(create_log_msg('Fechas de publicación tasa SOFR faltantes'))
            publish_sofr_dates=us_calendar[np.where((us_calendar>next_business_day) & (us_calendar<=trade_date))]
            last_sofr_date=sofr_hist.iloc[-1,0]
            for d in range(len(publish_sofr_dates)):
                publish_sofr_date=publish_sofr_dates[d]
                sofr_rate=sofr_consults.loc[sofr_consults["valuation_date"]==publish_sofr_date,"mid_price"]
                last_on_date=DateUtils().add_business_days(publish_sofr_date, -1, us_calendar)
                if DateUtils().add_business_days(last_sofr_date, 2, us_calendar)==publish_sofr_date:
                    sofr_updated=pd.concat([sofr_updated,pd.DataFrame({"Date":[last_on_date],"SOFR":sofr_rate})])
                last_sofr_date=last_on_date
            self.logger.info(create_log_msg('Fin actualización tasa SOFR publicada'))
            return sofr_updated
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la actualización del historio de la tasa SOFR'))
            raise PlataformError("Hubo un error en la actualización del historio de la tasa SOFR")
            
            
    def sofr_index(self,sofr_hist):
        """
        Calcula el indice SOFR

        Params:      
        sofr_hist (pd.dataframe): Dataframe de dos columnas que contiene el historico de la tasa SOFR
                                   Date: Fecha de formación de la tasa SOFR
                                   SOFR: Tasa SOFR.
           
        Returns
        -------
        sofr_index (pd.dataframe): Dataframe de dos columnas que contien el indice SOFR
                                   Date: Fecha del indice
                                   Index: Indice SOFR.

        """
        try:
            self.logger.info(create_log_msg('Inicia el calculo del indice SOFR'))
            us_calendar=np.array([dt.datetime.strptime(x,self._date_format).date() for x in self.calendar])
            sofr_hist["Mat_date"]=sofr_hist["Date"].apply(lambda x: DateUtils().add_business_days(x, 1, us_calendar))
            sofr_index=np.cumprod((1+sofr_hist["SOFR"]*(sofr_hist["Mat_date"]-sofr_hist["Date"]).dt.days/360))
            self.logger.info(create_log_msg('Finalizo el calculo del indice SOFR'))
            return pd.DataFrame({"Date":sofr_hist["Mat_date"],"Index":sofr_index})
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la generación del indice SOFR'))
            raise PlataformError("Hubo un error en la generación del indice SOFR")
            
    def sofr_bootstrap(self, variable_nodes_values, nodes_days, fixed_nodes_values, fut_swap_table, swap_tenors, future_tenors, sofr_acum, known_sofr_rates, interpolation_method, interpolation_nodes):
        """
        Calcula el error bootstrap para la curva swap sofr apartir del futuro de 1M, los futuros de 3M y los swap plain vanilla con vencimientos mayores a 3 años
        Params:
            variable_nodes_values (numpy.ndarray): Factores de descuento de cada nodo
            nodes_days (numpy.ndarray): Días correspondientes a los nodos de la curva
            fixed_nodes_values (numpy.ndarray): Factor de descuento ON
            fut_swap_table (pd.DataFrame): Tabla que contiene la información relevante de cada futuro y swap (tenor, tasa par, dias del cupon, tiempo entre cupones) 
            sofr_acum (float): SOFR acumulada desde la fecha de inicio del primer futuro a la fecha de valoración
            interpolation_method (str): Metodo de interpolación utilizado (linear_interpol, cubic_splines_interpol)
            interpolation_nodes (str): Valores sobre los que se interpola (nominal_rates, discount_factors, log_discount_factors)
        Return:
            error (float): Suma de errores al cuadrado
        """
        try:
            if fixed_nodes_values is None:
                nodes_values=variable_nodes_values
            else:
                nodes_values = np.append(np.array([fixed_nodes_values]),variable_nodes_values)
            fut_swap_table["days"].replace(np.nan,nodes_days[0],inplace=True)
            fut_swap_table["ois_curve"] = curve_function(nodes_days, nodes_values, fut_swap_table["days"].values, interpolation_method).get_curve(interpolation_nodes)
            
            
            #Futures bootstrap error
            #1M future
            one_future_info=fut_swap_table.iloc[np.where(np.logical_not(fut_swap_table["tenor"].isin(np.append(future_tenors,swap_tenors))))]
            fra_sofr=forward_functions(self.logger).fra_rate(one_future_info["ois_curve"].values,one_future_info["coupon_time"].values)
            sofr_rates_month=np.append(known_sofr_rates, fra_sofr)
            one_future_out=pd.DataFrame({"tenor":np.unique(one_future_info["tenor"]),"rate":np.unique(one_future_info["rate"]),"implied_market_rate":np.mean(sofr_rates_month)}).set_index("tenor")
            #3M futures
            futures_info=fut_swap_table.iloc[np.where(fut_swap_table["tenor"].isin(future_tenors))]
            futures_out=futures_info.groupby(["tenor","rate"]).apply(lambda x: forward_functions(self.logger).fra_rate_on(x["ois_curve"].values, x["coupon_time"].values,np.unique(x["tenor"])[0],futures_info.iloc[0,0],sofr_acum)).reset_index(level="rate")
            futures_out.rename(columns={0:"implied_market_rate"},inplace=True)
            
            #Swaps bootstrap error
            swap_info=fut_swap_table.iloc[np.where(fut_swap_table["tenor"].isin(swap_tenors))]
            swap_info["fra_rate_value"]=(swap_info["ois_curve"].shift(1).replace(np.nan,1)/swap_info["ois_curve"])-1
            float_leg=(swap_info["fra_rate_value"]*swap_info["ois_curve"]).cumsum()[swap_info["last_payment"]]
            fix_leg=((swap_info["coupon_time"]*swap_info["ois_curve"]).cumsum()[swap_info["last_payment"]])*swap_info["rate"][swap_info["last_payment"]]
            swap_out=pd.DataFrame({"tenor":swap_info["tenor"][swap_info["last_payment"]],"rate":fix_leg,"implied_market_rate":float_leg}).set_index("tenor")
            
            fut_swap_out=pd.concat([one_future_out,futures_out,swap_out])
            
            bootstrap_error = (fut_swap_out["rate"] - fut_swap_out["implied_market_rate"])
            return((np.dot(bootstrap_error,bootstrap_error))*1)
        except(Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)
            
    def sofr_curve(self, trade_date, sofr_inputs, sofr_index, sofr_hist):
        """
        Construye la curva SOFR a partir de la información de mercado del futuro SOFR de 1M, futuros SOFR de 3M
        y swaps TF-SOFR

        Parameters
        ----------
        trade_date (datetime.date): Fecha de valoración.
        sofr_inputs (pd.dataframe): Información de los futuros y swaps utiizados en la construcción de la curva cero
                                   id_precia: id asignado al instrumento
                                   tenor:  Tenores tenidos en cuenta de los swaps, incluido el tenor overnight.
                                   rate: Tasas par de los swaps, incluida la tasa overnight.
        sofr_index (pd.dataframe): Dataframe con al información del indice SOFR con dos columnas
            Date: Fecha del indice en formato YYYY-MM-DD
            Index: Valor del indice para la fecha
        sofr_hist (pd.dataframe):  Dataframe con al información del indice SOFR con dos columnas
            Date: Fecha de la tasa SOFR en formato YYYY-MM-DD
            SOFR: Valor de la tasa SOFR para la fecha 
            
        Returns
        -------
        sofr_curve_node (pd.dataframe): Dataframe con los nodos de la curva cero cupon ffe
            days: dias de la curva
            rate: tasa cero cupon
        sofr_curve_daily (pd.dataframe): Dataframe con de la curva cero cupon ffe diaria
            days: dias de la curva
            rate: tasa cero cupon

        """
        try:
            self.logger.info(create_log_msg('Inicia la construccion de la curva cero cupon SOFR'))
            curve_name="SwapCC_SOFR"
            #Curve Characteristics
            curve_characteristics = self.curves_characteristics.loc[np.where(self.curves_characteristics["curve_name"]==curve_name)].reset_index(drop=True)
            interpolation_method = curve_characteristics["interpolation_method"][0]
            interpolation_nodes = curve_characteristics["interpolation_nodes"][0]
            #Swap Characteristics
            swap_characteristics = self.swaps_characteristics.loc[np.where(self.swaps_characteristics["curve_name"]==curve_name)].reset_index(drop=True)
            swap_business_day_convention = swap_characteristics["business_day_convention"][0]
            swap_starting_type_convention = swap_characteristics["start_type"][0]
            swap_business_day_calendar=[dt.datetime.strptime(x,self._date_format).date() for x in self.calendar]
            swap_daycount_convention = swap_characteristics["daycount_convention"][0]
            bullet_tenors = swap_characteristics["bullet_tenors"][0].split(',')
            on_the_run_tenors=swap_characteristics["on_the_run_tenors"][0].split(',')
            frequency_payment = swap_characteristics["buyer_leg_frequency_payment"][0]
    
            #ON Rate Characteristics
            interest_rate_characteristics = self.interest_rates_characteristics.loc[np.where(self.interest_rates_characteristics["interest_rate_name"]==swap_characteristics["buyer_leg_rate"][0])].reset_index(drop=True)
            on_business_day_calendar=[dt.datetime.strptime(x,self._date_format).date() for x in self.calendar]
            on_daycount_convention = interest_rate_characteristics["daycount_convention"][0]
    
            #ON handling
            try:
                on_rate = sofr_inputs.loc[sofr_inputs["tenor"]=='ON',"mid"][0]
            except (Exception,):
                on_rate = sofr_inputs.loc[sofr_inputs["tenor"]=='ON',"mid"][1]
            on_date = DateUtils().add_business_days(trade_date, 1, on_business_day_calendar)
            on_day = (on_date-trade_date).days
            on_value = 1/(1+on_rate*DateUtils().day_count(trade_date,on_date,on_daycount_convention))
            
            #Futures handling
            futures_info = sofr_inputs.loc[sofr_inputs["id_precia"].str.contains('fut')].reset_index(drop=True)
            futures_info["fut_rate"]=1-futures_info["mid"]
            
            #1 month future
            onem_fut_rate=futures_info.loc[futures_info["id_precia"]=='fut_sofr_1m_1',"fut_rate"][0]
            month_dates=pd.Series(DateUtils().date_sequence(dt.date(trade_date.year, trade_date.month,1), dt.date(trade_date.year, trade_date.month, CalendarHandler(self.logger).get_last_day_month(trade_date))))            
            sofr_hist.set_index("Date", inplace=True)
            self.logger.info(create_log_msg('Extraccion tasas SOFR observadas'))            
            sofr_rate_values=pd.DataFrame(index=month_dates[month_dates<trade_date]).merge(sofr_hist, how="left",left_index=True, right_index=True).fillna(method="ffill")["SOFR"].values
            sofr_rate_values[np.where(np.isnan(sofr_rate_values))[0]]=sofr_hist["SOFR"].values[-1]
            sofr_prom_days=[(x-trade_date).days +1 for x in month_dates[month_dates>=trade_date].values]            
            onem_future_table=pd.DataFrame({"tenor":str(sofr_prom_days[-1]), "rate":onem_fut_rate, "days":pd.Series(sofr_prom_days), "coupon_time":1/360})
            
            #3 months futures
            futures_number = len(futures_info[futures_info["id_precia"].str.contains('3m')])
            
            self.logger.info(create_log_msg('Calculo fechas de vencimiento futuros 3M SOFR'))
            fut_dates_vto=forward_functions(self.logger).calculate_future_dates_vto(trade_date, "3M", swap_business_day_convention, 
                                               futures_number, swap_business_day_calendar, prior_day_adjust = None)[0:futures_number]
            self.logger.info(create_log_msg('Calculo fechas de inicio futuros 3M SOFR'))
            self.logger.info(create_log_msg('Fechas vencimientos futuros 3M SOFR: \n '+ ','.join(map(str, fut_dates_vto))))         
            fut_dates_start=np.array([DateUtils().month_third_wednesday(date,swap_business_day_calendar) for date in fut_dates_vto-rd.relativedelta(months=3)])
            fut_dates_start = np.array([DateUtils().following_business_day_convention(fut_date_start,swap_business_day_calendar) for fut_date_start in fut_dates_start])
            fut_days = [x.days for x in (fut_dates_vto-trade_date)]
            future_tenors = [forward_functions(self.logger).calculata_future_letters(x) for x in fut_dates_vto]
            
            future_rates=futures_info.loc[futures_info["id_precia"].str.contains('3m'),"fut_rate"].values
            future_table = pd.DataFrame()
            for i in range(len(fut_dates_vto)):
                tenor=future_tenors[i]
                fut_rate = future_rates[i]
                fut_date_vto = fut_dates_vto[i]
                fut_date_start  = fut_dates_start[i]
                future_days=np.array([(fut_date_start-trade_date).days,(fut_date_vto-trade_date).days])
                future_days=np.where(future_days<=0,np.nan,future_days)
                coupon_time = [np.nan,DateUtils().day_count(fut_date_start,fut_date_vto,swap_daycount_convention)]
                future_table = pd.concat([future_table, pd.DataFrame({"tenor":tenor, "rate":fut_rate, "days":pd.Series(future_days), "coupon_time":pd.Series(coupon_time)})])

            self.logger.info(create_log_msg(f'Calculo SOFR acumulada hasta {trade_date.strftime("%Y-%m-%d")}'))
            add_days= 0 if trade_date in swap_business_day_calendar else (trade_date-DateUtils().add_business_days(trade_date, -1, swap_business_day_calendar)).days
            sofr_index_dates = sofr_index["Date"]
            sofr_index_values = sofr_index["Index"][np.where(sofr_index_dates.isin(pd.Series([fut_dates_start[0],trade_date-dt.timedelta(days=add_days)])))[0]].values
            sofr_acum=sofr_index_values[1]/sofr_index_values[0]
            
            #Swap handling
            swap_tenors = sofr_inputs.loc[(sofr_inputs["id_precia"].str.contains('ois') & np.isin(sofr_inputs["tenor"],on_the_run_tenors)),"tenor"].values
            swap_rates = sofr_inputs.loc[(sofr_inputs["id_precia"].str.contains('ois') & np.isin(sofr_inputs["tenor"],on_the_run_tenors)),"mid"].values
            self.logger.info(create_log_msg('Calculo fechas de vencimiento swaps SOFR'))
            swap_tenor_dates=[DateUtils().tenor_date(
                            trade_date, x, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention) for x in swap_tenors]
    
            swap_tenor_days = [x.days for x in (np.array(swap_tenor_dates)-trade_date)]
                
            #Cashflow handling
            swap_table = pd.DataFrame()
            swap_last_dates=np.array([])
            last_payment_date=None
            
            self.logger.info(create_log_msg(self._flows_log_message))
            swaps_dates = DateUtils().tenor_sequence_dates(trade_date, swap_tenors[-1], frequency_payment, swap_business_day_calendar, swap_starting_type_convention, swap_business_day_convention)
            for swap_tenor in swap_tenors:
                #Looping though swap tenors
                swap_rate = swap_rates[np.where(swap_tenors==swap_tenor)][0]
                frequency = swap_tenor if swap_tenor in bullet_tenors else frequency_payment
                tenor_specs = DateUtils().tenor_specs(swap_tenor)
                frequency_specs = DateUtils().tenor_specs(frequency)
                if (tenor_specs["period"]== frequency_specs["period"]):
                    tenor_count =  int(tenor_specs["length"]/frequency_specs["length"])
                elif (tenor_specs["period"]== "Y" and frequency_specs["period"] == "M"):
                    tenor_count =  int(tenor_specs["length"]*12/frequency_specs["length"])
                swap_dates = swaps_dates[:tenor_count+1]
                if not swap_table.empty:
                    swap_dates=swap_dates[np.where(np.logical_not(pd.Series(swap_dates).isin(swap_last_dates)))]
                swap_last_dates=np.append(swap_last_dates,swap_dates)
                swap_days = [x.days for x in (swap_dates-trade_date)]
                
                if swap_table.empty:
                    default_date=trade_date
                else:
                    default_date=last_payment_date
                
                coupon_time = [DateUtils().day_count(default_date,swap_dates[0],swap_daycount_convention)]+[DateUtils().day_count(swap_dates[i-1],swap_dates[i],swap_daycount_convention) for i in range(1,len(swap_dates))]
                last_payment_date=swap_dates[-1]
                last_payment=np.append(np.repeat(False, len(coupon_time)-1),True) 
                swap_table = pd.concat([swap_table, pd.DataFrame({"tenor":swap_tenor, "rate":swap_rate, "days":pd.Series(swap_days), "coupon_time":pd.Series(coupon_time),"last_payment":last_payment})])
                
            swap_table.reset_index(drop=True,inplace=True)
            fut_swap_table=pd.concat([onem_future_table,future_table,swap_table]).reset_index(drop=True)
            market_info=pd.DataFrame({"days":[sofr_prom_days[-1]]+ fut_days + swap_tenor_days,
                                      "rates":np.concatenate(([onem_fut_rate],future_rates,swap_rates))}).sort_values("days").reset_index(drop=True)
            self.logger.info(create_log_msg('Inicia el proceso de optimizacion de la curva SOFR'))
            if on_day==market_info.iloc[0,0]:
                nodes_days=market_info["days"].values
                fixed_nodes_values = None
            else:
                nodes_days = np.concatenate(([on_day],market_info["days"].values))
                fixed_nodes_values = on_value
            
            variable_nodes_values = (1/(1+market_info["rates"]*market_info["days"]/360)).values
            bnds=[(0,None)]*len(variable_nodes_values)
            swap_nodes_values = optimize.minimize(self.sofr_bootstrap,variable_nodes_values,bounds=bnds,args=(nodes_days, fixed_nodes_values, fut_swap_table, swap_tenors, future_tenors, sofr_acum, sofr_rate_values, interpolation_method, interpolation_nodes),tol=1e-11).x
            if fixed_nodes_values is None:
                nodes_values=swap_nodes_values
            else:
                nodes_values = np.append(np.array(on_value), swap_nodes_values)
            self.logger.info(create_log_msg('Finalizo el proceso de optimizacion de la curva SOFR'))
            self.logger.info(create_log_msg('Finalizo la construccion de la curva cero cupon SOFR'))
            sofr_curve = curve_function(nodes_days, nodes_values, np.arange(1,max(nodes_days)+1), interpolation_method).get_curve(interpolation_nodes)
            sofr_curve_daily=pd.DataFrame({"days":np.arange(1,len(sofr_curve)+1),"rate":(1 / sofr_curve - 1) * 360 / np.arange(1,len(sofr_curve)+1)})
            sofr_curve_nodes=sofr_curve_daily.iloc[np.array(nodes_days)-1,:]
            
            return sofr_curve_nodes,sofr_curve_daily
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la creacion de la curva SOFR'))
            raise PlataformError("Hubo un error en la creacion de la curva SOFR")
            
    def ffe_bootstrap(self, variable_nodes_values, nodes_days, fixed_nodes_values, ois_swap_table, basis_swap_table, df_sofr_curve, interpolation_method, interpolation_nodes):
        """
        Calcula el error bootstrap para la curva swap sofr apartir del futuro de 1M, los futuros de 3M y los swap plain vanilla con vencimientos mayores a 3 años
        Params:
            variable_nodes_values (numpy.ndarray): Factores de descuento de cada nodo
            nodes_days (numpy.ndarray): Días correspondientes a los nodos de la curva
            fixed_nodes_values (numpy.ndarray): Factor de descuento ON
            fut_swap_table (pd.DataFrame): Tabla que contiene la información relevante de cada futuro y swap (tenor, tasa par, dias del cupon, tiempo entre cupones) 
            sofr_acum (float): SOFR acumulada desde la fecha de inicio del primer futuro a la fecha de valoración
            interpolation_method (str): Metodo de interpolación utilizado (linear_interpol, cubic_splines_interpol)
            interpolation_nodes (str): Valores sobre los que se interpola (nominal_rates, discount_factors, log_discount_factors)
        Return:
            error (float): Suma de errores al cuadrado
        """
        try:
            
            nodes_values = np.append(np.array([fixed_nodes_values]),variable_nodes_values)
            ois_swap_table["days"].replace(np.nan,nodes_days[0],inplace=True)
            ois_swap_table["projection_curve"] = curve_function(nodes_days, nodes_values, ois_swap_table["days"].values, interpolation_method).get_curve(interpolation_nodes)
            ois_swap_table["discount_curve"] = df_sofr_curve[ois_swap_table["days"].values-1]
            
            basis_swap_table["days"].replace(np.nan,nodes_days[0],inplace=True)
            basis_swap_table["projection_curve"] = curve_function(nodes_days, nodes_values, basis_swap_table["days"].values, interpolation_method).get_curve(interpolation_nodes)
            basis_swap_table["discount_curve"] = df_sofr_curve[basis_swap_table["days"].values-1]
                   
            #OIS Swaps bootstrap error
            ois_swap_table["fra_rate_value"]=(ois_swap_table["projection_curve"].shift(1).replace(np.nan,1)/ois_swap_table["projection_curve"])-1
            float_leg=(ois_swap_table["fra_rate_value"]*ois_swap_table["discount_curve"]).cumsum()[ois_swap_table["last_payment"]]
            fix_leg=((ois_swap_table["coupon_time"]*ois_swap_table["discount_curve"]).cumsum()[ois_swap_table["last_payment"]])*ois_swap_table["rate"][ois_swap_table["last_payment"]]
            ois_swap_out=pd.DataFrame({"tenor":ois_swap_table["tenor"][ois_swap_table["last_payment"]],"rate":fix_leg,"implied_market_rate":float_leg}).set_index("tenor")
            ois_error=(ois_swap_out["rate"] - ois_swap_out["implied_market_rate"])
            
            #basis Swaps bootstrap error
            basis_swap_table["ffe_fra_rate"]=(basis_swap_table["projection_curve"].shift(1).replace(np.nan,1)/basis_swap_table["projection_curve"])-1
            basis_swap_table["sofr_fra_rate"]=(basis_swap_table["discount_curve"].shift(1).replace(np.nan,1)/basis_swap_table["discount_curve"])-1
            ffe_leg=(basis_swap_table["ffe_fra_rate"]*basis_swap_table["discount_curve"]).cumsum()[basis_swap_table["last_payment"]]
            sofr_leg=((basis_swap_table["sofr_fra_rate"]+basis_swap_table["coupon_time"]*basis_swap_table["basis"])*basis_swap_table["discount_curve"]).cumsum()[basis_swap_table["last_payment"]]
            basis_swap_out=pd.DataFrame({"tenor":basis_swap_table["tenor"][basis_swap_table["last_payment"]],"ffe_leg":ffe_leg,"sofr_leg":sofr_leg}).set_index("tenor")
            
            basis_error= (basis_swap_out["ffe_leg"] - basis_swap_out["sofr_leg"])
            
            bootstrap_error = pd.concat([ois_error,basis_error])
            return(np.dot(bootstrap_error,bootstrap_error))
        except(Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)
    
    def ffe_curve(self, trade_date, ffe_inputs, sofr_curve):
        """
        Construye la curva FFE a partir de la información de mercado de swaps TF-FFE y swaps basis SOFR-FFE

        Parameters
        ----------
        trade_date (datetime.date): Fecha de valoración.
        ffe_inputs (pd.dataframe): Dataframe con la información de los instrumentos utilizados en la construcción de la curva con tres columnas
            id_precia: id asignado al instrumento
            tenor: tenor del swap
            mid_price: tasa o basis del swap
            
        Returns
        -------
        ffe_curve_node (pd.dataframe): Dataframe con los nodos de la curva cero cupon ffe
            days: dias de la curva
            rate: tasa cero cupon
        ffe_curve_daily (pd.dataframe): Dataframe con de la curva cero cupon ffe diaria
            days: dias de la curva
            rate: tasa cero cupon

        """
        try:
            self.logger.info(create_log_msg('Inicia la construccion de la curva cero cupon FFE'))
            swap_curve="SwapCC_FFE"
            #Curve Characteristics
            curve_characteristics = self.curves_characteristics.loc[np.where(self.curves_characteristics["curve_name"]==swap_curve)].reset_index(drop=True)
            interpolation_method = curve_characteristics["interpolation_method"][0]
            interpolation_nodes = curve_characteristics["interpolation_nodes"][0]
            #OIS Characteristics
            ois_characteristics = self.swaps_characteristics.loc[np.where(self.swaps_characteristics["swap_name"]=="FEDFUND")].reset_index(drop=True)
            ois_business_day_convention = ois_characteristics["business_day_convention"][0]
            ois_starting_type_convention = ois_characteristics["start_type"][0]
            
            ois_business_day_calendar=[dt.datetime.strptime(x,self._date_format).date() for x in self.calendar]
            ois_daycount_convention = ois_characteristics["daycount_convention"][0]
            ois_bullet_tenors = ois_characteristics["bullet_tenors"][0].split(',')
            ois_frequency_payment = ois_characteristics["buyer_leg_frequency_payment"][0]
            
            #Swap basis Characteristics
            basis_characteristics = self.swaps_characteristics.loc[np.where(self.swaps_characteristics["swap_name"]=="SOFR_FFE")].reset_index(drop=True)
            basis_business_day_convention = basis_characteristics["business_day_convention"][0]
            basis_starting_type_convention = basis_characteristics["start_type"][0]
            
            basis_business_day_calendar=[dt.datetime.strptime(x,self._date_format).date() for x in self.calendar]
            basis_daycount_convention = basis_characteristics["daycount_convention"][0]
            basis_bullet_tenors = basis_characteristics["bullet_tenors"][0].split(',')
            basis_frequency_payment = basis_characteristics["buyer_leg_frequency_payment"][0]
    
            #ON Rate Characteristics
            interest_rate_characteristics = self.interest_rates_characteristics.loc[np.where(self.interest_rates_characteristics["interest_rate_name"]==ois_characteristics["buyer_leg_rate"][0])].reset_index(drop=True)
            on_business_day_convention = interest_rate_characteristics["business_day_convention"][0]
            on_tenor = interest_rate_characteristics["tenor"][0]
            on_starting_type_convention = interest_rate_characteristics["start_type"][0] 
            on_business_day_calendar=[dt.datetime.strptime(x,self._date_format).date() for x in self.calendar]
            on_daycount_convention = interest_rate_characteristics["daycount_convention"][0]
            self.logger.info(create_log_msg('Inicia la construccion de la curva cero cupon SOFR'))
            
            #ON handling
            on_rate = ffe_inputs.loc[ffe_inputs["tenor"]=='ON',"mid_price"][0]
            on_date = DateUtils().tenor_date(trade_date, on_tenor, on_business_day_calendar, on_starting_type_convention, on_business_day_convention)
            on_day = (on_date-trade_date).days
            on_value = 1/(1+on_rate*DateUtils().day_count(trade_date,on_date,on_daycount_convention))
            
             
            #Swap handling
            ois_swap_tenors = ffe_inputs.loc[ffe_inputs["id_precia"].str.contains('ois'),"tenor"].values
            ois_swap_rates = ffe_inputs.loc[ffe_inputs["id_precia"].str.contains('ois'),"mid_price"].values
            
            self.logger.info(create_log_msg('Calculo fechas de vencimiento swaps FFE TF'))
            ois_tenor_dates=[DateUtils().tenor_date(
                            trade_date, x, ois_business_day_calendar, ois_starting_type_convention, ois_business_day_convention) for x in ois_swap_tenors]
    
            ois_swap_tenor_days = [x.days for x in (np.array(ois_tenor_dates)-trade_date)]           
            
            self.logger.info(create_log_msg(self._flows_log_message))
            ois_swap_table = pd.DataFrame()
            ois_swap_last_dates=np.array([])
            last_payment_date=None
            
            for swap_tenor in ois_swap_tenors:
                #Looping though swap tenors
                swap_rate = ois_swap_rates[np.where(ois_swap_tenors==swap_tenor)][0]
                frequency = swap_tenor if swap_tenor in ois_bullet_tenors else ois_frequency_payment
                swap_dates = DateUtils().tenor_sequence_dates(trade_date, swap_tenor, frequency, ois_business_day_calendar, ois_starting_type_convention, ois_business_day_convention)
                if not ois_swap_table.empty:
                    swap_dates=swap_dates[np.where(np.logical_not(pd.Series(swap_dates).isin(ois_swap_last_dates)))]
                ois_swap_last_dates=np.append(ois_swap_last_dates,swap_dates)
                swap_days = [x.days for x in (swap_dates-trade_date)]
                
                if ois_swap_table.empty:
                    default_date=trade_date
                else:
                    default_date=last_payment_date
                
                coupon_time = [DateUtils().day_count(default_date,swap_dates[0],ois_daycount_convention)]+[DateUtils().day_count(swap_dates[i-1],swap_dates[i],ois_daycount_convention) for i in range(1,len(swap_dates))]
                last_payment_date=swap_dates[-1]
                last_payment=np.append(np.repeat(False, len(coupon_time)-1),True)
                
                ois_swap_table = pd.concat([ois_swap_table, pd.DataFrame({"tenor":swap_tenor, "rate":swap_rate, "days":pd.Series(swap_days), "coupon_time":pd.Series(coupon_time),"last_payment":last_payment})])
            ois_swap_table.reset_index(drop=True,inplace=True)
            #Cashflow handling 
            
            basis_swap_tenors = ffe_inputs.loc[ffe_inputs["id_precia"].str.contains('basis'),"tenor"].values
            basis_swap_rates = ffe_inputs.loc[ffe_inputs["id_precia"].str.contains('basis'),"mid_price"].values
            self.logger.info(create_log_msg('Calculo fechas de vencimiento swaps basis SOFR-FFE'))
            basis_tenor_dates=[DateUtils().tenor_date(
                            trade_date, x, basis_business_day_calendar, basis_starting_type_convention, basis_business_day_convention) for x in basis_swap_tenors]
    
            basis_swap_tenor_days = [x.days for x in (np.array(basis_tenor_dates)-trade_date)]           
            self.logger.info(create_log_msg('Generacion flujos de los swaps basis SOFR-FFE'))
            basis_swap_table = pd.DataFrame()
            basis_swap_last_dates=np.array([])
            last_payment_date=None
            basis_swaps_dates = DateUtils().tenor_sequence_dates(trade_date, basis_swap_tenors[-1], basis_frequency_payment, basis_business_day_calendar, basis_starting_type_convention, basis_business_day_convention)
            for swap_tenor in basis_swap_tenors:
                #Looping though swap tenors
                swap_rate = basis_swap_rates[np.where(basis_swap_tenors==swap_tenor)][0]
                frequency = swap_tenor if swap_tenor in basis_bullet_tenors else basis_frequency_payment
                tenor_specs = DateUtils().tenor_specs(swap_tenor)
                frequency_specs = DateUtils().tenor_specs(frequency)
                if (tenor_specs["period"]== frequency_specs["period"]):
                    tenor_count =  int(tenor_specs["length"]/frequency_specs["length"])
                elif (tenor_specs["period"]== "Y" and frequency_specs["period"] == "M"):
                    tenor_count =  int(tenor_specs["length"]*12/frequency_specs["length"])
                swap_dates = basis_swaps_dates[:tenor_count+1]
                if not basis_swap_table.empty:
                    swap_dates=basis_swaps_dates[np.where(np.logical_not(pd.Series(swap_dates).isin(basis_swap_last_dates)))]
                basis_swap_last_dates=np.append(basis_swap_last_dates,swap_dates)
                swap_days = [x.days for x in (swap_dates-trade_date)]
                
                if basis_swap_table.empty:
                    default_date=trade_date
                else:
                    default_date=last_payment_date
                
                coupon_time = [DateUtils().day_count(default_date,swap_dates[0],basis_daycount_convention)]+[DateUtils().day_count(swap_dates[i-1],swap_dates[i],basis_daycount_convention) for i in range(1,len(swap_dates))]
                last_payment_date=swap_dates[-1]
                last_payment=np.append(np.repeat(False, len(coupon_time)-1),True)
                
                basis_swap_table = pd.concat([basis_swap_table, pd.DataFrame({"tenor":swap_tenor, "basis":swap_rate, "days":pd.Series(swap_days), "coupon_time":pd.Series(coupon_time),"last_payment":last_payment})])
            basis_swap_table.reset_index(drop=True,inplace=True)
            
            self.logger.info(create_log_msg('Inicia el proceso de optimizacion de la curva cero cupon FFE'))
            df_sofr_curve=(1/(1+sofr_curve*np.arange(1,len(sofr_curve)+1)/360))
            nodes_days = np.concatenate(([on_day],ois_swap_tenor_days,basis_swap_tenor_days))
            fixed_nodes_values = on_value
            variable_nodes_values = np.concatenate([(1/(1+ois_swap_rates*ois_swap_tenor_days/360)),df_sofr_curve[np.array(basis_swap_tenor_days)-1]])
            bnds=[(0,None)]*len(variable_nodes_values)
            swap_nodes_values = optimize.minimize(self.ffe_bootstrap,variable_nodes_values,bounds=bnds,args=(nodes_days, fixed_nodes_values, ois_swap_table, basis_swap_table, df_sofr_curve, interpolation_method, interpolation_nodes),tol=1e-10).x
            nodes_values = np.append(np.array(on_value), swap_nodes_values)
            self.logger.info(create_log_msg('Finalizo el proceso de optimizacion de la curva cero cupon FFE'))
            self.logger.info(create_log_msg('Finalizo la construccion de la curva cero cupon FFE'))
            ffe_curve = curve_function(nodes_days, nodes_values, np.arange(1,max(nodes_days)+1), interpolation_method).get_curve(interpolation_nodes)
            ffe_curve_daily=pd.DataFrame({"days":np.arange(1,len(ffe_curve)+1),"rate":(1 / ffe_curve - 1) * 360 / np.arange(1,len(ffe_curve)+1)})
            ffe_curve_nodes=ffe_curve_daily.iloc[np.array(nodes_days)-1,:]
            return ffe_curve_nodes,ffe_curve_daily
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la creacion de la curva FFE'))
            raise PlataformError("Hubo un error en la creacion de la curva FFE")
            