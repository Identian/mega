# Nativas de Python
import logging
from json import loads as json_loads, dumps as json_dumps
import datetime as dt
from dateutil import relativedelta as rd
from sys import argv, exc_info, stdout
from base64 import b64decode

# AWS
from awsglue.utils import getResolvedOptions 
from boto3 import client as aws_client

# De terceros
import pandas as pd
from sqlalchemy import create_engine, engine
from sqlalchemy.sql import text
import paramiko
import numpy as np
from scipy import optimize

# Personalizadas
from Interpolation import Interpol
from DateUtils import DateUtils

pd.options.mode.chained_assignment = None
first_error_msg = None

# LOGGER: INICIO --------------------------------------------------------------
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


logger = setup_logging(logging.INFO)

# LOGGER: FIN -----------------------------------------------------------------


# EXCEPCIONES PERSONALIZADAS: INICIO--------------------------------------------


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


# EXCEPCIONES PERSONALIZADAS: FIN-----------------------------------------------

# METODOLOGIA: INICIO-----------------------------------------------------------
"""
Created on Tue Oct 31 10:30:30 2023

@author: SebastianVelezHernan
"""


class curve_function:
    """
    Clase que contiene las funciones relacionadas a la construcción de curvas interpoladas
    """

    def __init__(
        self, curve_nodes_tenors, curve_nodes_df, curve_tenors, interpolation_method
    ):
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

        curve_nodes_rates = (
            (1 / self.curve_nodes_df - 1) * 360 / self.curve_nodes_tenors
        )
        rates_curve = Interpol(
            self.curve_nodes_tenors, curve_nodes_rates, self.curve_tenors
        ).method(self.interpolation_method)
        curve = 1 / (1 + rates_curve * self.curve_tenors / 360)
        return curve

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
        x_nodes = np.append(np.array([0]), self.curve_nodes_tenors)
        y_nodes = np.append(np.array([1]), self.curve_nodes_df)
        df_curve = Interpol(x_nodes, y_nodes, self.curve_tenors).method(
            self.interpolation_method
        )
        curve = df_curve
        return curve

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
        x_nodes = np.append(np.array([0]), self.curve_nodes_tenors)
        y_nodes = np.append(np.array([0]), curve_nodes_logdf)
        log_df_curve = Interpol(x_nodes, y_nodes, self.curve_tenors).method(
            self.interpolation_method
        )
        curve = np.exp(log_df_curve)
        return curve

    def get_curve(self, interpolation_nodes):
        """
        Calcula la curva interpolada dado el tipo de nodo ha interpolar
        Params:
        interpolation_nodes (str): nominal_rates, discount_factors, log_discount_factors
        Returns
        case : Función de interpolación sobre los nodos respectivos

        """
        switcher = {
            "nominal_rates": self.curve_from_nominal_rates,
            "discount_factors": self.curve_from_discount_factors,
            "log_discount_factors": self.curve_from_log_discount_factors,
        }
        case = switcher.get(interpolation_nodes, "error")
        if case == "error":
            msg = " interpolation_nodes invalido %s" % interpolation_nodes
            raise ValueError(msg)
        else:
            return case()


class forward_functions:
    """
    Clase que contiene las funciones relacionadas a futuros y forwards
    """

    # -----Attributes-----
    # Class attributes
    _future_letter_conventions = {
        1: "F",
        2: "G",
        3: "H",
        4: "J",
        5: "K",
        6: "M",
        7: "N",
        8: "Q",
        9: "U",
        10: "V",
        11: "X",
        12: "Z",
    }

    # --------------------
    def __init__(self, logger):
        self.logger = logger

    def calculata_future_letters(self, future_date):
        month_future = future_date.month
        letter = self._future_letter_conventions[month_future]
        year = str(future_date.year)[-1]
        return letter + year

    def fra_rate(self, df_curve, coupon_time):
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
            delta_coupon = 1 / coupon_time
            fra = (
                (np.append(np.array([1]), df_coupon[:-1]) / df_coupon) - 1
            ) * delta_coupon
            return fra[np.where(np.logical_not(np.isnan(fra)))]
        except (Exception,):
            self.logger.error(
                create_log_msg("Se genero un error en el calculo de las tasas FRA")
            )
            raise PlataformError("Hubo un error en el calculo de las tasas FRA")

    def fra_rate_on(
        self, df_curve, coupon_time, tenor="xx", first_coupon="zz", index_acum=None
    ):
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
            delta_coupon = (
                1 / coupon_time[np.where(np.logical_not(np.isnan(coupon_time)))]
            )
            if tenor == first_coupon:
                on_acum = index_acum
                df_curve[0] = 1
            else:
                on_acum = 1
            fra = (on_acum * df_coupon[0] / df_coupon[1] - 1) * delta_coupon
            return fra[0]
        except (Exception,):
            self.logger.error(
                create_log_msg(
                    "Se genero un error en el calculo de la tasa FRA, dada la tasa on acumulada"
                )
            )
            raise PlataformError(
                "Hubo un error en el calculo de la tasa FRA, dada la tasa on acumulada"
            )

    def calculate_future_dates_vto(
        self,
        trade_date,
        fut_months_term,
        business_day_convention,
        fut_number,
        calendar,
        prior_day_adjust=None,
    ):
        try:
            fut_months_term = int(fut_months_term.replace("M", ""))
            first_fut_date_year = dt.datetime(
                trade_date.year,
                fut_months_term * int(trade_date.month / fut_months_term),
                1,
            ).date()
            posible_fut_dates = [
                first_fut_date_year + rd.relativedelta(months=3 * i)
                for i in range(fut_number + 1)
            ]
            fut_dates = [
                DateUtils().month_third_wednesday(x) for x in posible_fut_dates
            ]
            fut_dates = np.array(
                [
                    DateUtils().following_business_day_convention(fut_date, calendar)
                    for fut_date in fut_dates
                ]
            )
            next_fut_dates = fut_dates[np.where(fut_dates > trade_date)]
            return next_fut_dates
        except (Exception,):
            self.logger.error(
                create_log_msg(
                    "Se genero un error en la creacion de las fechas de vencimiento de los futuros SOFR 3M"
                )
            )
            raise PlataformError(
                "Hubo un error en la creacion de las fechas de vencimiento de los futuros SOFR 3M"
            )


class swap_functions:
    """
    Clase que contiene las funciones relacionadas a la construcción de curvas swaps
    """

    def __init__(
        self,
        swaps_characteristics,
        curves_characteristics,
        interest_rates_characteristics,
        calendar,
        logger,
    ):
        self.swaps_characteristics = swaps_characteristics
        self.curves_characteristics = curves_characteristics
        self.interest_rates_characteristics = interest_rates_characteristics
        self.calendar = calendar
        self.logger = logger

    # -----Attributes-----

    _date_format = "%Y-%m-%d"
    _bootstrap_error_message = "Fallo el calculo del error bootstrap"
    _bootstrap_error_raise_message = (
        "No se pudo calcular el error bootstrap para la curva con los insumos dados"
    )
    _flows_log_message = "Generacion de fechas y flujos de los swaps empleados"

    # --------------------
    def ois_bootstrap(
        self,
        variable_nodes_values,
        nodes_days,
        fixed_nodes_values,
        swap_table,
        interpolation_method,
        interpolation_nodes,
    ):
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
            nodes_values = np.append(
                np.array([fixed_nodes_values]), variable_nodes_values
            )
            swap_table["ois_curve"] = curve_function(
                nodes_days,
                nodes_values,
                swap_table["swap_days"].values,
                interpolation_method,
            ).get_curve(interpolation_nodes)
            if "discount_curve" in swap_table.columns:
                swap_table["fra_rate_value"] = (
                    swap_table["ois_curve"].shift(1).replace(np.nan, 1)
                    / swap_table["ois_curve"]
                ) - 1
                float_leg = (
                    swap_table["fra_rate_value"] * swap_table["discount_curve"]
                ).cumsum()[swap_table["last_payment"]]
                fix_leg = (
                    (swap_table["coupon_time"] * swap_table["discount_curve"]).cumsum()[
                        swap_table["last_payment"]
                    ]
                ) * swap_table["swap_rate"][swap_table["last_payment"]]
                swap_out = pd.DataFrame(
                    {
                        "tenor": swap_table["swap_tenor"][swap_table["last_payment"]],
                        "fix_leg": fix_leg,
                        "float_leg": float_leg,
                    }
                ).set_index("tenor")
            else:
                swap_table["fra_rate_value"] = (
                    swap_table["ois_curve"].shift(1).replace(np.nan, 1)
                    / swap_table["ois_curve"]
                ) - 1
                float_leg = (
                    swap_table["fra_rate_value"] * swap_table["ois_curve"]
                ).cumsum()[swap_table["last_payment"]]
                fix_leg = (
                    (swap_table["coupon_time"] * swap_table["ois_curve"]).cumsum()[
                        swap_table["last_payment"]
                    ]
                ) * swap_table["swap_rate"][swap_table["last_payment"]]
                swap_out = pd.DataFrame(
                    {
                        "tenor": swap_table["swap_tenor"][swap_table["last_payment"]],
                        "fix_leg": fix_leg,
                        "float_leg": float_leg,
                    }
                ).set_index("tenor")

            bootstrap_error = swap_out["fix_leg"] - swap_out["float_leg"]
            return np.dot(bootstrap_error, bootstrap_error)
        except (Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)

    def ois_curve(
        self, swap_curve, trade_date, swaps_info, discount_curve=None, curve_in_t2=False
    ):
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
            self.logger.info(
                create_log_msg(
                    f"Inicia la construccion de la curva cero cupon {swap_curve}"
                )
            )
            # Curve Characteristics
            curve_characteristics = self.curves_characteristics.loc[
                np.where(self.curves_characteristics["curve_name"] == swap_curve)
            ].reset_index(drop=True)
            interpolation_method = curve_characteristics["interpolation_method"][0]
            interpolation_nodes = curve_characteristics["interpolation_nodes"][0]
            # Swap Characteristics
            swap_characteristics = self.swaps_characteristics.loc[
                np.where(self.swaps_characteristics["swap_curve"] == swap_curve)
            ].reset_index(drop=True)
            swap_business_day_convention = swap_characteristics[
                "business_day_convention"
            ][0]
            swap_starting_type_convention = swap_characteristics["start_type"][0]
            swap_business_day_calendar = [
                dt.datetime.strptime(x, self._date_format).date() for x in self.calendar
            ]
            swap_daycount_convention = swap_characteristics["daycount_convention"][0]
            bullet_tenors = swap_characteristics["bullet_tenors"][0].split(",")
            frequency_payment = swap_characteristics["buyer_leg_frequency_payment"][0]

            # ON Rate Characteristics
            interest_rate_characteristics = self.interest_rates_characteristics.loc[
                np.where(
                    self.interest_rates_characteristics["interest_rate_name"]
                    == swap_characteristics["buyer_leg_rate"][0]
                )
            ].reset_index(drop=True)
            on_business_day_convention = interest_rate_characteristics[
                "business_day_convention"
            ][0]
            on_tenor = interest_rate_characteristics["tenor"][0]
            on_starting_type_convention = interest_rate_characteristics["start_type"][0]
            on_business_day_calendar = [
                dt.datetime.strptime(x, self._date_format).date() for x in self.calendar
            ]
            on_daycount_convention = interest_rate_characteristics[
                "daycount_convention"
            ][0]

            # ON handling
            on_rate = swaps_info.loc[swaps_info["tenor"] == "ON", "rate"][0]
            on_date = DateUtils().tenor_date(
                trade_date,
                on_tenor,
                on_business_day_calendar,
                on_starting_type_convention,
                on_business_day_convention,
            )
            if curve_in_t2:
                on_date = (
                    trade_date + 1
                )  # Adjustment to curve in t+2, on_day is always 1 day
            on_day = (on_date - trade_date).days
            on_value = 1 / (
                1
                + on_rate
                * DateUtils().day_count(trade_date, on_date, on_daycount_convention)
            )
            # Swap handling
            swap_tenors = swaps_info.loc[
                np.where(np.logical_not(swaps_info["tenor"] == "ON")), "tenor"
            ].values
            swap_rates = swaps_info.loc[
                np.where(np.logical_not(swaps_info["tenor"] == "ON")), "rate"
            ].values
            effective_date = DateUtils().starting_date(
                trade_date, swap_business_day_calendar, swap_starting_type_convention
            )
            swap_tenor_dates = [
                DateUtils().tenor_date(
                    trade_date,
                    x,
                    swap_business_day_calendar,
                    swap_starting_type_convention,
                    swap_business_day_convention,
                )
                for x in swap_tenors
            ]

            swap_tenor_days = [
                x.days for x in (np.array(swap_tenor_dates) - trade_date)
            ]
            if curve_in_t2:
                swap_tenor_days = [
                    x.days for x in (np.array(swap_tenor_dates) - effective_date)
                ]  # Adjustment to curve in t+2

            self.logger.info(create_log_msg(self._flows_log_message))
            # Cashflow handling
            swap_table = pd.DataFrame()
            swap_last_dates = np.array([])
            last_payment_date = None

            for swap_tenor in swap_tenors:
                # Looping though swap tenors
                swap_rate = swap_rates[np.where(swap_tenors == swap_tenor)][0]
                frequency = (
                    swap_tenor if swap_tenor in bullet_tenors else frequency_payment
                )
                swap_dates = DateUtils().tenor_sequence_dates(
                    trade_date,
                    swap_tenor,
                    frequency,
                    swap_business_day_calendar,
                    swap_starting_type_convention,
                    swap_business_day_convention,
                )
                if not swap_table.empty:
                    swap_dates = swap_dates[
                        np.where(
                            np.logical_not(pd.Series(swap_dates).isin(swap_last_dates))
                        )
                    ]
                swap_last_dates = np.append(swap_last_dates, swap_dates)
                swap_days = [x.days for x in (swap_dates - trade_date)]

                if swap_table.empty:
                    default_date = trade_date
                else:
                    default_date = last_payment_date
                coupon_time = [
                    DateUtils().day_count(
                        default_date, swap_dates[0], swap_daycount_convention
                    )
                ] + [
                    DateUtils().day_count(
                        swap_dates[i - 1], swap_dates[i], swap_daycount_convention
                    )
                    for i in range(1, len(swap_dates))
                ]
                last_payment_date = swap_dates[-1]
                last_payment = np.append(np.repeat(False, len(coupon_time) - 1), True)

                swap_table = pd.concat(
                    [
                        swap_table,
                        pd.DataFrame(
                            {
                                "swap_tenor": swap_tenor,
                                "swap_rate": swap_rate,
                                "swap_days": pd.Series(swap_days),
                                "coupon_time": pd.Series(coupon_time),
                                "last_payment": last_payment,
                            }
                        ),
                    ]
                )
            swap_table.reset_index(drop=True, inplace=True)
            if discount_curve is not None:
                swap_table["discount_curve"] = discount_curve[
                    swap_table["swap_days"] - 1
                ].reset_index(drop=True)
            self.logger.info(
                create_log_msg(
                    f"Inicia el proceso de optimizacion de la curva {swap_curve}"
                )
            )
            nodes_days = [on_day] + swap_tenor_days
            fixed_nodes_values = on_value
            variable_nodes_values = 1 / (1 + swap_rates * swap_tenor_days / 360)
            optimizacion = optimize.minimize(
                self.ois_bootstrap,
                variable_nodes_values,
                args=(
                    nodes_days,
                    fixed_nodes_values,
                    swap_table,
                    interpolation_method,
                    interpolation_nodes,
                ),
            )
            if not optimizacion.success:
                self.logger.error(
                    create_log_msg(
                        f"Se genero un error en la optimizacion de la curva cero cupon {swap_curve}"
                    )
                )
                self.logger.error(create_log_msg(optimizacion.message))
                raise PlataformError(
                    f"Hubo un error en la optimizacion de la curva cero cupon {swap_curve}"
                )
            swap_nodes_values = optimizacion.x
            self.logger.info(
                create_log_msg(
                    f"Finalizo el proceso de optimizacion de la curva {swap_curve}"
                )
            )
            nodes_values = np.append(np.array(on_value), swap_nodes_values)
            if curve_in_t2:
                nodes_values = np.append(
                    np.array([on_value]),
                    swap_nodes_values * on_value ** (effective_date - trade_date).days,
                )
                nodes_days = np.append(
                    np.array(nodes_days[0]),
                    np.array(nodes_days[1:]) + (effective_date - trade_date).days,
                )

            ois_curve = curve_function(
                nodes_days,
                nodes_values,
                np.arange(1, max(nodes_days) + 1),
                interpolation_method,
            ).get_curve(interpolation_nodes)
            swap_curve_daily = pd.DataFrame(
                {
                    "days": np.arange(1, len(ois_curve) + 1),
                    "rate": (1 / ois_curve - 1)
                    * 360
                    / np.arange(1, len(ois_curve) + 1),
                }
            )
            swap_curve_nodes = swap_curve_daily.iloc[np.array(nodes_days) - 1, :]

            self.logger.info(
                create_log_msg(
                    f"Finalizo la construccion de la curva cero cupon {swap_curve}"
                )
            )
            return swap_curve_nodes, swap_curve_daily
        except (Exception,):
            self.logger.error(
                create_log_msg(
                    f"Se genero un error en la construcción de la curva cero cupon {swap_curve}"
                )
            )
            raise PlataformError(
                f"Hubo un error en en la construcción de la curva cero cupon {swap_curve}"
            )

    def clp_curve_usd_basis_bootstrap(
        self,
        variable_nodes_values,
        nodes_days,
        fixed_nodes_values,
        swap_table,
        interpolation_method,
        interpolation_nodes,
    ):
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
            nodes_values = np.append(
                np.array([fixed_nodes_values]), variable_nodes_values
            )
            swap_table["local_discount_curve"] = curve_function(
                nodes_days,
                nodes_values,
                swap_table["swap_days"].values,
                interpolation_method,
            ).get_curve(interpolation_nodes)

            swap_table_ = (
                swap_table.groupby(["swap_tenor", "swap_basis", "swap_rate"])
                .apply(
                    lambda x: (
                        (
                            (
                                (
                                    (
                                        (
                                            x["foreing_projection_curve"].shift(1)
                                            / x["foreing_projection_curve"]
                                        )
                                        - 1
                                    )
                                    * x["foreing_discount_curve"]
                                ).sum()
                                + x["foreing_discount_curve"].tail(1)
                                - x["local_discount_curve"].tail(1)
                            )
                            / (x["coupon_time"] * x["local_discount_curve"]).sum()
                        )
                        - x["swap_basis"]
                    ).tail(1)
                )
                .reset_index(level="swap_rate")
            )
            swap_table_.rename(columns={0: "swap_rate_implied"}, inplace=True)
            bootstrap_error = (
                swap_table_["swap_rate"] - swap_table_["swap_rate_implied"]
            )

            return np.dot(bootstrap_error, bootstrap_error)
        except (Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)

    def clp_curve_usd_basis(
        self,
        trade_date,
        swaps_info,
        fwd_points,
        spot,
        discount_curve_for,
        projection_curve_for,
        curve_in_t2=False,
    ):
        """
        Construye la curva cero cupon a partir de información de cross currency swaps

        Params
        ----------
        trade_date (datetime.date): Fecha de valoración.
        swaps_info (pd.dataframe): Dataframe de dos columnas
                                   tenor:  Tenores tenidos en cuenta de los swaps.
                                   rate:  Tasas par de los swaps TF-CAMARA
                                   basis: Basis del swap CAMARA-SOFR
        fwd_points (pd.dataframe): Dataframe con la curva nodos de puntos forward
                                   days:  Dias de la curva.
                                   rate: Puntos forward.
        spot (float): Tasa de cambio spot
        discount_curve_dom (numpy.ndarray) : Curva de descuento domestica.
        discount_curve_for (numpy.ndarray) : Curva de descuento foranea.
        projection_curve_for (numpy.ndarray): Curva de projección de la tasa foranea.
        curve_in_t2 (Boolean): La curva esta en t+2 (Opcional) False.

        Returns
        curve_details (dict):
            curve: Curva de factores de descuento diaria desde 1 día hasta el maximo tenor de la curva
            nodes: Dias correspondientes a los tenores de los instrumentos

        """
        try:
            self.logger.info(
                create_log_msg(
                    "Inicia la construccion de la curva cero cupon CLP colateral"
                )
            )
            curve_name = "SwapCC_USDCLP"
            # Curve Characteristics
            curve_characteristics = self.curves_characteristics.loc[
                np.where(self.curves_characteristics["curve_name"] == curve_name)
            ].reset_index(drop=True)
            interpolation_method = curve_characteristics["interpolation_method"][0]
            interpolation_nodes = curve_characteristics["interpolation_nodes"][0]
            # Swap Characteristics
            swap_characteristics = self.swaps_characteristics.loc[
                np.where(self.swaps_characteristics["swap_curve"] == curve_name)
            ].reset_index(drop=True)
            swap_business_day_convention = swap_characteristics[
                "business_day_convention"
            ][0]
            swap_starting_type_convention = swap_characteristics["start_type"][0]
            swap_business_day_calendar = [
                dt.datetime.strptime(x, self._date_format).date() for x in self.calendar
            ]
            swap_daycount_convention = swap_characteristics["daycount_convention"][0]
            bullet_tenors = swap_characteristics["bullet_tenors"][0].split(",")
            frequency_payment = swap_characteristics["buyer_leg_frequency_payment"][0]

            effective_date = DateUtils().starting_date(
                trade_date, swap_business_day_calendar, swap_starting_type_convention
            )
            effective_days = (effective_date - trade_date).days

            # Swap handling
            swap_tenors = swaps_info["tenor"].values
            swap_rates = swaps_info["rate"].values
            swaps_basis = swaps_info["basis"].values
            swap_tenor_dates = [
                DateUtils().tenor_date(
                    trade_date,
                    x,
                    swap_business_day_calendar,
                    swap_starting_type_convention,
                    swap_business_day_convention,
                )
                for x in swap_tenors
            ]

            swap_tenor_days = [
                x.days for x in (np.array(swap_tenor_dates) - trade_date)
            ]
            if curve_in_t2:
                swap_tenor_days = [
                    x.days for x in (np.array(swap_tenor_dates) - effective_date)
                ]  # Adjustment to curve in t+2

            # Cashflow handling
            self.logger.info(create_log_msg(self._flows_log_message))
            swap_table = pd.DataFrame()
            for swap_tenor in swap_tenors:
                # Looping though swap tenors
                swap_basis = swaps_basis[np.where(swap_tenors == swap_tenor)][0]
                swap_rate = swap_rates[np.where(swap_tenors == swap_tenor)][0]
                frequency = (
                    swap_tenor if swap_tenor in bullet_tenors else frequency_payment
                )
                swap_dates = DateUtils().tenor_sequence_dates(
                    trade_date,
                    swap_tenor,
                    frequency,
                    swap_business_day_calendar,
                    swap_starting_type_convention,
                    swap_business_day_convention,
                )
                swap_days = [x.days for x in (np.array(swap_dates) - trade_date)]
                if curve_in_t2:
                    swap_days = [
                        x.days for x in (np.array(swap_dates) - effective_date)
                    ]  # Adjustment to curve in t+2
                coupon_time = [0.0] + [
                    DateUtils().day_count(
                        swap_dates[i - 1], swap_dates[i], swap_daycount_convention
                    )
                    for i in range(1, len(swap_dates))
                ]
                swap_table = pd.concat(
                    [
                        swap_table,
                        pd.DataFrame(
                            {
                                "swap_tenor": swap_tenor,
                                "swap_basis": swap_basis,
                                "swap_rate": swap_rate,
                                "swap_days": pd.Series(swap_days),
                                "coupon_time": pd.Series(coupon_time),
                            }
                        ),
                    ]
                )

            swap_table.reset_index(drop=True, inplace=True)
            swap_table["foreing_discount_curve"] = 1 / (
                1
                + discount_curve_for[swap_table["swap_days"].values - 1]
                * swap_table["swap_days"].values
                / 360
            )
            swap_table["foreing_projection_curve"] = 1 / (
                1
                + projection_curve_for[swap_table["swap_days"].values - 1]
                * swap_table["swap_days"].values
                / 360
            )

            self.logger.info(
                create_log_msg(
                    "Inicia el proceso de optimizacion de la curva CLP colateral"
                )
            )
            fwd_days = fwd_points.loc[
                fwd_points["days"] < swap_tenor_days[0], "days"
            ].values
            nodes_days = np.append(fwd_days, swap_tenor_days)
            fixed_nodes_values = (
                1 / (1 + discount_curve_for[fwd_days - 1] * fwd_days / 360)
            ) / (
                1
                + fwd_points.loc[fwd_points["days"] < swap_tenor_days[0], "rate"] / spot
            )
            variable_nodes_values = 1 / (1 + swap_rates * swap_tenor_days / 360)
            bnds = [(0, None)] * len(variable_nodes_values)
            optimizacion = optimize.minimize(
                self.clp_curve_usd_basis_bootstrap,
                variable_nodes_values,
                bounds=bnds,
                args=(
                    nodes_days,
                    fixed_nodes_values,
                    swap_table,
                    interpolation_method,
                    interpolation_nodes,
                ),
                tol=1e-16,
            )
            if not optimizacion.success:
                self.logger.error(
                    create_log_msg(
                        "Se genero un error en la optimizacion de la curva cero cupon CLP colateral"
                    )
                )
                self.logger.error(create_log_msg(optimizacion.message))
                raise PlataformError(
                    "Hubo un error en la optimizacion de la curva cero cupon CLP colateral"
                )
            swap_nodes_values = optimizacion.x
            self.logger.info(
                create_log_msg(
                    "Finaliza el proceso de optimizacion de la curva CLP colateral"
                )
            )

            nodes_values = np.append(fixed_nodes_values, swap_nodes_values)
            colateral_curve = curve_function(
                nodes_days,
                nodes_values,
                np.arange(1, max(nodes_days) + 1),
                interpolation_method,
            ).get_curve(interpolation_nodes)

            swap_curve_daily = pd.DataFrame(
                {
                    "days": np.arange(1, len(colateral_curve) + 1),
                    "rate": (1 / colateral_curve - 1)
                    * 360
                    / np.arange(1, len(colateral_curve) + 1),
                }
            )
            swap_curve_nodes = pd.DataFrame(
                {
                    "days": np.append(fwd_days, swap_tenor_days),
                    "rate": (
                        1 / colateral_curve[np.append(fwd_days, swap_tenor_days) - 1]
                        - 1
                    )
                    * 360
                    / np.append(fwd_days, swap_tenor_days),
                }
            )
            self.logger.info(
                create_log_msg(
                    "Finalizo la construccion de la curva cero cupon CLP colateral"
                )
            )
            return swap_curve_nodes, swap_curve_daily
        except (Exception,):
            self.logger.error(
                create_log_msg(
                    "Se genero un error en la creacion de la curva CLP colateral"
                )
            )
            raise PlataformError("Hubo un error en la creacion de la CLP colateral")


# METODOLOGIA: FIN--------------------------------------------------------------


class GlueManager:
    PARAMS = [
        "DB_SECRET",
        "DATANFS_SECRET",
        "LAMBDA_PROCESS",
        # Viene del lambda launcher
        "VALUATION_DATE",
    ]

    @staticmethod
    def get_params() -> dict:
        """Obtiene los parametros de entrada del glue

        Raises:
            PlataformError: Cuando falla la obtencion de parametros

        Returns:
            tuple:
                params: Todos los parametros excepto los nombres de las lambdas
                lbds_dict: Nombres de las lambdas de metodologia
        """
        try:
            logger.info("Obteniendo parametros del glue job ...")
            params = getResolvedOptions(argv, GlueManager.PARAMS)
            logger.info("Obtencion de parametros del glue job exitosa")
            logger.debug("Parametros obtenidos del Glue:%s", params)
            return params
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la obtencion de los parametros del job de glue"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc




class SecretsManager:
    @staticmethod
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
            cliente_secrets_manager = aws_client("secretsmanager")
            secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
            if "SecretString" in secret_data:
                secret_str = secret_data["SecretString"]
            else:
                secret_str = b64decode(secret_data["SecretBinary"])
            logger.info("Se obtuvo el secreto.")
            return json_loads(secret_str)
        except (Exception,) as sec_exc:
            error_msg = f'Fallo al obtener el secreto "{secret_name}"'
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from sec_exc




class CLPcamaraETL:
    DB_TIMEOUT = 2
    FTP_TIMEOUT = 40
    SWI_CHARS_TABLE = "precia_utils_swi_swap_characteristics"
    CURVES_CHARS_TABLE = "precia_utils_swi_curves_characteristics"
    RATES_CHARS_TABLE = "precia_utils_swi_interest_rates_characteristics"
    CALENDAR_TABLE = "precia_utils_calendars"
    SWI_CS_NODES_TABLE = "pub_otc_inter_swap_cross_points_nodes"
    SWI_CS_DAILY_TABLE = "pub_otc_inter_swap_cross_daily"
    SWI_CC_DAILY_TABLE = "pub_otc_inter_swap_cc_daily"
    RATES_TABLE = "pub_exchange_rate_parity"
    FWI_TABLE = "pub_otc_forwards_inter_points_nodes"
    SWI_INPUTS_TABLE = "prc_otc_inter_swap"
    SWI_BASE_RATE = "Camara"
    BASE_CURVE = f"Swap_{SWI_BASE_RATE}"
    PREFIX = "SwapCC"
    CURVE_LABELS = {
        "CLP": "CLP_COLATERAL_USD",
        "CAMARA": "CAMARA_COLATERAL_USD",
    }  # Cambiar nombres de curvas de salida aca, en update_report_process y en payload de lambda trigger de cross
    CURVES_ARCHETYPE = """
    {{
        "CURVENAME": "{prefix}_{ratename}",
        "CURVES_INFO":{{
            "DAILY":{{
                "FILENAME": "{prefix}_{ratename}_Diaria_{filedate}.txt",
                "DATAFRAME": None,
                "DB_TABLE":"{swi_cs_daily_table}"
            }},
            "NODES": {{
                "FILENAME": "{prefix}_{ratename}_Nodos_{filedate}.txt",
                "DATAFRAME": None,
                "DB_TABLE":"{swi_cs_nodes_table}"
            }}
        }}
    }}
    """  # Para construir diccionario -> {'CLP':CURVES_ARCHETYPE,'CAMARA':CURVES_ARCHETYPE}
    # Eliminando las tabulaciones de la identacion de python
    CURVES_ARCHETYPE = "\n".join(line.strip() for line in CURVES_ARCHETYPE.splitlines())

    def __init__(
        self, db_secret: dict, valuation_date: str, datanfs_secret: dict
    ) -> None:
        try:
            self.valuation_date = valuation_date
            self.db_secret = db_secret
            self.datanfs_secret = datanfs_secret
            self.curves_dict = {
                label: eval(
                    CLPcamaraETL.CURVES_ARCHETYPE.format(
                        prefix=CLPcamaraETL.PREFIX,
                        ratename=CLPcamaraETL.CURVE_LABELS[label],
                        filedate=valuation_date.replace("-", ""),
                        swi_cs_daily_table=CLPcamaraETL.SWI_CS_DAILY_TABLE,
                        swi_cs_nodes_table=CLPcamaraETL.SWI_CS_NODES_TABLE,
                    )
                )
                for label in CLPcamaraETL.CURVE_LABELS
            }
            logger.info("curves_dict:\n%s", json_dumps(self.curves_dict))
            self.run_etl()
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion del objeto CLPcamaraETL"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    class ExtractManager:
        def __init__(self, db_secret: dict, valuation_date: str, curves_names) -> None:
            try:
                self.db_secret = db_secret
                self.curves_names = curves_names
                self.valuation_date = valuation_date
                self.utils_engine = create_engine(
                    db_secret["conn_string_sources"] + db_secret["schema_utils"],
                    connect_args={"connect_timeout": CLPcamaraETL.DB_TIMEOUT},
                )
                self.prc_engine = create_engine(
                    db_secret["conn_string_process"] + db_secret["schema_process"],
                    connect_args={"connect_timeout": CLPcamaraETL.DB_TIMEOUT},
                )
                self.sirius_engine = create_engine(
                    db_secret["conn_string_sirius"]
                    + db_secret["schema_sirius_publish"],
                    connect_args={"connect_timeout": CLPcamaraETL.DB_TIMEOUT},
                )
                self.pub_engine = create_engine(
                    db_secret["conn_string_publish"] + db_secret["schema_publish"],
                    connect_args={"connect_timeout": CLPcamaraETL.DB_TIMEOUT},
                )
                self.swap_chars_df = self.get_swap_chars()
                self.curve_chars_df = self.get_curve_chars()
                self.rate_chars_df = self.get_rates_chars()
                self.calendar_serie = self.get_clus_calendar()
                self.swap_inputs_df, self.swap_camara_df = self.get_swaps_inputs()
                self.usdclp_rate = self.get_usdclp_rate()
                self.fwi_usdclp_df = self.get_fwi_usdclp()
                self.last_business_dates = self.get_last_business_dates(
                    calendar_list=self.calendar_serie.to_list(), days_ago=2
                )
                self.on_camara_yesterday = self.get_on_camara(
                    self.last_business_dates[-1]
                )
                self.swapcc_usdois_daily = self.get_usdois_daily()
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto ExtractManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_swap_chars(self):
            try:
                logger.info("Consultando caracteristicas de los swaps en BD...")
                select_query = text(
                    f"""
                    SELECT swap_curve, swap_name,
                    daycount_convention,buyer_leg_rate,seller_leg_rate,buyer_leg_currency,seller_leg_currency,buyer_leg_frequency_payment,seller_leg_frequency_payment,start_type,on_the_run_tenors,bullet_tenors,business_day_convention,business_day_calendar,buyer_leg_projection_curve,seller_leg_projection_curve,buyer_leg_discount_curve,seller_leg_discount_curve,colateral
                    FROM {CLPcamaraETL.SWI_CHARS_TABLE}
                    """
                )
                swap_chars_df = pd.read_sql(select_query, con=self.utils_engine)
                logger.info(
                    "Caracteristicas de los swaps:\n%s",
                    swap_chars_df,
                )
                return swap_chars_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura de las caracteristicas de los swaps en BD"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_curve_chars(self):
            try:
                logger.info("Consultando caracteristicas de las curvas en BD...")
                select_query = text(
                    f"""
                    SELECT curve_name,interpolation_method,interpolation_nodes
                    FROM {CLPcamaraETL.CURVES_CHARS_TABLE}
                    """
                )
                curve_chars_df = pd.read_sql(select_query, con=self.utils_engine)
                logger.info(
                    "Caracteristicas de las curvas:\n%s",
                    curve_chars_df,
                )
                return curve_chars_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = (
                    "Fallo la lectura de las caracteristicas de las curvas en BD"
                )
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_rates_chars(self):
            try:
                logger.info(
                    "Consultando caracteristicas de las tasas de interes en BD..."
                )
                select_query = text(
                    f"""
                    SELECT
                    interest_rate_name,daycount_convention,currency,tenor,start_type,business_day_convention,business_day_calendar
                    FROM {CLPcamaraETL.RATES_CHARS_TABLE} WHERE
                    interest_rate_name = '{CLPcamaraETL.SWI_BASE_RATE}'
                    """
                )
                rate_chars_df = pd.read_sql(select_query, con=self.utils_engine)
                logger.info(
                    "Caracteristicas de la tasa de interes:\n%s",
                    rate_chars_df,
                )
                return rate_chars_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura de las caracteristicas de las tasas de interes en BD"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_clus_calendar(self):
            try:
                logger.info("Consultando los dias habiles del calendario CLUS en BD...")
                select_query = text(
                    f"""
                    SELECT dates_calendar FROM {CLPcamaraETL.CALENDAR_TABLE}
                    WHERE chilean_calendar = 1 AND federal_reserve_calendar = 1
                    """
                )
                calendar_df = pd.read_sql(select_query, con=self.utils_engine)
                calendar_df["dates_calendar"] = calendar_df["dates_calendar"].astype(
                    str
                )
                logger.info("Calendario de dias habiles de CLUS:\n%s", calendar_df)
                return calendar_df["dates_calendar"]
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura del calendario CLUS en BD"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_swaps_inputs(self):
            try:
                logger.info("Consultando las curvas insumo en BD...")
                select_query = text(
                    f"""
                    SELECT curve,tenor, mid_price AS rate FROM
                    {CLPcamaraETL.SWI_INPUTS_TABLE} WHERE curve IN
                    ('Swap_Camara','Swap_CLP') AND valuation_date =
                    '{self.valuation_date}' AND status_info = '1' ORDER BY days
                    """
                )
                swap_inputs_df = pd.read_sql(select_query, con=self.prc_engine)
                swap_camara_df = swap_inputs_df.loc[
                    swap_inputs_df["curve"] == "Swap_Camara", ["tenor", "rate"]
                ].reset_index(drop=True)
                logger.info("Swap_Camara:\n%s", swap_camara_df)
                swap_clp_df = (
                    swap_inputs_df.loc[
                        swap_inputs_df["curve"] == "Swap_CLP", ["tenor", "rate"]
                    ]
                    .rename(columns={"rate": "basis"})
                    .reset_index(drop=True)
                )
                logger.info("Swap_CLP:\n%s", swap_clp_df)
                swap_inputs_df = swap_clp_df.merge(
                    swap_camara_df, on="tenor"
                ).reset_index(drop=True)
                logger.info("swap_inputs_df:\n%s", swap_inputs_df)
                return swap_inputs_df, swap_camara_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura de las curvas insumo en BD"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_usdclp_rate(self):
            try:
                logger.info("Consultando las tasa de cambio USDCLP en BD...")
                select_query = text(
                    f"""
                    SELECT value_rates FROM {CLPcamaraETL.RATES_TABLE} WHERE
                    valuation_date = '{self.valuation_date}' AND status_info =
                    '1' AND id_precia = 'usdclp'
                    """
                )
                result_query = self.sirius_engine.execute(select_query).fetchall()
                if len(result_query) != 0:
                    usdclp_rate = float(result_query[0][0])
                else:
                    raise PlataformError(
                        "No hay informacion de la tasa para la fecha dada"
                    )
                logger.info("Tasa USDCLP: %s", usdclp_rate)
                return usdclp_rate
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura de la tasa de cambio USDCLP en BD"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_last_business_dates(self, calendar_list, days_ago):
            try:
                if self.valuation_date in calendar_list:
                    today_index = calendar_list.index(self.valuation_date)
                else:
                    raise PlataformError(
                        "La fecha de ejecucion no es un dia habil en chile y eeuu"
                    )

                dates_list = [
                    calendar_list[previous_index]
                    for previous_index in range(today_index - days_ago, today_index)
                ]
                logger.info(
                    "%s dia/s habil/es anteriores a %s del calendario CLUS:\n%s",
                    days_ago,
                    self.valuation_date,
                    dates_list,
                )
                return dates_list
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la obtencion de las ultimas fechas habiles del calendario clus"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_on_camara(self, valuation_date):
            try:
                logger.info(
                    "Consultando el nodo ON de swap camara (%s) en BD...",
                    valuation_date,
                )
                select_query = text(
                    f"""
                    SELECT mid_price FROM {CLPcamaraETL.SWI_INPUTS_TABLE} WHERE
                    curve = 'Swap_Camara' AND valuation_date =
                    '{valuation_date}' AND tenor = 'ON'
                    """
                )
                result_query = self.prc_engine.execute(select_query).fetchall()
                if len(result_query) != 0:
                    usdclp_rate = float(result_query[0][0])
                else:
                    raise PlataformError(
                        "No hay informacion del nodo ON de swap_camara para la fecha dada"
                    )
                logger.info(
                    "Nodo ON de swap_camara (%s): %s", valuation_date, usdclp_rate
                )
                return usdclp_rate
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura de la tasa de cambio USDCLP en BD"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_fwi_usdclp(self):
            try:
                logger.info("Consultando la tasa FWD USDCLP en BD...")
                select_query = text(
                    f"""
                    SELECT days_fwd AS days,mid_fwd AS rate FROM
                    {CLPcamaraETL.FWI_TABLE} WHERE instrument_fwd = 'USDCLP'
                    AND valuation_date = '{self.valuation_date}' ORDER BY
                    days_fwd
                    """
                )
                fwi_usdclp_df = pd.read_sql(select_query, con=self.pub_engine)
                logger.info("FWD USDCLP:\n%s", fwi_usdclp_df)
                return fwi_usdclp_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura de la tasa FWD USDCLP en BD"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_usdois_daily(self):
            try:
                logger.info("Consultando la curva diaria SwapCC_USDOIS en BD...")
                select_query = text(
                    f"""
                    SELECT rate FROM {CLPcamaraETL.SWI_CC_DAILY_TABLE} WHERE curve = 'SwapCC_USDOIS' AND valuation_date = '{self.valuation_date}' AND status_info = '1' ORDER by days
                    """
                )
                swapcc_usdois_daily = self.pub_engine.execute(select_query).fetchall()
                swapcc_usdois_daily = np.array(
                    [float(rate_tuple[0]) for rate_tuple in swapcc_usdois_daily]
                )
                logger.info("Curva diaria SwapCC_USDOIS:\n%s", swapcc_usdois_daily)
                return swapcc_usdois_daily
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura de la curva diaria SwapCC_USDOIS en BD"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

    class TransformManager:
        def __init__(
            self,
            swap_chars_df: pd.DataFrame,
            curve_chars_df: pd.DataFrame,
            rate_chars_df: pd.DataFrame,
            calendar_serie: pd.Series,
            swap_inputs_df: pd.DataFrame,
            trade_date: str,
            usdclp_rate: float,
            fwi_usdclp_df: pd.DataFrame,
            swapcc_usdois_daily: np.ndarray,
            swap_camara_df: pd.DataFrame,
            curves_dict: dict,
            last_business_dates: list[str],
            on_camara_yesterday: float,
        ) -> None:
            try:
                self.swap_chars_df = swap_chars_df
                self.curves_dict = curves_dict
                self.curve_chars_df = curve_chars_df
                self.rate_chars_df = rate_chars_df
                self.calendar_serie = calendar_serie
                self.swap_camara_df = self.calculate_on_camara(
                    swap_camara_df=swap_camara_df,
                    on_camara_yesterday=on_camara_yesterday,
                    last_business_dates=last_business_dates,
                )
                self.trade_date = dt.datetime.strptime(trade_date, "%Y-%m-%d").date()
                self.swap_inputs_df = swap_inputs_df
                self.usdclp_rate = usdclp_rate
                self.fwi_usdclp_df = fwi_usdclp_df
                self.swapcc_usdois_daily = swapcc_usdois_daily
                (
                    self.curves_dict["CLP"]["CURVES_INFO"]["NODES"]["DATAFRAME"],
                    self.curves_dict["CLP"]["CURVES_INFO"]["DAILY"]["DATAFRAME"],
                    self.curves_dict["CAMARA"]["CURVES_INFO"]["NODES"]["DATAFRAME"],
                    self.curves_dict["CAMARA"]["CURVES_INFO"]["DAILY"]["DATAFRAME"],
                ) = self.apply_methodology()
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto TransformManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def calculate_on_camara(
            self,
            swap_camara_df: pd.DataFrame,
            on_camara_yesterday,
            last_business_dates,
        ):
            try:
                logger.info(
                    "Calculando nodo ON de swap camara y sustituyendo su valor original..."
                )
                new_swaps_camara_df = swap_camara_df.copy()
                new_swaps_camara_df.set_index("tenor", inplace=True)
                yesterday = dt.datetime.strptime(last_business_dates[-1], "%Y-%m-%d")
                before_yesterday = dt.datetime.strptime(
                    last_business_dates[-2], "%Y-%m-%d"
                )
                on_camara_today = new_swaps_camara_df.at["ON", "rate"]
                new_on_camara = ((on_camara_today / on_camara_yesterday - 1) * 360) / (
                    yesterday - before_yesterday
                ).days
                new_swaps_camara_df.at["ON", "rate"] = new_on_camara
                new_swaps_camara_df.reset_index(inplace=True)
                logger.info("new_on_camara: %s", new_on_camara)
                logger.info("new_swaps_camara_df:\n%s", new_swaps_camara_df)
                return new_swaps_camara_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo el calculo del nodo ON"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def apply_methodology(self):
            try:
                logger.info("Aplicando metodologia...")
                method_funcs = swap_functions(
                    swaps_characteristics=self.swap_chars_df,
                    curves_characteristics=self.curve_chars_df,
                    interest_rates_characteristics=self.rate_chars_df,
                    calendar=self.calendar_serie,
                    logger=logger,
                )
                (
                    clp_colateral_nodes,
                    clp_colateral_daily,
                ) = method_funcs.clp_curve_usd_basis(
                    trade_date=self.trade_date,
                    swaps_info=self.swap_inputs_df,
                    fwd_points=self.fwi_usdclp_df,
                    spot=self.usdclp_rate,
                    discount_curve_for=self.swapcc_usdois_daily,
                    projection_curve_for=self.swapcc_usdois_daily,
                )
                logger.info("clp_colateral_nodes:\n%s", clp_colateral_nodes)
                logger.info("clp_colateral_daily:\n%s", clp_colateral_daily)
                (
                    camara_colateral_nodes,
                    camara_colateral_daily,
                ) = method_funcs.ois_curve(
                    swap_curve=CLPcamaraETL.BASE_CURVE,
                    trade_date=self.trade_date,
                    swaps_info=self.swap_camara_df,
                    discount_curve=1
                    / (
                        1
                        + clp_colateral_daily["rate"]
                        * clp_colateral_daily["days"]
                        / 360
                    ),
                )
                logger.info("camara_colateral_nodes:\n%s", camara_colateral_nodes)
                logger.info("camara_colateral_daily:\n%s", camara_colateral_daily)
                return (
                    clp_colateral_nodes,
                    clp_colateral_daily,
                    camara_colateral_nodes,
                    camara_colateral_daily,
                )
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la aplicacion de la metologia"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

    class LoadManager:
        def __init__(
            self,
            curves_dict: dict,
            valuation_date: str,
            datanfs_secret: dict,
            pub_engine: engine,
        ) -> None:
            try:
                self.curves_dict = curves_dict
                self.valuation_date = valuation_date
                self.datanfs_secret = datanfs_secret
                self.pub_engine = pub_engine
                self.run_load_manager()
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto LoadManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def disable_previous_info(
            self, table: str, curve_type: str, curve_name: str
        ) -> None:
            try:
                logger.info(
                    "Deshabilitando curva %s (%s) en BD publish...",
                    curve_name,
                    curve_type,
                )
                update_query = text(
                    f"""
                    UPDATE {table} SET status_info= 0 WHERE valuation_date =
                    '{self.valuation_date}' AND curve = '{curve_name}'
                    """
                )
                with self.pub_engine.connect() as conn:
                    conn.execute(update_query)
                logger.info(
                    "Dehabilitacion de informacion previa en BD publish exitosa"
                )
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la deshabilitacion de informacion en BD publish"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def insert_df(
            self, table: str, curve_df: pd.DataFrame, curve_type: str, curve_name: str
        ) -> None:
            try:
                logger.info(
                    "Insertando curva %s (%s) en BD publish...", curve_name, curve_type
                )
                full_curve_df = curve_df.copy()
                full_curve_df["valuation_date"] = self.valuation_date
                full_curve_df["curve"] = curve_name
                full_curve_df.to_sql(
                    table,
                    con=self.pub_engine,
                    if_exists="append",
                    index=False,
                )
                logger.info("Insercion de df en BD publish exitosa")
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la insercion del dataframe en BD publish"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def create_and_transfer_file(
            self, curve_df: pd.DataFrame, filename: str
        ) -> None:
            try:
                logger.info(
                    "Creando y transfiriendo el archivo '%s' al datanfs...",
                    filename,
                )
                curve_df = curve_df.round({"rate": 10})
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(
                    hostname=self.datanfs_secret["sftp_host"],
                    port=self.datanfs_secret["sftp_port"],
                    username=self.datanfs_secret["sftp_user"],
                    password=self.datanfs_secret["sftp_password"],
                    timeout=CLPcamaraETL.FTP_TIMEOUT,
                )
                with ssh_client.open_sftp().open(
                    f'{self.datanfs_secret["route_swap"]}{filename}',
                    "w",
                ) as file:
                    file.write(
                        curve_df.to_csv(
                            index=False,
                            header=False,
                            sep=" ",
                            line_terminator="\r\n",
                            encoding="utf-8",
                        )
                    )
                logger.info(
                    "Creacion y transferencia del archivo al datanfs exitosa",
                )
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion y transferencia de archivos"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def run_load_manager(self) -> None:
            try:
                logger.info("Publicando...")

                def publish_curve_type(curve_info, curvename, curvetype):
                    self.disable_previous_info(
                        table=curve_info["DB_TABLE"],
                        curve_type=curvetype,
                        curve_name=curvename,
                    )
                    self.insert_df(
                        table=curve_info["DB_TABLE"],
                        curve_type=curvetype,
                        curve_name=curvename,
                        curve_df=curve_info["DATAFRAME"],
                    )
                    self.create_and_transfer_file(
                        curve_df=curve_info["DATAFRAME"],
                        filename=curve_info["FILENAME"],
                    )

                def publish_curve(curve_dict):
                    list(
                        map(
                            lambda type_label: publish_curve_type(
                                curve_info=curve_dict["CURVES_INFO"][type_label],
                                curvename=curve_dict["CURVENAME"],
                                curvetype=type_label,
                            ),
                            curve_dict["CURVES_INFO"],
                        )
                    )

                list(
                    map(
                        lambda curve_label: publish_curve(
                            self.curves_dict[curve_label]
                        ),
                        self.curves_dict,
                    )
                )
                logger.info("Publicacion exitosa")
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la publicacion en BD y generacion de archivos"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

    def run_etl(self) -> None:
        try:
            extract_manager = CLPcamaraETL.ExtractManager(
                db_secret=self.db_secret,
                valuation_date=self.valuation_date,
                curves_names=self.curves_dict,
            )
            transform_manager = CLPcamaraETL.TransformManager(
                swap_chars_df=extract_manager.swap_chars_df,
                curve_chars_df=extract_manager.curve_chars_df,
                rate_chars_df=extract_manager.rate_chars_df,
                calendar_serie=extract_manager.calendar_serie,
                swap_inputs_df=extract_manager.swap_inputs_df,
                trade_date=self.valuation_date,
                usdclp_rate=extract_manager.usdclp_rate,
                fwi_usdclp_df=extract_manager.fwi_usdclp_df,
                swapcc_usdois_daily=extract_manager.swapcc_usdois_daily,
                swap_camara_df=extract_manager.swap_camara_df,
                curves_dict=self.curves_dict,
                on_camara_yesterday=extract_manager.on_camara_yesterday,
                last_business_dates=extract_manager.last_business_dates,
            )
            self.curves_dict = transform_manager.curves_dict
            CLPcamaraETL.LoadManager(
                valuation_date=self.valuation_date,
                datanfs_secret=self.datanfs_secret,
                curves_dict=self.curves_dict,
                pub_engine=extract_manager.pub_engine,
            )

        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la ejecucion del proceso de ETL"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc


class LambdaManager:
    @staticmethod
    def invoke(
        payload: dict,
        lambda_name: str,
    ) -> None:
        try:
            logger.info("Lanzando ejecución de lambda %s...", lambda_name)
            logger.info("Evento a enviar a la lambda:\n%s", payload)
            lambda_client = aws_client("lambda")
            lambda_response = lambda_client.invoke(
                FunctionName=lambda_name,
                InvocationType="RequestResponse",
                Payload=json_dumps(payload),
            )
            lambda_response_decoded = json_loads(
                lambda_response["Payload"].read().decode()
            )
            logger.info(
                "Respuesta de la lambda:\n%s", json_dumps(lambda_response_decoded)
            )
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = (
                f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
            )
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    @staticmethod
    def update_report_process(
        status,
        lambda_name,
        valuation_date,
        technical_description="",
        description="",
    ):
        try:
            report_payload = {
                "input_id": "SwapCC_CLP_COLATERAL_USD",
                "output_id": [
                    "SwapCC_CAMARA_COLATERAL_USD",
                    "SwapCC_CLP_COLATERAL_USD",
                ],
                "process": "Derivados OTC",
                "product": "swp_inter",
                "stage": "Metodologia",
                "status": status,
                "aws_resource": "glue-p-swi-process-chile-colateral",
                "type": "Archivo",
                "description": description,
                "technical_description": technical_description,
                "valuation_date": valuation_date,
            }
            LambdaManager.invoke(payload=report_payload, lambda_name=lambda_name) 
            logger.info("Se envia el reporte de estado del proceso exitosamente")
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = (
                f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
            )
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc


class ParameterManager:
    PARAMETER_STORE_NAME = "/ps-otc-lambda-reports"

    @staticmethod
    def get_parameter_from_ssm():
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
        ssm_client = aws_client("ssm")
        try:
            response = ssm_client.get_parameter(
                Name=ParameterManager.PARAMETER_STORE_NAME, WithDecryption=True
            )
            return response["Parameter"]["Value"]
        except ssm_client.exceptions.ParameterNotFound:
            logger.error(
                f"El parámetro '{ParameterManager.PARAMETER_STORE_NAME}' no existe en Parameter Store."
            )
        except Exception as e:
            logger.error(
                f"Error al obtener el parámetro '{ParameterManager.PARAMETER_STORE_NAME}' desde r Parameter Store: {e}"
            )


class Main:
    @staticmethod
    def main():
        try:
            logger.info("Ejecutando el job de glue...")
            report_lbd_name = ParameterManager.get_parameter_from_ssm() 

            params_dict = GlueManager.get_params()
            try:
                valuation_date_str = params_dict["VALUATION_DATE"]
                dt.datetime.strptime(
                    valuation_date_str, "%Y-%m-%d"
                )  # Solo para verificar que es una fecha valida
            except (Exception,):
                raise_msg = "Fallo la extraccion de la fecha de los parametros. Se tomara la fecha de hoy en su lugar"
                logger.warning(create_log_msg(raise_msg))
                today_date = dt.now().date()
                valuation_date_str = today_date.strftime("%Y-%m-%d")
            CLPcamaraETL(
                db_secret=SecretsManager.get_secret(params_dict["DB_SECRET"]),
                valuation_date=params_dict["VALUATION_DATE"],
                datanfs_secret=SecretsManager.get_secret(params_dict["DATANFS_SECRET"]),
            )
            payload = {
                "product": "Swap Inter",
                "input_name": [
                    "SwapCC_CAMARA_COLATERAL_USD",
                    "SwapCC_CLP_COLATERAL_USD",
                ],
                "valuation_date": [valuation_date_str],
            }
            
            LambdaManager.invoke(
                lambda_name=params_dict["LAMBDA_PROCESS"], payload=payload
            )
            LambdaManager.update_report_process(
                status="Exitoso",
                lambda_name=report_lbd_name,
                valuation_date=valuation_date_str,
                description="Proceso Finalizado",
            )
            logger.info("Ejecucion del job de glue exitosa!!!")
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la ejecucion del main del job de glue"
            error_msg = create_log_msg(raise_msg)
            logger.critical(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            LambdaManager.update_report_process(
                status="Fallido",
                lambda_name=report_lbd_name,
                valuation_date=valuation_date_str,
                technical_description=str(gen_exc),
                description=first_error_msg,
            )
            raise PlataformError(raise_msg) from gen_exc


if __name__ == "__main__":
    Main().main() 

