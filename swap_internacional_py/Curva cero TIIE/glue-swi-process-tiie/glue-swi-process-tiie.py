# Nativas de Python
import logging
from json import loads as json_loads, dumps as json_dumps
import datetime as dt
from dateutil import relativedelta as rd
from sys import argv, exc_info, stdout
from base64 import b64decode

# AWS
from boto3 import client as aws_client

from awsglue.utils import getResolvedOptions 

# De terceros
import pandas as pd
from sqlalchemy import create_engine
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
Created on Wed Nov  1 16:41:35 2023

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

    def tiie_bootstrap(
        self,
        variable_nodes_values,
        nodes_days,
        fixed_nodes_values,
        on_value,
        effective_days,
        swap_table,
        interpolation_method,
        interpolation_nodes,
        discount_curve,
    ):
        """
        Calcula el error bootstrap para la curva swap tiie apartir de los swap plain vanilla
        Params:
            variable_nodes_values (numpy.ndarray): Factores de descuento de cada nodo
            nodes_days (numpy.ndarray): Días correspondientes a los nodos de la curva
            fixed_nodes_values (numpy.ndarray): Factor de descuento TIIE ON y 1X1
            on_value: (float): Factor de descuento TIIE
            effective_days (int): Dias entre la fecha de negociacion y la fecha efectiva
            swap_table (pd.DataFrame): Tabla que contiene la información relevante de cada swap (tenor, tasa par, dias del cupon, tiempo entre cupones)
            interpolation_method (str): Metodo de interpolación utilizado (linear_interpol, cubic_splines_interpol)
            interpolation_nodes (str): Valores sobre los que se interpola (nominal_rates, discount_factors, log_discount_factors)
        Return:
            error (float): Suma de errores al cuadrado
        """
        try:
            nodes_values = np.append(fixed_nodes_values, variable_nodes_values)
            nodes_values_t0 = nodes_values * on_value
            nodes_days_t0 = np.array(nodes_days) + effective_days

            swap_table["proyection_curve"] = curve_function(
                nodes_days,
                nodes_values,
                swap_table["projection_days"].values,
                interpolation_method,
            ).get_curve(interpolation_nodes)
            swap_table["fra_rate_value"] = (
                swap_table["proyection_curve"].shift(1, fill_value=1)
                / swap_table["proyection_curve"]
                - 1
            )
            if discount_curve is None:
                swap_table["discount_curve"] = curve_function(
                    nodes_days_t0,
                    nodes_values_t0,
                    swap_table["discount_days"].values,
                    interpolation_method,
                ).get_curve(interpolation_nodes)

            float_leg = (
                swap_table["fra_rate_value"] * swap_table["discount_curve"]
            ).cumsum()[swap_table["last_payment"]]
            fix_leg = (
                (swap_table["coupon_time"] * swap_table["discount_curve"]).cumsum()[
                    swap_table["last_payment"]
                ]
            ) * swap_table["rate"][swap_table["last_payment"]]

            swap_val = (float_leg - fix_leg) * 10000
            bootstrap_error = np.sqrt(np.dot(swap_val, swap_val)) / 10000
            return bootstrap_error
        except (Exception,):
            self.logger.error(create_log_msg(self._bootstrap_error_message))
            raise PlataformError(self._bootstrap_error_raise_message)

    def tiie_curve(self, trade_date, tiie_info, discount_curve=None):
        """
        Construye la curva ois cero cupon TIIE a partir de información de swaps plain vanilla

        Params
        ----------
        trade_date (datetime.date): Fecha de valoración.
        swaps_info (pd.dataframe): Información de los swaps utiizados en la construcción de la curva cero
                                   tenor:  Tenores tenidos en cuenta de los swaps, incluido el tenor overnight.
                                   rate: Tasas par de los swaps, incluida la tasa overnight.
        discount_curve (np.ndarray): Curva de descuento diaria a utilizar, expresa en factores de descuento
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
            swap_curve = "SwapCC_TIIE"
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
            on_starting_type_convention = interest_rate_characteristics["start_type"][0]
            on_business_day_calendar = [
                dt.datetime.strptime(x, self._date_format).date() for x in self.calendar
            ]
            on_daycount_convention = interest_rate_characteristics[
                "daycount_convention"
            ][0]

            # Fixed nodes handling
            effective_date = DateUtils().starting_date(
                trade_date, swap_business_day_calendar, swap_starting_type_convention
            )
            effective_days = (effective_date - trade_date).days
            on_rate = tiie_info.loc[tiie_info["tenor"] == "ON", "rate"][0]
            on_date = DateUtils().tenor_date(
                trade_date,
                "ON",
                on_business_day_calendar,
                on_starting_type_convention,
                on_business_day_convention,
            )
            on_value = 1 / (
                1
                + on_rate
                * DateUtils().day_count(trade_date, on_date, on_daycount_convention)
            )
            tiie28_rate = tiie_info.loc[
                tiie_info["tenor"] == "1x1", "rate"
            ].reset_index(drop=True)[0]
            tiie28_date = DateUtils().tenor_date(
                effective_date,
                "1x1",
                on_business_day_calendar,
                on_starting_type_convention,
                on_business_day_convention,
            )
            tiie28_day = (tiie28_date - effective_date).days
            tiie28_value = 1 / (
                1
                + tiie28_rate
                * DateUtils().day_count(
                    effective_date, tiie28_date, on_daycount_convention
                )
            )

            # Swap handling
            swap_tenors = tiie_info.loc[
                np.where(np.logical_not(tiie_info["tenor"].isin(["ON", "1x1"]))),
                "tenor",
            ].values
            swap_rates = tiie_info.loc[
                np.where(np.logical_not(tiie_info["tenor"].isin(["ON", "1x1"]))), "rate"
            ].values
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
                x.days for x in (np.array(swap_tenor_dates) - effective_date)
            ]
            self.logger.info(create_log_msg(self._flows_log_message))
            swap_table = pd.DataFrame()
            swap_last_dates = np.array([])
            last_payment_date = None

            swaps_dates = DateUtils().tenor_sequence_dates(
                trade_date,
                swap_tenors[-1],
                frequency_payment,
                swap_business_day_calendar,
                swap_starting_type_convention,
                swap_business_day_convention,
            )[1:]
            for swap_tenor in swap_tenors:
                # Looping though swap tenors
                swap_rate = swap_rates[np.where(swap_tenors == swap_tenor)][0]
                frequency = (
                    swap_tenor if swap_tenor in bullet_tenors else frequency_payment
                )
                tenor_specs = DateUtils().tenor_specs(swap_tenor)
                frequency_specs = DateUtils().tenor_specs(frequency)
                if tenor_specs["period"] == frequency_specs["period"]:
                    tenor_count = int(tenor_specs["length"] / frequency_specs["length"])
                elif tenor_specs["period"] == "Y" and frequency_specs["period"] == "M":
                    tenor_count = int(
                        tenor_specs["length"] * 12 / frequency_specs["length"]
                    )
                swap_dates = swaps_dates[:tenor_count]
                if not swap_table.empty:
                    swap_dates = swap_dates[
                        np.where(
                            np.logical_not(pd.Series(swap_dates).isin(swap_last_dates))
                        )
                    ]
                swap_last_dates = np.append(swap_last_dates, swap_dates)
                swap_days_discount = [x.days for x in (swap_dates - trade_date)]
                swap_days_projection = [x.days for x in (swap_dates - effective_date)]

                if swap_table.empty:
                    default_date = effective_date
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
                                "tenor": swap_tenor,
                                "rate": swap_rate,
                                "projection_days": pd.Series(swap_days_projection),
                                "discount_days": pd.Series(swap_days_discount),
                                "coupon_time": pd.Series(coupon_time),
                                "last_payment": last_payment,
                            }
                        ),
                    ]
                )
            swap_table.reset_index(drop=True, inplace=True)
            if discount_curve is not None:
                swap_table["discount_curve"] = curve_function(
                    np.arange(1, len(discount_curve) + 1),
                    discount_curve.values,
                    swap_table["discount_days"].values,
                    interpolation_method,
                ).get_curve(interpolation_nodes)
            self.logger.info(
                create_log_msg(
                    f"Inicia el proceso de optimizacion de la curva {swap_curve}"
                )
            )
            nodes_days = [tiie28_day] + swap_tenor_days
            fixed_nodes_values = tiie28_value
            variable_nodes_values = 1 / (1 + swap_rates * swap_tenor_days / 360)
            bnds = [(1e-6, None)] * len(variable_nodes_values)
            optimizacion = optimize.minimize(
                self.tiie_bootstrap,
                variable_nodes_values,
                bounds=bnds,
                args=(
                    nodes_days,
                    fixed_nodes_values,
                    on_value,
                    effective_days,
                    swap_table,
                    interpolation_method,
                    interpolation_nodes,
                    discount_curve,
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
                    "Hubo un error en la optimizacion de la curva cero cupon {swap_curve}"
                )
            swap_nodes_values = optimizacion.x
            self.logger.info(
                create_log_msg(
                    f"Finalizo el proceso de optimizacion de la curva {swap_curve}"
                )
            )

            nodes_values = np.concatenate(
                (
                    np.array([on_value]),
                    np.array([tiie28_value * on_value]),
                    np.array(swap_nodes_values) * on_value,
                )
            )
            nodes_days_curve = np.append(
                np.array([effective_days]), np.array(nodes_days) + effective_days
            )
            ois_curve = curve_function(
                nodes_days_curve,
                nodes_values,
                np.arange(1, max(nodes_days_curve) + 1),
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
            swap_curve_nodes = swap_curve_daily.iloc[nodes_days_curve - 1, :]
            self.logger.info(
                create_log_msg(
                    f"Finalizo la construccion de la curva cero cupon {swap_curve}"
                )
            )
            return swap_curve_nodes, swap_curve_daily
        except (Exception,):
            self.logger.error(
                create_log_msg("Se genero un error en la creacion de la curva TIIE")
            )
            raise PlataformError("Hubo un error en la creacion de la curva TIIE")


# METODOLOGIA: FIN--------------------------------------------------------------


class GlueManager:
    PARAMS = [
        "DB_SECRET",
        "DATANFS_SECRET",
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




class TieeETL:
    DB_TIMEOUT = 2
    FTP_TIMEOUT = 40
    SWI_CHARS_TABLE = "precia_utils_swi_swap_characteristics"
    CURVES_CHARS_TABLE = "precia_utils_swi_curves_characteristics"
    RATES_CHARS_TABLE = "precia_utils_swi_interest_rates_characteristics"
    CALENDAR_TABLE = "precia_utils_calendars"
    SWI_CC_NODES_TABLE = "pub_otc_inter_swap_cc_points_nodes"
    SWI_CC_DAILY_TABLE = "pub_otc_inter_swap_cc_daily"
    SWI_INPUTS_TABLE = "prc_otc_inter_swap"
    RATE_NAME = "Tiie"
    CURVE_NAME = f"SwapCC_{RATE_NAME}"

    def __init__(
        self, db_secret: dict, valuation_date: str, datanfs_secret: dict
    ) -> None:
        try:
            self.valuation_date = valuation_date
            self.db_secret = db_secret
            self.datanfs_secret = datanfs_secret
            self.run_etl()
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion del objeto SwapCCtieeETL"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    class ExtractManager:
        def __init__(self, db_secret: dict, valuation_date: str) -> None:
            try:
                self.db_secret = db_secret
                self.valuation_date = valuation_date
                self.utils_engine = create_engine(
                    db_secret["conn_string_sources"] + db_secret["schema_utils"],
                    connect_args={"connect_timeout": TieeETL.DB_TIMEOUT},
                )
                self.prc_engine = create_engine(
                    db_secret["conn_string_process"] + db_secret["schema_process"],
                    connect_args={"connect_timeout": TieeETL.DB_TIMEOUT},
                )
                self.swap_chars_df = self.get_swap_chars()
                self.curve_chars_df = self.get_curve_chars()
                self.rate_chars_df = self.get_rates_chars()
                self.calendar_serie = self.get_mxn_calendar()
                self.swap_tiie_df = self.get_swap_tiie()

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
                    FROM {TieeETL.SWI_CHARS_TABLE} WHERE swap_curve =
                    '{TieeETL.CURVE_NAME}'
                    """
                )
                swap_chars_df = pd.read_sql(select_query, con=self.utils_engine)
                logger.info(
                    "Caracteristicas del swap:\n%s",
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
                    FROM {TieeETL.CURVES_CHARS_TABLE} WHERE curve_name =
                    '{TieeETL.CURVE_NAME}'
                    """
                )
                curve_chars_df = pd.read_sql(select_query, con=self.utils_engine)
                logger.info(
                    "Caracteristicas de la curva:\n%s",
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
                    FROM {TieeETL.RATES_CHARS_TABLE} WHERE
                    interest_rate_name = '{TieeETL.RATE_NAME}'
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

        def get_mxn_calendar(self):
            try:
                logger.info(
                    "Consultando los dias habiles del calendario de mexico en BD..."
                )
                select_query = text(
                    f"""
                    SELECT dates_calendar FROM {TieeETL.CALENDAR_TABLE}
                    WHERE mexican_calendar = 1
                    """
                )
                calendar_df = pd.read_sql(select_query, con=self.utils_engine)
                calendar_df["dates_calendar"] = calendar_df["dates_calendar"].astype(
                    str
                )
                logger.info("Calendario de dias habiles de mexico:\n%s", calendar_df)
                return calendar_df["dates_calendar"]
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura del calendario de mexico en BD"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_swap_tiie(self):
            try:
                logger.info("Consultando las tasas de la curva Swap TIIE en BD...")
                select_query = text(
                    f"""
                    SELECT tenor, mid_price AS rate FROM
                    {TieeETL.SWI_INPUTS_TABLE} WHERE curve =
                    'Swap_TIIE' AND valuation_date = '{self.valuation_date}'
                    AND status_info = '1' ORDER BY days
                    """
                )
                swap_tiie_df = pd.read_sql(select_query, con=self.prc_engine)
                swap_tiie_df["tenor"] = (
                    swap_tiie_df["tenor"].str.replace("M", "x1").replace("28D", "1x1")
                )
                logger.info("Swap TIIE:\n%s", swap_tiie_df)
                return swap_tiie_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la lectura de la curva insumo Swap TIIE en BD"
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
            swap_tiie_df: pd.DataFrame,
            trade_date: str,
        ) -> None:
            try:
                self.swap_chars_df = swap_chars_df
                self.curve_chars_df = curve_chars_df
                self.rate_chars_df = rate_chars_df
                self.calendar_serie = calendar_serie
                self.swap_tiie_df = swap_tiie_df
                self.trade_date = dt.datetime.strptime(trade_date, "%Y-%m-%d").date()
                (
                    self.swapcc_tiee_nodes,
                    self.swapcc_tiee_daily,
                ) = self.apply_methodology()
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto TransformManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def apply_methodology(self) -> tuple[pd.DataFrame, pd.DataFrame]:
            try:
                logger.info("Aplicando metodologia...")
                method_funcs = swap_functions(
                    swaps_characteristics=self.swap_chars_df,
                    curves_characteristics=self.curve_chars_df,
                    interest_rates_characteristics=self.rate_chars_df,
                    calendar=self.calendar_serie,
                    logger=logger,
                )
                swapcc_tiee_nodes, swapcc_tiee_daily = method_funcs.tiie_curve(
                    trade_date=self.trade_date, tiie_info=self.swap_tiie_df
                )
                logger.info("SwapCC_TIIE_nodes:\n%s", swapcc_tiee_nodes)
                logger.info("SwapCC_TIIE_daily:\n%s", swapcc_tiee_daily)
                return swapcc_tiee_nodes, swapcc_tiee_daily
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
            swapcc_tiee_daily: pd.DataFrame,
            swapcc_tiee_nodes: pd.DataFrame,
            valuation_date: str,
            db_secret: dict,
            datanfs_secret: dict,
        ) -> None:
            try:
                self.swapcc_tiee_daily = swapcc_tiee_daily
                self.swapcc_tiee_nodes = swapcc_tiee_nodes
                self.valuation_date = valuation_date
                self.datanfs_secret = datanfs_secret
                self.pub_engine = create_engine(
                    db_secret["conn_string_publish"] + db_secret["schema_publish"],
                    connect_args={"connect_timeout": TieeETL.DB_TIMEOUT},
                )
                self.file_date = self.valuation_date.replace("-", "")
                self.run_load_manager()
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto LoadManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def disable_previous_info(self, table, curve_type: str) -> None:
            try:
                logger.info("Deshabilitando curva (%s) en BD publish...", curve_type)
                update_query = text(
                    f"""
                    UPDATE {table} SET status_info= 0 WHERE valuation_date =
                    '{self.valuation_date}' AND curve = 'SwapCC_Tiie'
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
            self, table: str, curve_df: pd.DataFrame, curve_type: str
        ) -> None:
            try:
                logger.info("Insertando curva (%s) en BD publish...", curve_type)
                full_curve_df = curve_df.copy()
                full_curve_df["valuation_date"] = self.valuation_date
                full_curve_df["curve"] = TieeETL.CURVE_NAME
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
            self, curve_df: pd.DataFrame, curve_type: str
        ) -> None:
            try:
                logger.info(
                    "Creando y transfiriendo el archivo (%s) al datanfs...",
                    curve_type,
                )
                curve_df = curve_df.round({"rate": 10})
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(
                    hostname=self.datanfs_secret["sftp_host"],
                    port=self.datanfs_secret["sftp_port"],
                    username=self.datanfs_secret["sftp_user"],
                    password=self.datanfs_secret["sftp_password"],
                    timeout=TieeETL.FTP_TIMEOUT,
                )
                with ssh_client.open_sftp().open(
                    f'{self.datanfs_secret["route_swap"]}{TieeETL.CURVE_NAME}_{curve_type}_{self.file_date}.txt',
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
                self.disable_previous_info(
                    table=TieeETL.SWI_CC_NODES_TABLE, curve_type="Nodos"
                )
                self.insert_df(
                    table=TieeETL.SWI_CC_NODES_TABLE,
                    curve_df=self.swapcc_tiee_nodes,
                    curve_type="Nodos",
                )
                self.create_and_transfer_file(
                    curve_df=self.swapcc_tiee_nodes, curve_type="Nodos"
                )
                self.disable_previous_info(
                    table=TieeETL.SWI_CC_DAILY_TABLE, curve_type="Diaria"
                )
                self.insert_df(
                    table=TieeETL.SWI_CC_DAILY_TABLE,
                    curve_df=self.swapcc_tiee_daily,
                    curve_type="Diaria",
                )
                self.create_and_transfer_file(
                    curve_df=self.swapcc_tiee_daily, curve_type="Diaria"
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
            extract_manager = TieeETL.ExtractManager(
                db_secret=self.db_secret, valuation_date=self.valuation_date
            )
            transform_manager = TieeETL.TransformManager(
                swap_chars_df=extract_manager.swap_chars_df,
                curve_chars_df=extract_manager.curve_chars_df,
                rate_chars_df=extract_manager.rate_chars_df,
                calendar_serie=extract_manager.calendar_serie,
                swap_tiie_df=extract_manager.swap_tiie_df,
                trade_date=self.valuation_date,
            )
            TieeETL.LoadManager(
                swapcc_tiee_daily=transform_manager.swapcc_tiee_daily,
                swapcc_tiee_nodes=transform_manager.swapcc_tiee_nodes,
                valuation_date=self.valuation_date,
                db_secret=self.db_secret,
                datanfs_secret=self.datanfs_secret,
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
                "input_id": "SwapCC_TIIE",
                "output_id": ["SwapCC_TIIE"],
                "process": "Derivados OTC",
                "product": "swp_inter",
                "stage": "Metodologia",
                "status": status,
                "aws_resource": "glue-p-swi-process-tiie",
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
            TieeETL(
                db_secret=SecretsManager.get_secret(params_dict["DB_SECRET"]),
                valuation_date=valuation_date_str,
                datanfs_secret=SecretsManager.get_secret(params_dict["DATANFS_SECRET"]),
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

