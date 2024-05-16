"""
=============================================================

Nombre: glue-job-hzdl-etl-process.py
Tipo: Glue Job

Autores:
    - Lorena Julieth Torres Hernández
Tecnología - Precia

    --Metodologia
    - Sebastian Velez Hernandez
Investigacion y desarrollo - Precia



Ultima modificación: 03/01/2024

En el presente script se realiza la creacion de la curva de 
hazzard local, almacenamiento en la base de datos de publish.
=============================================================
"""


#Nativas de Python
import base64
import calendar
from dateutil import rrule
from datetime import date
import logging
import json
import re
import sys
from sys import exc_info, stdout
from sqlalchemy import create_engine
import sqlalchemy as sa


#AWS
import boto3
from awsglue.utils import getResolvedOptions 


#De terceros
from dateutil import relativedelta as rd
import datetime as dt
import pandas as pd
import numpy as np
from scipy import  interpolate


date_format = "%Y-%m-%d"
_bootstrap_error_message = 'Fallo el calculo del error bootstrap'
_bootstrap_error_raise_message ="No se pudo calcular el error bootstrap para la curva con los insumos dados"

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


# Class representing a date manager for financial dates
class DateUtils():
    """
    Clase que representa un gestor de fechas para fechas financieras
    Fecha: 2022-06-13
    """
    # -----Attributes-----
    # Class attributes
    _tenor_conventions = {
        'D': ['D', 1],
        'ON': ['D', 1],
        'TN': ['D', 2],
        'SW': ['D', 7],
        'W': ['D', 7],
        'M': ['M', 1],
        'B': ['M', 2],
        'Q': ['M', 3],
        'H': ['M', 6],
        'Y': ['Y', 1]}
    # -----Methods-----
    def date_sequence(self, initial_date, final_date):
        """
        Calcular la secuencia de datetime.dates entre dos fechas dadas
        Fecha: 2020-06-26
        :param initial_date: Fecha inicial en formato datetime.date
        :param final_date: Fecha final en formato datetime.date
        :return: Lista con la secuencia de fechas ()
        """
        num_days = (final_date - initial_date).days + 1
        date_seq = [initial_date + dt.timedelta(days=x) for x in range(num_days)]
        return date_seq


    def is_leap(self, year):
        """
        Calcular booleano que indica si el agno dado es bisiesto (True) o no (False)
        Fecha: 2021-02-02
        :param year: Agno
        :return: Booleano que indica si el agno dado es bisiesto (True) o no (False)
        """
        if year % 4 == 0 and (not (year % 100 == 0) or year % 400 == 0):
            return True
        else:
            return False


    def is_leap_dates(self, date_ini, date_end):
        """
        Calcular el numero de agnos bisiestos entre dos fechas
        Fecha: 2021-02-02
        :param date_ini: Fecha inicial del calendario en formato datetime.date
        :param date_end: Fecha final del calendario en formato datetime.date
        :return: Numero de agnos bisiestos entre dos fechas
        # Desarrollo
         1. Si el agno de la fecha inicial es bisiesto y la fecha de incio es antes del 29 de febrero,
            suma 1 al conteo de bisiestos. En el caso contrario no se suma porque ya paso el bisiesto.
         2. Si el agno de la fecha final es bisiesto y la fecha final es despues del 29 de febrero,
            suma 1 al conteo de bisiestos. En el caso contrario no se suma porque no alcanza a llegar al bisiesto.
         Estas validaciones se realizan porque se toma como base el dia del calculo.
         3. En el ciclo se valida que el year de la fecha inicial sea menor que el year de la fecha final
           Si el agno es bisiesto suma  366 dias al agno de la fecha inicial de lo contrario solo suma 365
           Si el agno es bisiesto suma 1 al conteo de bisiestos
        """
        subst_year = 0
        if (DateUtils.is_leap(self, date_ini.year)):
            date_year_leap_ini = dt.date(date_ini.year, 2, 29)
            if (date_ini < date_year_leap_ini):
                subst_year = subst_year + 1
        else:
            subst_year = 0
        if (DateUtils.is_leap(self, date_end.year) and date_end.year > date_ini.year):
            date_year_leap = dt.date(date_end.year, 2, 29)
            if (date_end >= date_year_leap):
                subst_year = subst_year + 1
        else:
            subst_year = 0


        date_betw = date_ini + dt.timedelta(days=365)
        if DateUtils.is_leap(self, date_end.year):
            date_end = date_end + dt.timedelta(days=-366)
        else:
            date_end = date_end + dt.timedelta(days=-365)

        while int(date_betw.year) < int(date_end.year):  # 2020 < 2024
            if (DateUtils.is_leap(self, date_betw.year)):
                days_add = 366
            else:
                days_add = 365

            date_betw = date_betw + dt.timedelta(days=days_add)
            if (DateUtils.is_leap(self, date_betw.year)):
                subst_year = subst_year + 1

        return subst_year


    def previous_business_date(self, date, calendar):
        """
        Calcular la fecha habil anterior
        Fecha: 2020-06-26
        :param date: Fecha de inicio en formato datetime.date
        :param calendar: Calendario de dias habiles
        :return: Fecha habil anterior en formato datetime.date
        """
        offset = -1
        flag = True
        while flag:
            offset_date = date + dt.timedelta(days=offset)
            if (offset_date in calendar):
                flag = False
            offset = offset - 1
        previous_business_date = offset_date
        return previous_business_date


    def following_business_date(self, date, calendar):
        """
        Calcular la siguiente fecha habil
        Fecha: 2022-06-13
        :param date: Fecha de inicio en formato datetime.date
        :param calendar: Calendario de dias habiles
        :return: Siguiente fecha habil anterior en formato datetime.date
        """
        offset = 1
        flag = True
        while flag:
            offset_date = date + dt.timedelta(days=offset)
            if (offset_date in calendar):
                flag = False
            offset = offset + 1
        following_business_date = offset_date
        return following_business_date


    def add_business_days(self, date, days, calendar):
        """
        Calcular la fecha correspondiente de agregar un numero determinado de dias habiles a una fecha de referencia
        Fecha: 2022-06-13
        :param date: Fecha de referencia en formato datetime.date
        :param days: Numero de fechas laborales
        :param calendar: Calendario dias habiles
        :return: Fecha correspondiente al agregar dias habiles en formato datetime.date
        """
        offset_date = date
        if (days > 0):
            for i in range(days):
                offset_date = self.following_business_date(self= self, date=offset_date, calendar=calendar)
        elif (days < 0):
            for i in range(-days):
                offset_date = self.previous_business_date(offset_date, calendar)
        else:
            business_days = self.business_days(offset_date, offset_date, calendar)
            if not (offset_date in business_days):
                msg = 'No se pueden agregar cero dias habiles a una fecha no habil.'
                raise ValueError(msg)
        business_date = offset_date
        return business_date


    def no_adjustment_business_day_convention(self, date, calendar):
        """
        Calcular la fecha de pago segun una convencion de no ajuste (No Adjustment convention)
        Fecha: 2022-06-13
        :param date: Fecha de pago en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: La fecha de pago en formato datetime.date
        """
        return date


    def previous_business_day_convention(self, date, calendar):
        """
        Calcular la fecha de pago segun una convencion anterior (Previous convention)
        Fecha: 2022-06-13
        :param date: Fecha de pago en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: La fecha de pago en formato datetime.date
        """
        cal = np.array(calendar)
        previous_business_day_convention = max(cal[np.where(cal <= date)])        
        return previous_business_day_convention


    def following_business_day_convention(self, date, calendar):
        """
        Calcular la fecha de pago segun una convencion Siguiente  (Following convention)
        Fecha: 2022-06-13
        :param date: Fecha de pago en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: La fecha de pago en formato datetime.date
        """
        cal = np.array(calendar)
        following_business_day_convention = min(cal[np.where(cal >= date)])
        return following_business_day_convention


    def modified_previous_business_day_convention(self, date, calendar):
        """
        Calcular la fecha de pago segun una convencion anterior modificada (Modified Previous convention)
        Fecha: 2020-06-26
        :param date: Fecha de pago en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: La fecha de pago en formato datetime.date
        """
        cal = np.array(calendar)
        following_business_day = min(cal[np.where(cal >= date)])
        previous_business_day = max(cal[np.where(cal <= date)])
        if (date.month == previous_business_day.month):
            modified_previous_business_day_convention = previous_business_day
        else:
            modified_previous_business_day_convention = following_business_day
        return modified_previous_business_day_convention


    def modified_following_business_day_convention(self, date, calendar):
        """
        Calcular la fecha de pago segun una convencion siguiente modificada (Modified Following convention)
        Fecha: 2020-06-26
        :param date: Fecha de pago en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: La fecha de pago en formato datetime.date
        """
        cal = np.array(calendar)
        following_business_day = min(cal[np.where(cal >= date)])
        previous_business_day = max(cal[np.where(cal <= date)])
        if (date.month == following_business_day.month):
            modified_following_business_day_convention = following_business_day
        else:
            modified_following_business_day_convention = previous_business_day
        return modified_following_business_day_convention


    def end_of_month_business_day_convention(self, date, calendar):
        """
        Calcular el ultimo dia del mes para una fecha dada
        Fecha: 2020-06-26
        :param date: Fecha de pago en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: Ultimo dia del mes
        """
        first_date_next_month = date.replace(day=28) + dt.timedelta(days=4)  # this will never fail
        end_of_month_business_day_convention = first_date_next_month - dt.timedelta(days=first_date_next_month.day)
        return end_of_month_business_day_convention


    def end_of_month_no_adjustment_business_day_convention(self, date, calendar):
        """
        Calcular la fecha de pago bajo una convencion de fin de mes sin ajuste (No Adjustment convention)
        Fecha: 2020-06-26
        :param date: Fecha de pago en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: Fecha de pago en formato datetime.date
        """
        end_of_month_no_adjustment_business_day_convention = self.end_of_month_business_day_convention(date,
                                                                                                       calendar)
        return end_of_month_no_adjustment_business_day_convention


    def end_of_month_previous_business_day_convention(self, date, calendar):
        """
        Calcular la fecha de pago bajo una convencion de fin de mes anterior (Previous convention)
        Fecha: 2020-06-26
        :param date: Fecha de pago en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: Fecha de pago en formato datetime.date
        """
        end_of_month_day_convention = self.end_of_month_business_day_convention(date, calendar)
        previous_business_day = self.previous_business_day_convention(end_of_month_day_convention, calendar)
        following_business_day = self.following_business_day_convention(end_of_month_day_convention, calendar)
        if (end_of_month_day_convention.month == previous_business_day.month):
            modified_previous_business_day_convention = previous_business_day
        else:
            modified_previous_business_day_convention = following_business_day
        return modified_previous_business_day_convention


    def end_of_month_following_business_day_convention(self, date, calendar):
        """
        Calcular la fecha de pago bajo una convencion de fin de mes siguiente (Following convention)
        Fecha: 2020-06-26
        :param date: Fecha de pago en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: Fecha de pago en formato datetime.date
        """
        end_of_month_day_convention = self.end_of_month_business_day_convention(date, calendar)
        following_business_day = self.following_business_day_convention(end_of_month_day_convention, calendar)
        previous_business_day = self.previous_business_day_convention(end_of_month_day_convention, calendar)
        if (end_of_month_day_convention.month == following_business_day.month):
            modified_following_business_day_convention = following_business_day
        else:
            modified_following_business_day_convention = previous_business_day
        return modified_following_business_day_convention


    def business_date(self, date, calendar, business_day_convention):
        """
        Calcular la fecha de pago de un contrato con la convencion de dias habiles dada.
        Fecha: 2020-06-26
        :param date: Fecha de negociacion en formato datetime.date
        :param calendar: Calendario dias habiles
        :param business_day_convention: Nombre de la convencion de dias habiles
        :return: Fecha de pago correspondiente en formato datetime.date
        """
        switcher = {
            'no_adjustment': self.no_adjustment_business_day_convention,
            'previous': self.previous_business_day_convention,
            'following': self.following_business_day_convention,
            'modified': self.modified_previous_business_day_convention,
            'modified_following': self.modified_following_business_day_convention,
            'end_of_month_no_adjustment': self.end_of_month_no_adjustment_business_day_convention,
            'end_of_month_previous': self.end_of_month_previous_business_day_convention,
            'end_of_month_following': self.end_of_month_following_business_day_convention
        }
        case = switcher.get(business_day_convention, 'error')
        if (case == 'error'):
            msg = ' starting_day_convention invalida %s' % business_day_convention
            raise ValueError(msg)
        else:
            return case(self = self,date=date, calendar=calendar)


    def days_business(self, initial_date, final_date, calendar):
        """
        Calcular el numero de dias habiles entre dos fechas
        Fecha: 2022-06-13
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: Numero de dias habiles entre las dos fechas
        """
        days_business = len(self.business_days(initial_date, final_date, calendar))
        return days_business


    def days_act(self, initial_date, final_date):
        """
        Calcular el numero de dias entre dos fechas
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Numero de dias entre las dos fechas
        """
        days_act = (final_date - initial_date).days
        return days_act


    def days_non_leap(self, initial_date, final_date):
        """
        Calcular el numero de dias entre dos fechas bajo una convencion de agnos no bisiesto
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Numero de dias entre las dos fechas bajo la convencion de agno no bisiesto
        """
        days_act = (final_date - initial_date).days
        rs = rrule.rruleset()
        rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=initial_date + dt.timedelta(days=1), until=final_date, bymonth=2,
                             bymonthday=29))
        leap_days = len(list(rs))
        days_non_leap = days_act - leap_days
        return days_non_leap


    def days_30_u(self, initial_date, final_date):
        """
        Calcular el numero de dias entre dos fechas bajo una convencion de 30 dias por mes de EE.UU.
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Numero de dias entre las dos fechas bajo la convencion de 30 dias por mes de EE.UU.
        """
        y_1 = initial_date.year
        y_2 = final_date.year
        m_1 = initial_date.month
        m_2 = final_date.month
        d_1 = min(initial_date.day, 30)
        if d_1 == 30:
            d_2 = final_date.day
        else:
            d_2 = min(final_date.day, 30)
        days_30_u = 360 * (y_2 - y_1) + 30 * (m_2 - m_1) + (d_2 - d_1)
        return days_30_u


    def days_30_e(self, initial_date, final_date):
        """
        Calcular el numero de dias entre dos fechas bajo una convencion europea de 30 dias por mes.
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Numero de dias entre las dos fechas bajo la convencion europea de 30 dias por mes.
        """
        y_1 = initial_date.year
        y_2 = final_date.year
        m_1 = initial_date.month
        m_2 = final_date.month
        d_1 = min(initial_date.day, 30)
        d_2 = min(final_date.day, 30)
        days_30_e = 360 * (y_2 - y_1) + 30 * (m_2 - m_1) + (d_2 - d_1)
        return days_30_e


    def days_30_co_2(self, initial_date, final_date):
        """
        Calcular el numero de dias entre dos fechas bajo una convencion colombiana, no verificada, de 30 dias por mes.
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Numero de dias entre las dos fechas bajo la convencion colombiana de 30 dias por mes.
        """
        y_1 = initial_date.year
        y_2 = final_date.year
        m_1 = initial_date.month
        m_2 = final_date.month
        d_1 = min(initial_date.day, 30)
        d_2 = min(final_date.day, 30)
        if m_1 == 2 and d_1 == calendar.monthrange(y_1, m_1)[1] and d_1 != d_2:
            d_1 = 30
        if m_2 == 2 and d_2 == calendar.monthrange(y_2, m_2)[1] and d_1 != d_2:
            d_2 = 30
        days_30_co = 360 * (y_2 - y_1) + 30 * (m_2 - m_1) + (d_2 - d_1)
        return days_30_co


    def days_30_co(self, initial_date, final_date):
        """
        Calcular el numero de dias entre dos fechas bajo una convencion colombiana, de precia, de 30 dias por mes.
        Fecha: 2020-07-14
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Numero de dias entre las dos fechas bajo la convencion colombiana de 30 dias por mes.
        """
        y_1 = initial_date.year
        y_2 = final_date.year
        m_1 = initial_date.month
        m_2 = final_date.month
        d_1 = initial_date.day
        d_2 = final_date.day
        d_1_base = d_1
        # obtener cuantos dias hay en el mes de ese year
        dp_2 = calendar.monthrange(y_2, m_2)[1]
        # si los dias del mes de la fecha final son menores a los dias de la fecha inicial
        # [su mes es mas corto que fecha inicial]
        # y la fecha final es el fin de mes -> d_2 = d_1_base #Poner el mismo dia de la fecha inicial
        if dp_2 < d_1 and d_2 == dp_2:
            d_2 = d_1_base
        # ver cuantos dias del year agregar.
        dv = (y_2 - y_1 - 1) * 360
        # si es el mismo year, dv es cero. Lo mismo sucede si hay solo un year de diferencia
        if dv < 0:
            dv = 0
        if y_2 != y_1:
            if d_2 >= d_1:
                # si estan en years diferentes y el d_2 es mayor o igual al d_1 [dif mes larga]
                # Contar el numero de dias por diferencia de meses y dias entre fechas
                # Ajustando los 12 meses que no se calcularon en dv
                ds = ((12 - m_1 + m_2) * 30) + d_2 - d_1
            else:
                # Si years diferentes y d_1 es mas grande que d_2 [dif mes corta]
                # contar el numero de dias solo por diferencia de meses quitando un mes
                # (que se ajustara por dias) y ajustando los 12 meses que no se calcularon en dv
                ds = (12 - m_1 + m_2 - 1) * 30
                if d_1 == 31:
                    # Si d_1 tiene 31 dias, solo se le suman los dias de la fecha 2
                    ds += d_2
                else:
                    # Se suman 30 dias mas la diferencia de dias entre las fechas
                    ds = ds + 30 - d_1 + d_2
        else:
            if d_2 >= d_1:
                # si estan en el mismo year y el d_2 es mayor o igual al d_1 [dif mes larga]
                # Contar el numero de dias por diferencia de meses y dias entre fechas
                ds = ((m_2 - m_1) * 30) + d_2 - d_1
            else:
                # Si years diferentes y d_1 es mas grande que d_2 [dif mes corta]
                # contar el numero de dias solo por diferencia de meses quitando un mes
                # (que se ajustara por dias)
                ds = (m_2 - m_1 - 1) * 30
                if ds < 0:
                    ds = 0
                if d_1 == 31:
                    # Si d_1 tiene 31 dias, solo se le suman los dias de la fecha 2
                    ds = ds + d_2
                else:
                    # Se suman 30 dias mas la diferencia de dias entre las fechas
                    ds = ds + 30 - d_1 + d_2
        if (d_1 == 30 and d_2 == 31):
            ds = ds - 1

        days_30_co = dv + ds

        return days_30_co


    def day_count_1(self, initial_date, final_date):
        """
        Calcular el recuento de dias entre dos fechas bajo una convencion 1/1.
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Recuento de dias entre las dos fechas bajo una convencion 1/1.
        """
        day_count_1 = 1
        return day_count_1


    def day_count_30_360_u(self, initial_date, final_date):
        """
        Calcular el recuento de dias entre dos fechas bajo una convencion 30U/360..
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Recuento de dias entre las dos fechas bajo una convencion 30U/360.
        """
        day_count_30_360_u = self.days_30_u(initial_date, final_date) / 360
        return day_count_30_360_u


    def day_count_30_360_e(self, initial_date, final_date):
        """
        Calcular el recuento de dias entre dos fechas bajo una convencion 30E/360..
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Recuento de dias entre las dos fechas bajo una convencion 30E/360.
        """
        day_count_30_360_e = self.days_30_e(initial_date, final_date) / 360
        return day_count_30_360_e


    def day_count_30_360_co(self, initial_date, final_date):
        """
        Calcular el recuento de dias entre dos fechas bajo una convencion 30CO/360..
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Recuento de dias entre las dos fechas bajo una convencion 30CO/360.
        """
        day_count_30_360_co = self.days_30_co(initial_date, final_date) / 360
        return day_count_30_360_co


    def day_count_act_360(self, initial_date, final_date):
        """
        Calcular el recuento de dias entre dos fechas bajo una convencion Act/360
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Recuento de dias entre las dos fechas bajo una convencion Act/360.
        """
        day_count_act_360 = self.days_act(initial_date, final_date) / 360
        return day_count_act_360


    def day_count_act_365(self, initial_date, final_date):
        """
        Calcular el recuento de dias entre dos fechas bajo una convencion Act/365
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Recuento de dias entre las dos fechas bajo una convencion Act/365.
        """
        all_dates = self.date_sequence(initial_date + dt.timedelta(days=1), final_date)
        leap_years = [self.is_leap(x.year) for x in all_dates]
        day_count_act_365 = sum(leap_years) / 366 + (len(leap_years) - sum(leap_years)) / 365
        return day_count_act_365


    def day_count_act_365_f(self, initial_date, final_date):
        """
        Calcular el recuento de dias entre dos fechas bajo una convencion Act/365 (fija) (Act/365 (Fixed) convention)
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Recuento de dias entre las dos fechas bajo una convencion Act/365 (fija).
        """
        day_count_act_365 = self.days_act(initial_date, final_date) / 365
        return day_count_act_365


    def day_count_nl_365(self, initial_date, final_date):
        """
        Calcular el recuento de dias entre dos fechas bajo una convencion Act/365 sin bisiesto (Act/365 (Non Leap) convention)
        Fecha: 2020-06-26
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :return: Recuento de dias entre las dos fechas bajo una convencion Act/365 sin bisiestos.
        """
        day_count_act_365 = self.days_non_leap(initial_date, final_date) / 365
        return day_count_act_365


    def day_count_bus_252(self, initial_date, final_date, calendar):
        """
        Calcular el recuento de dias entre dos fechas bajo una convencion Buss/252 (dias habiles).
        Fecha: 2022-06-13
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: Recuento de dias entre las dos fechas bajo una convencion Buss/252 (dias habiles).
        """
        day_count_bus_252 = self.days_business(initial_date, final_date, calendar) / 252
        return day_count_bus_252


    def day_count(self, initial_date, final_date, day_count_convention, calendar=None):
        """
        Calcular el recuento de dias entre dos fechas dada su convencion
        Fecha: 2022-06-13
        :param initial_date: Fecha de inicio del calendario en formato datetime.date
        :param final_date: Fecha de finalizacion del calendario en formato datetime.date
        :param day_count_convention: Nombre de la convencion de la fecha de recuento de dias
        :param calendar: Calendario dias habiles, default = None
        :return: Recuento de dias entre las dos fechas correspondiente.
        """
        switcher = {
            '1': self.day_count_1,
            '30_360_u': self.day_count_30_360_u,
            '30_360_e': self.day_count_30_360_e,
            '30_360_co': self.day_count_30_360_co,
            'act_360': self.day_count_act_360,
            'act_365': self.day_count_act_365,
            'act_365_f': self.day_count_act_365_f,
            'nl_365': self.day_count_nl_365,
            'bus_252': self.day_count_bus_252
        }
        case = switcher.get(day_count_convention, 'error')
        if (case == 'error'):
            msg = ' day_count_convention invalido %s' % day_count_convention
            raise ValueError(msg)
        elif (case == 'day_count_bus_252'):
            return case(initial_date, final_date, calendar)
        else:
            return case(initial_date, final_date)


    def same_day_starting_day_convention(self, trade_date, calendar=None):
        """
        Calcular la fecha de inicio de un contrato bajo la convencion de iniciar el mismo dia (Same Day Starting convention)
        Fecha: 2020-06-26
        :param trade_date: Fecha de negociacion en formato datetime.date
        :param calendar: Calendario dias habiles, default = None
        :return: Fecha de inicio del contrato en formato datetime.date
        """
        starting_date = trade_date
        return starting_date


    def on_starting_day_convention(self, trade_date, calendar):
        """
        Calcular la fecha de inicio de un contrato bajo la convencion de iniciar ON (ON Starting convention)
        Fecha: 2022-06-13
        :param trade_date: Fecha de negociacion en formato datetime.date
        :param calendar: Calendario dias habiles
        :return: Fecha de inicio del contrato en formato datetime.date
        """
        starting_date = self.add_business_days(trade_date, 1, calendar)
        return starting_date


    def spot_starting_day_convention(self, trade_date, calendar, spot_days=2):
        """
        Calcular la fecha de inicio de un contrato bajo la convencion de iniciar Spot (Spot Starting convention)
        Fecha: 2022-06-13
        :param trade_date: Fecha de negociacion en formato datetime.date
        :param calendar: Calendario dias habiles
        :param spot_days: Numero dias spot, default = 2
        :return: Fecha de inicio del contrato en formato datetime.date
        """

        starting_date = self.add_business_days(self = self, date=trade_date, days=int(spot_days), calendar=calendar)
        return starting_date


    def forward_starting_day_convention(self):
        """
        Calcular la fecha de inicio de un contrato bajo la convencion de iniciar adelante (Forward Starting convention)
        Fecha: 2020-06-26
        :return: NotImplementedError
        """
        raise NotImplementedError


    def starting_date(self, trade_date, calendar, starting_day_convention):
        """
        Calcular la fecha de inicio de un contrato dada su convencion.
        Fecha: 2022-06-13
        :param trade_date: Fecha de negociacion en formato datetime.date
        :param calendar: Calendario dias habiles
        :param starting_day_convention: Nombre de la convencion del dia de inicio
        :return: Fecha de inicio del contrato dada su convencion en formato datetime.date
        """

        switcher = {
            'spot_starting': self.spot_starting_day_convention,
            'forward_starting': self.forward_starting_day_convention,
            'same_day_starting': self.same_day_starting_day_convention,
            'on_starting': self.on_starting_day_convention
        }
        case = switcher.get(starting_day_convention, 'error')
        if (case == 'error'):
            msg = ' starting_day_convention invalido %s' % starting_day_convention
            raise ValueError(msg)
        else:
            return case(self = self, trade_date=trade_date, calendar=calendar)


    def tenor_specs(self, tenor):
        """
        Calcular la fecha de inicio de un contraro dada su convencion.
        Fecha: 2020-06-26
        :param tenor: Plazo del mercado (market tenor)
        :return: Lista con el periodo y el numero de periodos de un plazo.
        """
        tenor_raw_period = re.sub('\d', '', tenor)
        tenor_raw_length = int(re.sub(tenor_raw_period, '', tenor)) if re.sub(tenor_raw_period, '',
                                                                              tenor).isdigit() else 1
        if (tenor_raw_period in self._tenor_conventions.keys()):
            tenor_info = self._tenor_conventions[tenor_raw_period]
            tenor_period = tenor_info[0]
            tenor_length = int(tenor_info[1]) * tenor_raw_length
        elif bool(re.search("x1",tenor,re.I)):
            tenor_period = "D"
            tenor_raw_length = int(re.sub("x1", '', tenor, re.I))
            tenor_length = 28 * tenor_raw_length
        else:
            msg = 'Plazo %s no esta permitido' % tenor
            raise ValueError(msg)
        tenor_specs = {'period': tenor_period, 'length': tenor_length}
        return tenor_specs


    def add_tenor(self, initial_date, tenor):
        """
        Calcular la fecha correspondiente de agregar un plazo dado a una fecha inicial
        Fecha: 2020-06-26
        :param initial_date: Fecha de referencia en formato datetime.date
        :param tenor: Plazo
        :return: Fecha correspondiente en formato datetime.date
        """
        tenor_specs = self.tenor_specs(self=self,tenor=tenor)
        if (tenor_specs['period'] == 'D'):
            tenor_date = initial_date + dt.timedelta(days=tenor_specs['length'])
        elif (tenor_specs['period'] == 'M'):
            tenor_date = initial_date + rd.relativedelta(months=tenor_specs['length'])
        elif (tenor_specs['period'] == 'Y'):
            tenor_date = initial_date + rd.relativedelta(years=tenor_specs['length'])
        else:
            msg = 'Plazo %s no es valido.' % tenor
            raise ValueError(msg)
        return (tenor_date)


    def tenor_date(self, trade_date, tenor, calendar, starting_day_convention, business_day_convention):
        """
        Calcular la fecha correspondiente de un plazo de mercado segun una convencion de calendario y dia para un Swap
        Fecha: 2022-06-13
        :param trade_date: Fecha de negociacion en formato datetime.date
        :param tenor: Plazo
        :param calendar: Calendario dias habiles
        :param starting_day_convention: Nombre de la convencion del dia de inicio
        :param business_day_convention: Nombre de la convencion de dias habiles
        :return: Fecha correspondiente de un plazo de mercado segun convencion con formato datetime.date
        """
        start_date = self.starting_date(self = self, trade_date=trade_date,calendar=calendar, starting_day_convention=starting_day_convention)
        tenor_raw_date = self.add_tenor(self = self, initial_date=start_date, tenor=tenor)
        tenor_date = self.business_date(self=self, date=tenor_raw_date, calendar=calendar, business_day_convention=business_day_convention)
        return tenor_date
 
        
    def tenor_sequence_dates(self, trade_date, tenor, frequency, business_day_calendar, starting_day_convention, business_day_convention):
        """
        Calcular la fecha correspondiente de un plazo de mercado segun una convencion de calendario y dia para un Swap
        Fecha: 2023-02-06
        :param trade_date: Fecha de negociacion en formato datetime.date
        :param tenor: Plazo
        :param calendar: Calendario dias habiles
        :param starting_day_convention: Nombre de la convencion del dia de inicio
        :param business_day_convention: Nombre de la convencion de dias habiles
        :return: Fecha correspondiente de un plazo de mercado segun convencion con formato datetime.date
        """
        effective_date = self.starting_date(trade_date, business_day_calendar,starting_day_convention)
        tenor_specs = self.tenor_specs(self=self,tenor=tenor)
        frequency_specs = self.tenor_specs(self=self,tenor=frequency)
        if (tenor_specs["period"]== frequency_specs["period"]):
            tenor_count =  int(tenor_specs["length"]/frequency_specs["length"])
        elif (tenor_specs["period"]== "Y" and frequency_specs["period"] == "M"):
            tenor_count =  int(tenor_specs["length"]*12/frequency_specs["length"])
        tenors = [str(x)+ frequency_specs["period"] for x in range(frequency_specs["length"],frequency_specs["length"]+(tenor_count*frequency_specs["length"]),frequency_specs["length"])]
        tenors_dates= [effective_date]+[self.tenor_date(trade_date, x, business_day_calendar, starting_day_convention, business_day_convention) for x in tenors]
        return np.array(tenors_dates)
 
    
    def month_third_wednesday(self, date_to_adjust, calendar_business_days =None, prior_day_adjust = None):
        """
        Calcula la fecha correspondiente al tercer miercoles del mes
        Fecha: 2023-02-06
        :param date_to_adjust: Fecha a ajustar en formato datetime.date
        :param calendar: Calendario dias habiles
        :param prior_day_adjust: Días anteriores de ajuste
        :return: Fecha correspondiente al tercer día del mes en formato datetime.date
        """
        date = dt.datetime(date_to_adjust.year,date_to_adjust.month,1).date()
        last_day = calendar.monthrange(date.year, date.month)[1]
        date_list = [date + dt.timedelta(days=x) for x in range(last_day)]
        third_wednesday = [date for date in date_list if date.weekday()==2][2]
        if prior_day_adjust is not None:
            third_wednesday = self.add_business_days(third_wednesday, prior_day_adjust, calendar_business_days)
        return third_wednesday

class CalendarHandler():
    """Clase para el uso de los calendarios de dias habiles en distintas jurisdicciones y combinacion de jurisdicciones
    """
    def __init__(self, logger):
        self.logger = logger
        
        
    def get_last_day_month(self, trade_date):
        """
        Entrega el ultimo día del mes
        """
        try:
            last_day=calendar.monthrange(trade_date.year, trade_date.month)[1]
            return last_day
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la extracción del ultimo dia del mes'))
            raise PlataformError("Hubo un error en la extracción del ultimo dia del mes")
        
        
    def get_last_buss_day_months(self, calendar_dt):
        """
        Extrae el ultimo día habil para cada año-mes del calendario suministrado
        """
        try:
            df_cal = pd.to_datetime(pd.DataFrame(calendar_dt)[0])
            max_montly_bussiness_days = df_cal.groupby([pd.DatetimeIndex(df_cal).year, 
                                                        pd.DatetimeIndex(df_cal).month]).max().reset_index(drop = True)
            max_montly_bussiness_cal = [x.to_pydatetime().date() for x in max_montly_bussiness_days.tolist()]
        
            return max_montly_bussiness_cal
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la extracción del ultimo dia habil para los meses del calendario'))
            raise PlataformError("Hubo un error en la extracción del ultimo dia habil para los meses del calendario")
    
    
    def get_first_buss_day_months(self, calendar_dt):
        """
        Extrae el primer día habil para cada año-mes del calendario suministrado
        """
        try:
            df_cal = pd.to_datetime(pd.DataFrame(calendar_dt)[0])
            min_montly_bussiness_days = df_cal.groupby([pd.DatetimeIndex(df_cal).year, 
                                                        pd.DatetimeIndex(df_cal).month]).min().reset_index(drop = True)
            min_montly_bussiness_cal = [x.to_pydatetime().date() for x in min_montly_bussiness_days.tolist()]
        
            return min_montly_bussiness_cal
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la extracción del primer dia habil para los meses del calendario'))
            raise PlataformError("Hubo un error en la extracción del primer dia habil para los meses del calendario")


class actions_db():
    CONN_CLOSE_ATTEMPT_MSG = "Intentando cerrar la conexion a BD"
    def __init__(self, db_url : str) -> None:
        """Gestiona la conexion y las transacciones a base de datos

        Args:
            db_url_src  (str): url necesario para crear la conexion a base de datos

        Raises:
            PlataformError: Cuando falla la creacion del objeto
        """
        try:
            self.db_url  = db_url 
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
                self.db_url , connect_args={"connect_timeout": 2}
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
        
    
    def get_beta_data (self):
        """
        Se realiza la consulta de los betas en la tabla de renta fija local

        Returns:
            beta_data_df: dataframe con la información de los betas generados por renta fija local 
        """        
        
        try:
            db_connection = self.create_connection()
            select_query = sa.sql.text(
            f"""SELECT cc_curve, rate_date, term, rate FROM pub_rfl_yield WHERE cc_curve IN ('BAAA2', 'CEC') AND rate_date =  '{valuation_date}' """)
            logger.debug(select_query)
            beta_data_df = pd.read_sql(select_query, db_connection)
            beta_data_df = beta_data_df.iloc[:-1]
            cec_df = beta_data_df[beta_data_df["cc_curve"]=='CEC']
            baaa2_df = beta_data_df[beta_data_df["cc_curve"]=='BAAA2']
            cec_df = cec_df["rate"]
            baaa2_df = baaa2_df["rate"]
            db_connection.close()
            return cec_df, baaa2_df
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  a la BD para obtener la infomacion de los betas"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc
        

    def get_hzd_co_data (self):
        """
        Se realiza la consulta de los hazzard de Colombia 
        Returns:
            beta_data_df: dataframe con la información del día habil y los posteriores dias no habiles hasta el siquiente dia hábil
        """        
        
        try:
            db_connection = self.create_connection()
            count_query = sa.sql.text(f"""SELECT COUNT(*) FROM pub_otc_hazzard_rates WHERE valuation_date =  '{valuation_date}' AND status_info = 1""")
            data_hzd_co = pd.read_sql(count_query, db_connection)
            data_hzd_co = data_hzd_co.iloc[0,0]
            select_query = sa.sql.text(f"""SELECT days, rate FROM pub_otc_hazzard_rates WHERE valuation_date =  '{valuation_date}' AND id_precia = 'CO'  AND status_info = 1""")
            hazzard_co = pd.read_sql(select_query, db_connection)
            logger.debug(select_query)
            db_connection.close()
            return data_hzd_co, hazzard_co
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  a la BD para obtener la informacion de los betas"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc


    def get_calendar_col (self):
        """
        Se realiza la consulta de los dias hábiles segun la fecha actual

        Returns:
            beta_data_df: dataframe con la información del día habil segun el calendario de Colombia 
        """        
        
        try:
            db_connection = self.create_connection()
            select_query = sa.sql.text("SELECT dates_calendar, bvc_calendar FROM precia_utils_calendars WHERE bvc_calendar = 1")
            logger.debug(select_query)
            calendar_col_df = pd.read_sql(select_query, db_connection)
            calendar_col_df = calendar_col_df.iloc[:-1]
            db_connection.close()
            calendar_col_df = calendar_col_df["dates_calendar"]
            return calendar_col_df
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  a la BD para obtener la infomacion de los dias habiles"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc


    def get_hzdl_params (self):
        """
        Se realiza la consulta los parametros para el calculo del spread de hzd local
        Returns:
            hzdl_params_df: dataframe con la información de los parametros 
        """        
        
        try:
            db_connection = self.create_connection()
            select_query = sa.sql.text("SELECT counterparty, ref_curve, spread from precia_utils_hzd_params")
            logger.debug(select_query)
            hzdl_params_df = pd.read_sql(select_query, db_connection)
            db_connection.close()
            return hzdl_params_df
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la consulta  a la BD"
            logger.error(create_log_msg(raise_msg))
            raise PlataformError() from fpc_exc


    def insert_data(self, table: str, info_df: pd.DataFrame):
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
                
                info_df.to_sql(table, con=db_connection, if_exists="append", index=False)
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


    def disable_previous_info(self, valuation_date, curve):
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
                logger.info("Se intenta deshabilitar la informacion...")
                status_info = 0
                update_query = ("UPDATE pub_otc_probabilities SET status_info= {} WHERE valuation_date = '{}'".format(status_info, valuation_date))
                logger.debug(update_query)
                conn.execute(update_query)
                logger.debug(curve)
                curve = tuple(curve)
                update_query = ("UPDATE pub_otc_hazzard_rates SET status_info= {} WHERE valuation_date = '{}' and id_precia in {}".format(status_info, valuation_date, curve))
                logger.debug(update_query)
                conn.execute(update_query)                

                logger.info("Se deshabilita informacion con exito")
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


class hzdl_methodological:

    def hazard_cum(self, tao, hazard_rates_curve):
        
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
            error_msg = 'No se lograron calcular los Hazzard rates acumulados'
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg)


    def sp_probability(self, haz_rate_info, days):
        
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
            try:
                hazard_rates_curve = interpolate.interp1d(haz_rate_info["days"],  haz_rate_info["hazard"], kind = 'next', fill_value = "extrapolate")(days_haz)
            except(Exception,):
                hazard_rates_curve = interpolate.interp1d(haz_rate_info["days"],  haz_rate_info["rate"], kind = 'next', fill_value = "extrapolate")(days_haz)
            hazard_rates_cum = self.hazard_cum(days_haz/360, hazard_rates_curve)
            sp = np.exp(-hazard_rates_cum[days-1])
            dt = (days-np.append([0],days[:-1]))/360
            dp = 1 - sp
            dp_cond = hazard_rates_curve[days-1] * dt
            return(pd.DataFrame({"days":days, "sp":sp,"dp": dp, "dp_cond":dp_cond}))
        except(Exception,):
            logger.error(create_log_msg('Se genero un error en el calculo de las probabilidades de supervivencia y default'))
            raise PlataformError("Hubo un error en el calculo de las probabilidades de supervivencia y default")


    def hazard_objective_bond(self, days, cc_rf, cc_corp, rec = 0.4, base = 365):
        """
        Construye la curva de hazard rates para una contraparte local dadas las curvas CEC_Pesos y la curva corporativa de referencia

        Params
        ----------
        days (numpy.ndarray) : Días de vencimiento de los titulos corporativos.                                
        cc_rf (numpy.ndarray) : Curva CEC_Pesos.
        cc_rf (numpy.ndarray) : Curva corporativa de referencia.
        rec (float): Tasa de recuperación del cds
        base (float): Base del cds
        
        Returns
        ------------
        (pd.dataframe): Curva hazarrd
                            days:  Dias de la curva.
                            hazard: Hazard asociado a la contraparte.
        """
        try:
            times=days/base
            df_rf=1/(1+cc_rf[days-1])**(times)
            df_corp=1/(1+cc_corp[days-1])**(times)
            hazards=np.ones(len(days))
            hazards[0]=-np.log((df_corp[0]-df_rf[0]*rec)/(df_rf[0]*(1-rec)))/times[0]
            for i in range(1,len(days)):
                hazards[i]=-(np.log((df_corp[i]-df_rf[i]*rec)/(df_rf[i]*(1-rec)))+self.hazard_cum(times[:i],hazards[:i])[-1])/(times[i]-times[i-1])
            if len(np.where(hazards<0)[0])>0:
                hazards[np.where(hazards<0)[0]]=hazards[min(np.where(hazards<0)[0])-1]
            return pd.DataFrame({"days":days,"rate":hazards})
        except(Exception,):
            logger.error(create_log_msg(''))
            raise PlataformError("")    


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


def update_report_process(status, description, technical_description, curves):
    """
    Estructura el reporte de estado del proceso y luego llama la funcion que invoca la lambda
    
    Parameters:
        status (str): Estado del proceso
        description (str): Descripcion del proceso
        technical_description (str): Descripcion técnica del proceso (llegado el caso haya fallado el proceso)
        
    Returns:
        None
    """
    try:
        if isinstance(valuation_date, date):
            valuation_dates = valuation_date.strftime(date_format)
        else:
            valuation_dates = valuation_date
        lambda_name = get_parameter_from_ssm(parameter_store_name)
        report_process = {
            "input_id": "Hazzard_local",
            "output_id":curves,
            "process": "Hazzard",
            "product": "Derivados OTC",
            "stage": "Metodologia",
            "status": "",
            "aws_resource": "glue-p-hzdl-etl-process",
            "type": "archivo",
            "description": "",
            "technical_description": "",
            "valuation_date": valuation_dates
            }
        report_process["status"]=status
        report_process["description"]=description
        report_process["technical_description"]=technical_description
        launch_lambda(lambda_name, report_process)
        logger.info("Se envia el reporte de estado del proceso") 
    except (Exception,) as fpc_exc:
        raise_msg = "No se logro enviar el estado a la lambda de reportes"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError() from fpc_exc

try:
    logger.info("Inicia la lectura de parametros del glue...")
    params_key = [
        "DB_SECRET",
        "VALUATION_DATE",
        "LBD_FILE"    
    ]
    params_dict = get_params(params_key)
    secret_db = params_dict["DB_SECRET"]
    secret_db = get_secret(secret_db)
    db_url_publish = secret_db["conn_string_aurora_publish"]
    schema_publish_otc = secret_db["schema_aurora_publish"]
    db_url_publish = db_url_publish+schema_publish_otc
    db_url_pub_rfl = secret_db["conn_string_back_8_publish"]
    schema_pub_rfl = secret_db["schema_back_8_publish"]
    db_url_pub_rfl = db_url_pub_rfl+schema_pub_rfl
    db_url_sources = secret_db["conn_string_aurora_sources"]
    schema_precia_utils_otc = secret_db["schema_utils"]
    db_url_precia_utils = db_url_sources+schema_precia_utils_otc

    parameter_store_name = 'ps-otc-lambda-reports'
    valuation_date = params_dict["VALUATION_DATE"]
    lambda_hazzard_file = params_dict["LBD_FILE"]
except (Exception,) as fpc_exc:
    raise_msg = "Fallo la lectura de los parametros de Glue"
    logger.error(create_log_msg(raise_msg))
    raise PlataformError() from fpc_exc


def run():
    try:

        logger.info('Se inicia con el proceso para Hazzard local')
        logger.info('Se inicia a consultar la informacion necesaria para el porceso...')
        actionsdb = actions_db(db_url_pub_rfl)
        cec, baaa2 = actionsdb.get_beta_data()
        actionsdb = actions_db(db_url_precia_utils)
        calendar_col_df = actionsdb.get_calendar_col()
        hzdl_params_df = actionsdb.get_hzdl_params()
        actionsdb = actions_db(db_url_publish)
        data_hzd_co, hazards_co = actionsdb.get_hzd_co_data()
        logger.info('Se finaliza la recoleccion de la informacion necesaria.')
        data_db_hazzard = []
        data_db_probabilities = []
        logger.info(data_hzd_co)
        if data_hzd_co != 0:
            curve = list(hzdl_params_df["counterparty"].values)
            actionsdb.disable_previous_info(valuation_date, curve)
            hzdl_meth = hzdl_methodological()
            spread_list = []
            for item in curve:
                logger.info('Se esta realizando la creación para la curva: '+str(item))
                spread = hzdl_params_df.loc[hzdl_params_df["counterparty"]== item, "spread"].values[0]
                spread_list.append(spread)
                spread = float(spread)
                baaa2 = pd.Series(baaa2)
                baaa2 = baaa2.values
                corp_curve=baaa2+spread
                date_ini=dt.datetime.strptime(valuation_date,"%Y-%m-%d").date()
                calendar_col_df = calendar_col_df.astype(str)
                business_day_calendar=[dt.datetime.strptime(x,date_format).date() for x in  calendar_col_df]
                last_date_pd = DateUtils.tenor_date(DateUtils,date_ini,"7Y", business_day_calendar, "spot_starting", "modified_following")
                cds_last_day = (last_date_pd-date_ini).days
                days_prob = np.arange(60,cds_last_day,60)
                ### Aplicacion metodologia
                cec = pd.Series(cec)
                cec = cec.values
                hazards=hzdl_meth.hazard_objective_bond(days_prob, cec/100, corp_curve/100)# la funcion retorna un dataframe de hazards
                hazards["id_precia"] = item
                hazards["valuation_date"] = valuation_date
                sd_prob_cc=hzdl_meth.sp_probability(hazards, days_prob)
                sd_prob_co=hzdl_meth.sp_probability(hazards_co, days_prob)
                dp_corp=sd_prob_co["dp"]+sd_prob_co["sp"]*sd_prob_cc["dp"]
                sp_corp=sd_prob_cc["sp"]*sd_prob_co["sp"]
                dp_corp_out=pd.DataFrame({"days":days_prob,"pd_value":dp_corp, "id_precia":item, "valuation_date":valuation_date}) # dataframe probabilidades default
                sp_corp_out=pd.DataFrame({"days":days_prob,"ps_value":sp_corp, "id_precia":item, "valuation_date":valuation_date}) # dataframe probabilidades supervivencia
                data_to_db = pd.merge(dp_corp_out, sp_corp_out, on="days", how='inner', validate="one_to_one")
                new_columns_name = {'id_precia_x':'id_precia', 'valuation_date_x':'valuation_date'}
                data_to_db.rename(columns=new_columns_name, inplace=True)
                data_to_db = data_to_db[['id_precia', 'days', 'pd_value', 'ps_value', 'valuation_date']]

                data_hazzard = pd.DataFrame(hazards)
                data_db_hazzard.append(data_hazzard)
                data_probabilities = pd.DataFrame(data_to_db)
                data_db_probabilities.append(data_probabilities)
            data_hazzard = pd.concat(data_db_hazzard, ignore_index=True)
            data_probabilities = pd.concat(data_db_probabilities, ignore_index=True)
            actionsdb.insert_data("pub_otc_hazzard_rates", data_hazzard)
            logger.info('Se inserta la informacion de rates en base de datos para Hazzard rates')
            actionsdb.insert_data("pub_otc_probabilities", data_probabilities)
            logger.info('Se inserta la informacion de probabilidades en base de datos')
            logger.info('Se finaliza el proceso para Hazzard local.')
            payload = {}
            launch_lambda(lambda_hazzard_file, payload=payload)
            update_report_process("Exitoso", "Proceso Finalizado", "", curve)
        else: 
            logger.info('No hay informacion para los Hazzard de Colombia')

    
    except (Exception,) as init_exc:
        error_msg = "No se logro ejecutar el Glue para Hazzard local"
        logger.error(create_log_msg(error_msg))
        update_report_process("Fallido", error_msg, str(init_exc), "")
        raise PlataformError() from init_exc


if __name__ == "__main__":
    run()