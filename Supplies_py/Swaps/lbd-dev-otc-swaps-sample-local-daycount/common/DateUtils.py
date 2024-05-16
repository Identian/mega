from dateutil import rrule
import re
import calendar
import requests
import sys
import datetime as dt
import pandas as pd
import numpy as np
from dateutil import relativedelta as rd

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

        # date_betw = date_ini # 2020-12-11
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
                offset_date = self.following_business_date(offset_date, calendar)
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
            return case(date, calendar)

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
        leap_years = [self.is_leap_year(x.year) for x in all_dates]
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
        starting_date = self.add_business_days(trade_date, spot_days, calendar)
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
            return case(trade_date, calendar)

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
        tenor_specs = self.tenor_specs(tenor)
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
        start_date = self.starting_date(trade_date, calendar, starting_day_convention)
        tenor_raw_date = self.add_tenor(start_date, tenor)
        tenor_date = self.business_date(tenor_raw_date, calendar, business_day_convention)
        return tenor_date