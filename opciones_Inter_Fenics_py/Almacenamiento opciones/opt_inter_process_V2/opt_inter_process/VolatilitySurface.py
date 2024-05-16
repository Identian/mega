import pandas as pd
import numpy as np
import datetime as dt
import logging
from opt_inter_process.functions.Rounding import rd
from opt_inter_process.functions.Interpolation import Interpol as interpol
from precia_utils.precia_aws import get_params
from precia_utils.precia_logger import create_log_msg  # error message
from precia_utils.precia_exceptions import (
    PlataformError,
)  # Raise de excepcion y/o manejo de excepcion que se quiere ver en el correo

logger = logging.getLogger()


class VolatilitySurface:
    """Clase que contiene los metodos que se aplican en manera de negocio a las superficies de volatilidad de opciones internacionales
    Args:
        df_surface (pd.DataFrame): DataFrame que contiene la información de la superficie de volatilidad en estrategias proveniente de Refinitiv
        fwd_info (pd.DataFrame): DataFrame que contiene los valores de fecha-maduracion y tenor de Puntos Fwd del instrumento
        val_date (str): Fecha de valoracion
        instrument (str): Instrumento (par moneda)
    """

    def __init__(self, df_surface, fwd_info, val_date, instrument):
        params = get_params(
            [
                "TENORS",
                "STRATEGIES",
                "ID_BODY",
                "DECIMAL_ROUND",
                "INTERPOLATION_METHOD",
                "BASIC_POINTS",
            ]
        )
        self.int_surface = df_surface
        self.tenors = params["TENORS"].split(",")
        self.strategies = params["STRATEGIES"].split(",")
        self.id_body = params["ID_BODY"]
        self.dec = int(params["DECIMAL_ROUND"])
        self.method = params["INTERPOLATION_METHOD"]
        self.addition = float(params["BASIC_POINTS"])
        self.fwd_info = fwd_info
        self.val_date = val_date
        self.instrument = instrument

    def surface_completition(self):
        """Completacion de la superficie de volatilidad de la tasas de cambio internacional de manera de extrapolacion,
        y asi obtener la superficie para todos los nodos estandar publicados actualemente.
        Se convierten las variables pertinentes a los tipos esperados y por cada tenor-estrategia que no exista en la superficie estandar, se realiza una extrapolacion de la misma con los valores de mercado.
        """
        error_msg = (
            "Fallo la extension de la superficie de volatilidad para: "
            + self.instrument
        )
        try:
            logger.info("Complemento de la superficie %s", self.instrument)
            self.int_surface.tenor = pd.Categorical(
                self.int_surface.tenor, categories=self.tenors, ordered=True
            )
            self.int_surface.sort_values(["strategy", "tenor"], inplace=True)
            self.int_surface["mid"] = 0.5 * (
                self.int_surface["bid"] + self.int_surface["ask"]
            )
            logger.debug(self.int_surface)
            #self.int_surface[["mid", "bid", "ask"]] /= 100
            if any(self.int_surface.maturity_date.isna()):
                logger.info(
                    "Uso de fechas fwd para fechas de maduracion vacias en la superficie %s",
                    self.instrument,
                )
                logger.warning(
                    "Tenores para %s cuya fecha de maduracion fue buscada en los puntos fwd: \n %s",
                    self.instrument,
                    str(
                        list(
                            self.int_surface.loc[
                                self.int_surface.maturity_date.isna(),
                                ["tenor", "strategy"],
                            ].itertuples(index=False, name=None)
                        )
                    ),
                )
                self.int_surface.loc[
                    self.int_surface.maturity_date.isna(), "maturity_date"
                ] = (
                    (
                        (
                            self.int_surface.loc[
                                self.int_surface.maturity_date.isna(),
                                ["maturity_date", "tenor"],
                            ]
                            .reset_index()
                            .set_index("tenor")
                        ).join(
                            self.fwd_info[["maturity_date", "tenor"]].set_index(
                                "tenor"
                            ),
                            on="tenor",
                            how="left",
                            lsuffix="_old",
                            rsuffix="_new",
                        )
                    )
                    .reset_index()
                    .set_index("index")["maturity_date_new"]
                )
            self.int_surface["maturity_date"] = [
                d.date()
                for d in pd.to_datetime(
                    self.int_surface["maturity_date"]
                ).dt.to_pydatetime()
            ]
            self.int_surface["days"] = (
                self.int_surface["maturity_date"]
                - dt.datetime.strptime(self.val_date, "%Y-%m-%d").date()
            ).dt.days
            logger.debug(self.int_surface)
            opc_surface = self.int_surface.copy()
            opc_surface.set_index(["strategy", "tenor"], inplace=True)
            market_tenors = opc_surface.index.tolist()
            opc_inter_tenors = pd.DataFrame({"tenor": self.tenors})
            opc_inter_strategies = pd.DataFrame({"strategy": self.strategies})
            opc_all_tenors = pd.merge(opc_inter_strategies, opc_inter_tenors, "cross")
            opc_surface = opc_all_tenors.set_index(["strategy", "tenor"]).join(
                opc_surface, how="outer"
            )
            opc_all_tenors = list(zip(*map(opc_all_tenors.get, opc_all_tenors)))
            tenors_to_create = list(set(opc_all_tenors) - set(market_tenors))
            logger.info("Tenores a extender: %s", tenors_to_create)
            opc_surface.reset_index(inplace=True)
            opc_surface.sort_values(["strategy", "days"], inplace=True)
            if tenors_to_create != []:
                logger.info(
                    "Extension de la Superficies de Volatilidad: %s", self.instrument
                )
                for tenor in tenors_to_create:
                    logger.info("Calculando extension para: %s", tenor)
                    strategy = tenor[0]
                    tenor_new = tenor[1]
                    if (
                        int((len(opc_inter_tenors) - 1) * 0.5)
                        >= (
                            opc_inter_tenors.index[opc_inter_tenors.tenor == tenor_new]
                            - 1
                        )[0]
                    ):
                        logger.info("Extension del corto plazo")
                        market_tenors_selection = [
                            i for i in market_tenors if strategy in i
                        ]
                        nearby_tenors = [
                            market_tenors_selection[0][1],
                            market_tenors_selection[1][1],
                        ]
                        int_surface_strategy = self.int_surface.loc[
                            (self.int_surface.strategy == strategy)
                            & (self.int_surface.tenor.isin(nearby_tenors))
                        ]
                        if (
                            tenor_new == "ON"
                            and tenor_new not in self.fwd_info.tenor.values
                        ):
                            day = 1
                        else:
                            day = self.fwd_info.loc[
                                self.fwd_info.tenor == tenor_new, "days"
                            ].values[0]
                        if (
                            rd(int_surface_strategy.mid.values[0],self.dec)
                            == rd(int_surface_strategy.mid.values[1],self.dec)
                        ):
                            int_surface_strategy.mid.values[1] *= 1 + self.addition
                            opc_surface.loc[
                                (opc_surface.strategy == strategy)
                                & (
                                    opc_surface.tenor
                                    == int_surface_strategy.tenor.values[1]
                                ),
                                "mid",
                            ] *= (
                                1 + self.addition
                            )
                        vol_value = rd(
                            int_surface_strategy.loc[
                                int_surface_strategy.tenor == nearby_tenors[0], "mid"
                            ].values
                            - (
                                int_surface_strategy.loc[
                                    int_surface_strategy.tenor == nearby_tenors[0],
                                    "days",
                                ].values
                                - day
                            )
                            * (
                                (
                                    int_surface_strategy.loc[
                                        int_surface_strategy.tenor == nearby_tenors[1],
                                        "mid",
                                    ].values
                                    - int_surface_strategy.loc[
                                        int_surface_strategy.tenor == nearby_tenors[0],
                                        "mid",
                                    ].values
                                )
                                / (
                                    int_surface_strategy.loc[
                                        int_surface_strategy.tenor == nearby_tenors[1],
                                        "days",
                                    ].values
                                    - int_surface_strategy.loc[
                                        int_surface_strategy.tenor == nearby_tenors[0],
                                        "days",
                                    ].values
                                )
                            ),
                            self.dec,
                        )[0]
                    else:
                        logger.info("Extension del largo plazo")
                        market_tenors_selection = [
                            i for i in market_tenors if strategy in i
                        ]
                        nearby_tenors = [
                            market_tenors_selection[-1][1],
                            market_tenors_selection[-2][1],
                        ]
                        int_surface_strategy = self.int_surface.loc[
                            (self.int_surface.strategy == strategy)
                            & (self.int_surface.tenor.isin(nearby_tenors))
                        ]
                        day = self.fwd_info.loc[
                            self.fwd_info.tenor == tenor_new, "days"
                        ].values[0]
                        if (
                            rd(int_surface_strategy.mid.values[0],self.dec)
                            == rd(int_surface_strategy.mid.values[1],self.dec)
                        ):
                            int_surface_strategy.mid.values[1] *= 1 + self.addition
                            opc_surface.loc[
                                (opc_surface.strategy == strategy)
                                & (
                                    opc_surface.tenor
                                    == int_surface_strategy.tenor.values[1]
                                ),
                                "mid",
                            ] *= (
                                1 + self.addition
                            )
                        vol_value = rd(
                            int_surface_strategy.loc[
                                int_surface_strategy.tenor == nearby_tenors[0], "mid"
                            ].values
                            + 0.1
                            * (
                                day
                                - int_surface_strategy.loc[
                                    int_surface_strategy.tenor == nearby_tenors[0],
                                    "days",
                                ].values
                            )
                            * (
                                (
                                    int_surface_strategy.loc[
                                        int_surface_strategy.tenor == nearby_tenors[0],
                                        "mid",
                                    ].values
                                    - int_surface_strategy.loc[
                                        int_surface_strategy.tenor == nearby_tenors[1],
                                        "mid",
                                    ].values
                                )
                                / (
                                    int_surface_strategy.loc[
                                        int_surface_strategy.tenor == nearby_tenors[0],
                                        "days",
                                    ].values
                                    - int_surface_strategy.loc[
                                        int_surface_strategy.tenor == nearby_tenors[1],
                                        "days",
                                    ].values
                                )
                            ),
                            self.dec,
                        )[0]
                    opc_surface.loc[
                        (opc_surface.tenor == tenor_new)
                        & (opc_surface.strategy == strategy),
                        ["mid", "days"],
                    ] = np.array([vol_value, day])
                
            logger.info("Redondeo a %s decimales", str(self.dec))
            opc_surface["mid"] = rd(opc_surface["mid"], self.dec)
            logger.debug(opc_surface)
            opc_surface.loc[
                opc_surface["valuation_date"].isna(), "valuation_date"
            ] = self.val_date
            opc_surface.loc[
                opc_surface["currency"].isna(), "currency"
            ] = self.instrument
            logger.info("Adicion ID-Precia")
            opc_surface.loc[opc_surface["id_precia"].isna(), "id_precia"] = (
                self.id_body.split("[strategy]")[0]
                + opc_surface.loc[opc_surface["id_precia"].isna(), "strategy"]
                + self.id_body.split("[instrument]")[0][-1]
                + self.instrument
                + self.id_body.split("[tenor]")[0][-1]
                + opc_surface.loc[opc_surface["id_precia"].isna(), "tenor"]
            )
            opc_surface.drop(columns=["ask", "bid", "maturity_date"], inplace=True)
            opc_surface_copy = opc_surface.copy()
            opc_surface_copy = opc_surface_copy.pivot(
                index="days", columns=["strategy"], values="mid"
            )
            opc_surface_copy.reset_index(inplace=True)
            opc_surface_copy["instrument"] = self.instrument
            opc_surface_copy["valuation_date"] = self.val_date
            opc_surface_copy["tenor"] = self.tenors
            opc_surface.sort_values("days", inplace=True)
            opc_surface_copy.sort_values("days", inplace=True)
            opc_surface_copy["days"] = opc_surface_copy["days"].astype("int")
        except PlataformError:
            logger.error(create_log_msg(error_msg))
            raise
        except (Exception,) as sec_exc:
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from sec_exc
        self.int_surface = opc_surface
        self.int_surface_db = opc_surface_copy
        logger.info("Finalizacion complemento de la superficie %s", self.instrument)

    def strategies_to_deltas(self):
        """Conversion de una superficie de volatilidad en terminso de estrategias a terminos de deltas.
        Toma las distintas columnas de estrategias para crear un DataFrame unificado con los valores de las volatilidades en terminos de deltas.
        Este calculo se puede convalidad con el area de I+D
        """
        error_msg = (
            "Fallo la transformacion de la superfcie de estrategias a deltas para: "
            + self.instrument
        )
        try:
            logger.info(
                "Inicio transformacion superficie %s en terminos de estrategias a deltas",
                self.instrument,
            )
            call_90d = (
                self.int_surface.loc[self.int_surface.strategy == "ATM", ["mid"]].values
                + self.int_surface.loc[
                    self.int_surface.strategy == "10BF", ["mid"]
                ].values
                - (
                    0.5
                    * self.int_surface.loc[
                        self.int_surface.strategy == "10RR", ["mid"]
                    ].values
                )
            ).flatten()

            call_75d = (
                self.int_surface.loc[self.int_surface.strategy == "ATM", ["mid"]].values
                + self.int_surface.loc[
                    self.int_surface.strategy == "25BF", ["mid"]
                ].values
                - (
                    0.5
                    * self.int_surface.loc[
                        self.int_surface.strategy == "25RR", ["mid"]
                    ].values
                )
            ).flatten()

            call_50d = (
                self.int_surface.loc[self.int_surface.strategy == "ATM", ["mid"]].values
            ).flatten()

            call_25d = (
                self.int_surface.loc[self.int_surface.strategy == "ATM", ["mid"]].values
                + self.int_surface.loc[
                    self.int_surface.strategy == "25BF", ["mid"]
                ].values
                + (
                    0.5
                    * self.int_surface.loc[
                        self.int_surface.strategy == "25RR", ["mid"]
                    ].values
                )
            ).flatten()

            call_10d = (
                self.int_surface.loc[self.int_surface.strategy == "ATM", ["mid"]].values
                + self.int_surface.loc[
                    self.int_surface.strategy == "10BF", ["mid"]
                ].values
                + (
                    0.5
                    * self.int_surface.loc[
                        self.int_surface.strategy == "10RR", ["mid"]
                    ].values
                )
            ).flatten()
            opc_d = pd.DataFrame(
                [
                    self.int_surface["days"].unique(),
                    call_90d,
                    call_75d,
                    call_50d,
                    call_25d,
                    call_10d,
                ]
            ).T
            opc_d.columns = ["days", "X0.9", "X0.75", "X0.5", "X0.25", "X0.1"]
            opc_d.days = opc_d.days.astype(int)
            opc_d.loc[:, ~opc_d.columns.isin(["days"])] = rd(
                opc_d.loc[:, ~opc_d.columns.isin(["days"])], self.dec
            )
            opc_d["instrument"] = self.instrument
            opc_d["valuation_date"] = self.val_date
        except PlataformError:
            logger.error(create_log_msg(error_msg))
            raise
        except (Exception,) as sec_exc:
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from sec_exc
        self.int_surface_deltas = opc_d
        logger.info(
            "Fin transformacion superficie %s en terminos de estrategias a deltas",
            self.instrument,
        )

    def interpol_nodes_curves(self):
        """Interpolacion de una curva diaria desde una curva de nodos
        se interpolan los valores de dias y los precios/niveles mid de cada columna.
        Se crea un dataframe con la informacion interpolada, la secuencia de dias, la fecha de valoracion y el instrumento de la curva.
        """
        error_msg = (
            "Se genero un error en la inteporlacion de la curva de nodos, para el dia: "
            + self.val_date
            + ". E instrumento/curva: "
            + self.instrument
        )
        try:
            logger.info("Inicio interpolacion superficies %s", self.instrument)
            curves = [self.int_surface_db, self.int_surface_deltas]
            nodes_curve = [
                self.int_surface_db.columns[
                    ~self.int_surface_db.columns.isin(
                        ["instrument", "valuation_date", "days", "tenor"]
                    )
                ],
                self.int_surface_deltas.columns[
                    ~self.int_surface_deltas.columns.isin(
                        ["tenor", "days", "instrument", "valuation_date"]
                    )
                ],
            ]
            daily_curves = []
            for curv in range(len(curves)):
                interpolated_dict = {}
                values_to_interpolate = nodes_curve[curv]
                days = [
                    x
                    for x in range(
                        min(self.int_surface_deltas.days.values),
                        max(self.int_surface_deltas.days.values + 1),
                    )
                ]
                for i in values_to_interpolate:
                    try:
                        interpolated_values = rd(
                            interpol(
                                curves[curv].days.values, curves[curv][i].values, days
                            ).method(self.method),
                            self.dec,
                        )
                        interpolated_dict.update({i: interpolated_values})
                    except:
                        pass
                interpolated_dict.update({"valuation_date": self.val_date})
                interpolated_dict.update({"instrument": self.instrument})
                interpolated_dict.update({"days": days})
                daily_curves.append(pd.DataFrame(interpolated_dict))
                logger.info("Informacion interpolada para: " + self.instrument)

        except PlataformError:
            logger.error(create_log_msg(error_msg))
            raise
        except (Exception,) as sec_exc:
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from sec_exc
        self.daily_int_surface = daily_curves[0]
        self.daily_int_surface_deltas = daily_curves[1]
        logger.info("Fin interpolacion superficies %s", self.instrument)

    def all_surface_df(self):
        logger.info(
            "Inicio calculos adicionales - Superficies de volatilidad: %s",
            self.instrument,
        )
        self.surface_completition()
        self.strategies_to_deltas()
        self.interpol_nodes_curves()
        logger.info(
            "Fin calculos adicionales - Superficies de volatilidad: %s", self.instrument
        )
