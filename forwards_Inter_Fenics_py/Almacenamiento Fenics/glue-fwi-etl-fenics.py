"""
===============================================================================

Nombre: glue-fwi-etl-fenics.py

Tipo: Glue Job

Autor:
    - Hector Augusto Daza Roa
    Tecnologia - Precia

Ultima modificacion: 28/11/2023

Este es el script de la ETL de insumos de forward inter para los archivos de
Fenics: 
- Descarga el archivo insumo en S3 (fue previamiente transferido desde el
FTP de Fenics por el glue de coleccion glue-*-fwi-collector-fenics).
- Realiza un proceso de ETL sobre la información de estos archivos (calculo de
mid, cálculo de nodos overnight, cálculo de días, redondeo, etc.)
- Valida la estructura de insumo (para esto se apoya en la lambda
lbd-*-csv-validator)
- Carga la informacion resultante de la ETL en BD publish
- Si ocurre una excepcion envia correo de error

Parametros del Glue Job (Job parameters o Input arguments):
    "--DB_SECRET"= <<Nombre del secreto de BD>>
    "--MAIL_SECRET"= <<Nombre del secreto del servidor de correos>>
    "--LBD_CSV_VALIDATOR"= "lbd-%env-csv-validator"
    "--GLUE_OPI"= "glue-job-%env-process-otc-opt-inter"
    "--SM_FWI_ARN"= <<ARN de la step function sm-%env-otc-fwd-inter-points)>>
    "--S3_SCHEMA_BUCKET"= "s3-%env-csv-validator-schematics"
    "--S3_FILE_PATH"= <<Ruta del archivo de insumo en el bucket de S3 (Viene de
    la lambda lbd-%env-trigger-etl-fenics)>>
    "--S3_FWI_BUCKET"= <<Nombre del bucket de S3 donde esta el archivo de
    insumo (Viene de la lambda lbd-%env-trigger-etl-fenics)>>
    "PRECIA_API": <<URL de la API interna de Optimus K>>
    %env: Ambiente: dev, qa o p

===============================================================================
"""
# NATIVAS DE PYTHON
import logging
from time import sleep
from os.path import join as path_join, basename as path_basename
from json import loads as json_loads, dumps as json_dumps
from re import match
from datetime import datetime as dt
from email.message import EmailMessage
from io import StringIO
from smtplib import SMTP
from sys import argv, exc_info, stdout
from base64 import b64decode


# AWS
from awsglue.utils import getResolvedOptions 
from boto3 import client as aws_client

# DE TERCEROS
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

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


class WrongCurvesError(Exception):
    def __init__(
        self,
        error_message="Algunas curvas no superaron las validaciones",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)


# EXCEPCIONES PERSONALIZADAS: FIN-----------------------------------------------


class ReportEmail:
    """
    Clase que representa los correos que reportan las variaciones del día

    Attributes:
    ----------
    subject: str
        Asunto del correo
    body: str
        Cuerpo del correo
    data_file: dict
        Información para el archivo adjunto del correo

    Methods:
    --------
    connect_to_smtp(secret)
        Conecta al servicio SMTP de precia

    create_mail_base
        Crea el objeto EmailMessage con los datos básicos para enviar el correo

    attach_file_to_message(message, df_file)
        Adjunta el archivo al mensaje para enviar por el correo

    send_email(smtp_connection, message)
        Envia el objeto EmailMessage construido a los destinatarios establecidos

    run()
        Orquesta los métodos de la clase

    """

    def __init__(self, subject, body, data_file=None) -> None:
        self.subject = subject
        self.body = body
        self.data_file = data_file

    def connect_to_smtp(self, secret):
        """Conecta al servicio SMTP de precia con las credenciales que vienen en secret
        Parameters:
        -----------
        secret: dict, required
            Contiene las credenciales de conexión al servicio SMTP

        Returns:
        --------
        Object SMTP
            Contiene la conexión al servicio SMTP
        """
        error_msg = "No fue posible conectarse al relay de correos de Precia"
        try:
            logger.info("Conectandose al SMTP ...")
            connection = SMTP(host=secret["server"], port=secret["port"])
            connection.starttls()
            connection.login(secret["user"], secret["password"])
            logger.info("Conexión exitosa.")
            return connection
        except (Exception,) as url_exc:
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from url_exc

    def create_mail_base(self, mail_from, mails_to):
        """Crea el objeto EmailMessage que contiene todos los datos del email a enviar
        Parameters:
        -------
        mail_from: str, required
            Dirección de correo que envía el mensaje
        mails_to: str, required
            Direcciones de correo destinatario

        Returns:
        Object EmailMessage
            Contiene el mensaje base (objeto) del correo

        Raises:
        ------
        PlataformError
            Si no se pudo crear el objeto EmailMessage

        """
        error_msg = "No se crear el correo para el SMTP"
        try:
            logger.info('Creando objeto "EmailMessage()" con los datos básicos...')
            message = EmailMessage()
            message["Subject"] = self.subject
            message["From"] = mail_from
            message["To"] = mails_to
            message.set_content(self.body)
            logger.info("Mensaje creado correctamente")
            return message
        except (Exception,) as mail_exp:
            logger.error(create_log_msg(error_msg))
            raise PlataformError(error_msg) from mail_exp

    def send_email(self, smtp_connection, message):
        """Envia el objeto EmailMessage construido a los destinatarios establecidos
        Parameters:
        -----------
        smtp_connection: Object SMTP, required
            Contiene la conexión con el servicio SMTP
        message: Object EmailMessage
            Mensaje para enviar por correo

        Raises:
        -------
        PlataformError
            Si no pudo enviar el correo
            Si no pudo cerrar la conexión SMTP
        """
        logger.info("Enviando Mensaje...")
        try:
            smtp_connection.send_message(message)
        except (Exception,) as conn_exc:
            logger.error(create_log_msg("No se pudo enviar el mensaje"))
            raise PlataformError("No se pudo enviar el mensaje") from conn_exc
        finally:
            try:
                smtp_connection.quit()
                logger.info("Conexión cerrada: SMTP")
            except (Exception,) as quit_exc:
                logger.error(create_log_msg("No se pudo cerra la conexión"))
                raise PlataformError(
                    "Hubo un error cerrando la conexión SMTP"
                ) from quit_exc


class FwiFenicsETL:
    TRANSLATE_DICT_TABLE = "precia_utils_suppliers_api_dictionary"
    FWI_TABLE = "pub_otc_forwards_inter_points_nodes"
    SCALE_TABLE = "precia_utils_fwi_vendors_factors"
    DB_TIMEOUT = 2
    ON_EXCLUDE_LIST = ["USDPHP"]

    def __init__(self, s3_path: str, input_df: str, db_secret: dict) -> None:
        try:
            self.input_df = input_df
            self.s3_path = s3_path
            self.db_secret = db_secret
            self.filename = "'No identificado'"
            self.run_etl()
        except (Exception,) as init_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion del objeto FwiFenicsETL"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from init_exc

    class ExtractManager:
        def __init__(self, s3_path: str, db_secret: dict) -> None:
            try:
                self.utils_engine = create_engine(
                    db_secret["conn_string_sources"] + db_secret["schema_utils"],
                    connect_args={"connect_timeout": FwiFenicsETL.DB_TIMEOUT},
                )
                self.s3_path = s3_path
                self.filename = path_basename(s3_path)
                self.metainfo = self.get_metainfo()
                translate_df = self.get_translate_df()
                large_curves_df = translate_df.loc[
                    translate_df["name_input"].str.contains("2"),
                    ["name_input", "precia_alias"],
                ]
                self.large_curves = list(large_curves_df["name_input"].unique())
                self.simple_curves = list(
                    map(
                        lambda x: x.replace("2", ""),
                        self.large_curves,
                    )
                )
                self.nodes_to_repeat = self.get_nodes_to_repeat(
                    large_curves_df=large_curves_df,
                    simple_curves=self.simple_curves,
                    large_curves=self.large_curves,
                    translate_df=translate_df,
                )
                self.ids_dict, self.cols_dict = self.get_translate_dicts(
                    translate_df=translate_df[
                        ~translate_df["name_input"].isin(self.simple_curves)
                    ]
                )
                self.scales = self.get_scales()
            except (Exception,) as init_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto ExtractManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from init_exc

        def get_metainfo(
            self,
        ) -> dict:
            try:
                logger.info("Obteniendo informacion del nombre del insumo...")
                WORD_REGEX = r"[a-zA-Z]+"
                DATE_REGEX = r"\d{8}"
                HOUR_REGEX = r"\d{4}"
                FILE_REGEX = r"^({product_regex})_({market_regex})_({date_regex})_({hour_regex})_({type_regex})\.({extension_regex})$"
                # FMD_FX_20231009_1200_INTRA.csv
                # ^([a-zA-Z]+)_([a-zA-Z]+)_(\d{8})_(\d{4})_([a-zA-Z]+)\.([a-zA-Z]+)$
                name_match = match(
                    FILE_REGEX.format(
                        type_regex=WORD_REGEX,
                        product_regex=WORD_REGEX,
                        market_regex=WORD_REGEX,
                        hour_regex=HOUR_REGEX,
                        date_regex=DATE_REGEX,
                        extension_regex=WORD_REGEX,
                    ),
                    self.filename,
                )
                if name_match:
                    metainfo = {
                        "product": name_match.group(1),
                        "market": name_match.group(2),
                        "valuation_date": name_match.group(3),
                        "hora": name_match.group(4),
                        "tipo": name_match.group(5),
                        "extension": name_match.group(6),
                    }
                    logger.info("Metainfo: %s", metainfo)
                else:
                    raise_msg = "La estructura del nombre del archivo no es la esperada"
                    raise_msg += ". Valide que se este cargando el archivo correcto"
                    raise UserError(raise_msg)
                if metainfo["extension"] != "csv":
                    raise_msg = "El tipo de archivo recibido no puede ser procesado"
                    raise_msg += " por la ETL. Solo se admiten archivos CSV"
                    raise UserError(raise_msg)
                logger.info("Informacion del nombre del insumo obtenida con exito")
                return metainfo
            except (Exception,) as file_exc:
                global first_error_msg
                raise_msg = "Fallo la obtencion de la metainfo del insumo"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from file_exc

        def get_translate_dicts(self, translate_df: pd.DataFrame) -> tuple:
            try:
                logger.info(
                    "Extrayendo diccionarios de equivalencias de terminos entre Precia y Fenics en BD..."
                )
                ids_df = translate_df[
                    translate_df["alias_type"].str.fullmatch("id")
                ].drop(["alias_type"], axis=1)
                ids_dict = ids_df.set_index("supplier_alias")["precia_alias"].to_dict()
                fields_df = translate_df[
                    translate_df["alias_type"].str.fullmatch("field")
                ].drop(["alias_type"], axis=1)
                fields_dict = fields_df.set_index("supplier_alias")[
                    "precia_alias"
                ].to_dict()
                logger.debug("fields_dict:\n%s", fields_dict)
                logger.info(
                    "Consulta de diccionarios de equivalencias de terminos entre Precia y Fenics en BD exitosa"
                )
                return ids_dict, fields_dict
            except (Exception,) as gad_exc:
                global first_error_msg
                raise_msg = "Fallo la obtencion de los diccionarios de traduccion"
                raise_msg += " en la tabla de diccionario de APIs en BD utils"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gad_exc

        def get_translate_df(self) -> pd.DataFrame:
            try:
                logger.info(
                    "Consultando equivalencias de terminos entre Precia y Fenics en BD..."
                )
                select_query = text(
                    f"""
                    SELECT supplier_alias,precia_alias,alias_type,name_input
                    FROM {FwiFenicsETL.TRANSLATE_DICT_TABLE} WHERE id_byproduct
                    = 'fwd_inter' AND id_supplier = 'Fenics'
                    """
                )
                with self.utils_engine.connect() as conn:
                    translate_df = pd.read_sql(select_query, con=conn)
                logger.info(
                    "Consulta de equivalencias de terminos entre Precia y Fenics en BD exitosa"
                )
                return translate_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la consulta de las equivalencias de terminos"
                raise_msg += "  entre Precia y Fenics en BD utils"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_scales(self) -> pd.DataFrame:
            try:
                logger.info(
                    "Consultando factores de escalamiento sobre los precios en BD..."
                )
                select_query = text(
                    f"""
                    SELECT instrument,scale FROM {FwiFenicsETL.SCALE_TABLE}
                    WHERE supplier = 'Fenics'
                    """
                )
                scales_df = pd.read_sql(select_query, con=self.utils_engine)
                scales_df.set_index("instrument", inplace=True)
                logger.info(
                    "Consulta de factores de escalamiento sobre los precio en BD exitosa"
                )
                return scales_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la consulta de los factores de escalamiento"
                raise_msg += "  sobre los precios en BD utils"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_nodes_to_repeat(
            self,
            large_curves_df: pd.DataFrame,
            simple_curves: list,
            large_curves: list,
            translate_df: pd.DataFrame,
        ) -> pd.DataFrame:
            try:
                simple_curves_df = translate_df.loc[
                    translate_df["name_input"].isin(simple_curves),
                    ["precia_alias"],
                ]
                replace_dict = dict(zip(large_curves, simple_curves))
                large_curves_df["simple_precia_alias"] = large_curves_df[
                    "precia_alias"
                ].replace(replace_dict, regex=True)
                nodes_to_repeat_df = large_curves_df.loc[
                    large_curves_df["simple_precia_alias"].isin(
                        simple_curves_df["precia_alias"]
                    ),
                    ["precia_alias"],
                ]
                return nodes_to_repeat_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la obtencion de los nodos a repetir"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

    class TransformManager:
        def __init__(
            self,
            cols_dict: dict,
            ids_dict: dict,
            input_df: pd.DataFrame,
            valuation_date: str,
            nodes_to_repeat: pd.DataFrame,
            simple_curves: list,
            large_curves: list,
            scales: pd.DataFrame,
        ) -> None:
            try:
                self.cols_dict = cols_dict
                self.ids_dict = ids_dict
                self.input_df = input_df
                self.nodes_to_repeat = nodes_to_repeat
                self.simple_curves = simple_curves
                self.large_curves = large_curves
                self.scales = scales
                self.expected_cols = list(self.cols_dict.values())
                self.expected_precia_ids = self.set_expected_ids(
                    ids_dict=ids_dict,
                    large_curves=large_curves,
                    simple_curves=simple_curves,
                    nodes_to_repeat=nodes_to_repeat,
                )
                self.valuation_date = dt.strptime(valuation_date, "%Y%m%d").date()
                self.new_cols_dict = {
                    "valuation_date": self.valuation_date,
                    "tenor_fwd": lambda x: x["precia_id"].str.split("_").str[-1],
                    "instrument_fwd": lambda x: x["precia_id"].str.split("_").str[-2],
                }
                (
                    self.correct_curves,
                    self.wrong_curves,
                ) = self.run_trans_manager()
            except (Exception,) as init_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto TransformManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from init_exc

        def set_expected_ids(
            self, ids_dict, large_curves, simple_curves, nodes_to_repeat
        ):
            try:
                logger.info("Construyendo lista de nodos esperados...")
                expected_ids_list = (
                    list(ids_dict.values())
                    + nodes_to_repeat["precia_alias"]
                    .replace(dict(zip(large_curves, simple_curves)), regex=True)
                    .apply(lambda x: x)
                    .to_list()
                )
                logger.info("Lista de nodos esperados construida con exito...")
                return expected_ids_list
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "No se pudo establecer la lista de nodos esperados"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def translate(self, process_df: pd.DataFrame) -> pd.DataFrame:
            """
            Reemplaza los identificadores de nodos y los nombres de las
            columnas que vienen del vendor por los que maneja precia

            Args:
                process_df (pd.DataFrame): Informacion original del insumo
                antes de cualquier transformacion

            Raises:
                PlataformError: Cuando el reemplazo falla

            Returns:
                pd.DataFrame: Informacion con los terminos usados por Precia
            """
            try:
                logger.info("Traduciendo df...")
                process_df = process_df.rename(columns=self.cols_dict)
                process_df = process_df.replace(self.ids_dict)
                logger.debug("Dataframe traducido:\n%s", process_df)
                logger.info("Df traducido con exito")

                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "No se pudo realizar el reemplazo de nombres"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def filter(self, process_df: pd.DataFrame) -> pd.DataFrame:
            """Filtra las columnas y filas para dejar solo las de interes

            Args:
                process_df (pd.DataFrame): Informacion a filtrar

            Raises:
                PlataformError: Cuando falla el filtro

            Returns:
                pd.DataFrame: Informacion con solo los nodos y columnas
                esperados
            """
            try:
                logger.info("Filtrando df...")
                process_df = process_df.loc[  # Filtro de columnas
                    :,
                    list(set(process_df.columns).intersection(self.expected_cols)),
                ]
                if "precia_id" in process_df.columns:
                    process_df = process_df[  # Filtro de filas
                        process_df["precia_id"].isin(self.expected_precia_ids)
                    ]
                logger.debug("Dataframe filtrado:\n%s", process_df)

                logger.info("Df filtrado con exito")
                return process_df
            except (Exception,) as fil_exc:
                global first_error_msg
                raise_msg = "No se logro extraer la informacion de interes"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from fil_exc

        def add_simple_curves(
            self,
            nodes_to_repeat: pd.DataFrame,
            process_df: pd.DataFrame,
            simple_curves: list,
            large_curves: list,
        ) -> pd.DataFrame:
            try:
                logger.info("Agregando curvas simples...")
                replace_dict = dict(zip(large_curves, simple_curves))
                simple_curves_df = process_df[
                    process_df["precia_id"].isin(nodes_to_repeat["precia_alias"])
                ]
                simple_curves_df = simple_curves_df.copy()
                simple_curves_df["precia_id"] = simple_curves_df["precia_id"].replace(
                    replace_dict, regex=True
                )
                process_df = pd.concat(
                    [process_df, simple_curves_df], ignore_index=True
                )
                logger.info("Curvas simples agregadas con exito")
                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la adicion de las curvas simple"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def split_curves(self, process_df: pd.DataFrame) -> list:
            try:
                logger.info("Conviertiendo en lista de dfs por curva...")
                curve_group = process_df.groupby("instrument_fwd")
                curve_list = list(
                    map(
                        lambda x: curve_group.get_group(x),
                        process_df["instrument_fwd"].unique(),
                    )
                )
                logger.info("Conversion en lista de df por curva exitosa")
                return curve_list
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = (
                    "Fallo la conversion del dataframe en una lista de dataframes"
                )
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def split_expected_ids(self, expected_ids: list) -> list:
            try:
                logger.info("Conviertiendo lista en lista de dataframes...")
                expected_ids_df = pd.DataFrame(expected_ids, columns=["precia_id"])
                expected_ids_df = expected_ids_df.assign(
                    instrument_fwd=lambda x: x["precia_id"].str.split("_").str[-2]
                )
                curve_group = expected_ids_df.groupby("instrument_fwd")
                return curve_group
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = (
                    "Fallo la conversion del dataframe en una lista de dataframes"
                )
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def correct_days(self, process_df: pd.DataFrame) -> pd.DataFrame:
            try:
                logger.info("Verificando dias de nodo ON vs SP...")
                if process_df.at["ON", "days_fwd"] == process_df.at["SP", "days_fwd"]:
                    logger.info("Los dias del nodo SP coinciden con los del nodo ON")
                    if process_df.at["ON", "days_fwd"] == 1:
                        logger.info("Se modifican los dias de SP a 2")
                        process_df.at["SP", "days_fwd"] = 2
                    else:
                        logger.info("Se modifican los dias de ON a 1")
                        process_df.at["ON", "days_fwd"] = 1
                logger.info("Verificacion de dias de nodo ON vs SP exitosa")
                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo ajuste por dias repetidos en nodos ON y SP"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def verify_and_eliminate_sn(self, process_df: pd.DataFrame) -> pd.DataFrame:
            try:
                logger.info("Verificando dias de nodo SN vs SP...")
                process_df.set_index("tenor_fwd", inplace=True)
                if (
                    "SN" in process_df.index
                    and process_df.at["SN", "days_fwd"]
                    == process_df.at["SP", "days_fwd"]
                ):
                    logger.info(
                        "Los dias del nodo SN son iguales a los dias del nodo SP"
                    )
                    logger.info("Se elimina el nodo SN")
                    process_df.drop(["SN"], axis=0, inplace=True)
                process_df.reset_index(inplace=True)
                logger.info("Verificacion de dias de nodo SN vs SP exitosa")
                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo ajuste por dias repetidos en nodos SN y SP"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def process_curve(
            self, process_df: pd.DataFrame, expexted_ids_by_curve
        ) -> tuple:
            try:
                process_df = process_df.copy()
                process_df = process_df.reset_index(drop=True)
                curve_name = process_df.loc[0, "instrument_fwd"]
                logger.info("Procesando curva %s...", curve_name)
                expected_precia_ids = expexted_ids_by_curve.get_group(curve_name)[
                    "precia_id"
                ].to_list()
                validation_msg = f"CURVA {curve_name}:\n\n"
                df_is_valid, validation_msg = self.validate_precia_ids(
                    process_df=process_df,
                    expected_precia_ids=expected_precia_ids,
                    validation_msg=validation_msg,
                )
                if not df_is_valid:
                    logger.error(
                        "Se interrumpe el procesamiento de la curva debido a nodos faltantes"
                    )
                    logger.error(validation_msg)
                    return process_df, df_is_valid, validation_msg, curve_name
                process_df["ask_fwd"] = process_df["ask_fwd"].astype(float)
                process_df["bid_fwd"] = process_df["bid_fwd"].astype(float)
                on_exclude = curve_name in FwiFenicsETL.ON_EXCLUDE_LIST
                process_df = self.calculate_sp(
                    process_df=process_df, instrument=curve_name
                )
                if not on_exclude:
                    process_df.set_index("tenor_fwd", inplace=True)
                    process_df = self.calculate_on(
                        process_df=process_df, instrument=curve_name
                    )
                    process_df = self.correct_days(process_df)
                    process_df.reset_index(inplace=True)
                process_df = self.verify_and_eliminate_sn(process_df)
                scale = self.scales.at[curve_name, "scale"]
                logger.info("Escalando precios (X%s)...", scale)
                process_df[["ask_fwd", "bid_fwd"]] = (
                    process_df[["ask_fwd", "bid_fwd"]] * scale
                )
                logger.info("Escalamiento de precios exitoso")
                logger.info("Calculando precios mid...")
                process_df["mid_fwd"] = (
                    process_df["bid_fwd"] + process_df["ask_fwd"]
                ) / 2
                logger.info("Precios mid calculados con exito")
                logger.info("Redondeando precios a 8 cifras...")
                process_df[["ask_fwd", "bid_fwd", "mid_fwd"]] = process_df[
                    ["ask_fwd", "bid_fwd", "mid_fwd"]
                ].round(8)
                logger.info("Redondeo de precios a 8 cifras exitoso")
                df_is_valid, validation_msg = self.post_validate(
                    process_df=process_df, validation_msg=validation_msg
                )



                if df_is_valid:
                    validation_msg = f"CURVA {curve_name} PROCESADA CON EXITO"
                    logger.info("Curva %s procesada con exito", curve_name)
                else:
                    logger.info(
                        "Curva %s procesada con exito, pero se encontraron inconsistencias en la validacion final",
                        curve_name,
                    )
                    logger.error(validation_msg)
                return process_df, df_is_valid, validation_msg, curve_name
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo el procesamiento de la curva"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def calculate_sp(
            self, process_df: pd.DataFrame, instrument: str
        ) -> pd.DataFrame:
            try:
                logger.info("Calculando nodo SPOT...")
                effective_date = dt.strptime(  # Ultimo nodo
                    process_df["effective_date"].iloc[-1],
                    "%d-%b-%Y %H:%M:%S",
                ).date()
                sp_node = pd.DataFrame(
                    {
                        "bid_fwd": [0],
                        "ask_fwd": [0],
                        "tenor_fwd": ["SP"],
                        "days_fwd": [(effective_date - self.valuation_date).days],
                        "maturity_date": [0],
                        "valuation_date": [self.valuation_date],
                        "instrument_fwd": instrument,
                        "precia_id": "SPOT_" + instrument,
                    }
                )
                process_df.drop(columns=["effective_date"], inplace=True)
                process_df = pd.concat([sp_node, process_df])
                logger.info("Calculo de nodo SPOT exitoso")
                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo el calculo del nodo spot"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def calculate_on_price_with_tn(self, process_df, price_type, delta_term):
            try:
                on_price = process_df.at["ON", price_type]
                tn_price = process_df.at["TN", price_type]
                new_on_bid_price = (on_price + tn_price) / delta_term
                process_df.at["ON", price_type] = new_on_bid_price
                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo el calculo del precio del nodo ON usando el nodo TN"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def calculate_on_prices(self, process_df: pd.DataFrame):
            try:
                logger.info("Calculando precios de nodo ON...")
                sp_days = process_df.at["SP", "days_fwd"]
                sp_index = process_df.index.get_loc("SP")
                tenor_next_to_sp_index = process_df.index[sp_index + 1]
                tenor_next_to_sp_days = process_df.at[
                    tenor_next_to_sp_index, "days_fwd"
                ]
                on_ask_price = process_df.at[tenor_next_to_sp_index, "ask_fwd"] / (
                    tenor_next_to_sp_days - sp_days
                )
                on_bid_price = process_df.at[tenor_next_to_sp_index, "bid_fwd"] / (
                    tenor_next_to_sp_days - sp_days
                )
                logger.info("Calculo de precios de nodo ON exitoso")
                return on_ask_price, on_bid_price
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo el calculo de los precios del nodo ON"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def calculate_on(
            self, process_df: pd.DataFrame, instrument: str
        ) -> pd.DataFrame:
            try:
                logger.info("Calculando nodo ON...")
                tn_exist = "TN" in process_df.index
                on_exist = "ON" in process_df.index
                if tn_exist and on_exist:
                    logger.info("Instrumento tiene nodos TN y ON")
                    on_maturity_date = pd.to_datetime(
                        process_df.at["ON", "maturity_date"]
                    ).date()
                    delta_term = (on_maturity_date - self.valuation_date).days
                    process_df = self.calculate_on_price_with_tn(
                        process_df, "bid_fwd", delta_term
                    )
                    process_df = self.calculate_on_price_with_tn(
                        process_df, "ask_fwd", delta_term
                    )
                    process_df.drop(["TN"], axis=0, inplace=True)
                elif not (tn_exist or on_exist):
                    logger.info("Instrumento NO tiene nodos ni TN ni ON")
                    on_ask_price, on_bid_price = self.calculate_on_prices(process_df)
                    on_node = pd.DataFrame(
                        {
                            "bid_fwd": [on_bid_price],
                            "ask_fwd": [on_ask_price],
                            "tenor_fwd": ["ON"],
                            "days_fwd": [1],
                            "maturity_date": [0],
                            "valuation_date": [self.valuation_date],
                            "instrument_fwd": instrument,
                            "precia_id": "P_Fwd_" + instrument + "_ON",
                        }
                    )
                    on_node.set_index("tenor_fwd", inplace=True)
                    process_df = pd.concat([on_node, process_df])
                process_df.drop(columns=["maturity_date"], inplace=True)
                logger.info("Calculo de nodo ON exitoso")
                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo el calculo del nodo overnight"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def validate_precia_ids(
            self,
            process_df: pd.DataFrame,
            expected_precia_ids: list,
            validation_msg: str = "",
            is_valid_df: bool = True,
        ) -> tuple:
            """
            Realiza la validacion de los identificadores de precia recibidos vs
            los esperados

            Args:
                process_df (pd.DataFrame): Informacion a validar inconsistency
                (str, optional): Inconsistencia encontrada en una validacion
                previa. Defaults to "". is_valid_df (bool, optional): Resultado
                de una validacion previa. Defaults to True.

            Raises:
                PlataformError: Cuando falla la validacion

            Returns:
                tuple:
                    is_valid_df (bool): Si el dataframe es valido o no
                    inconsistency (str): Incosistencia encontrada en el
                    dataframe (acumulada a la/s recibida/s en los parametros)
            """
            try:
                if "precia_id" in process_df.columns:
                    present_precia_ids = set(process_df["precia_id"])
                    missing_precia_ids = set(expected_precia_ids) - present_precia_ids
                    if missing_precia_ids:
                        is_valid_df = False
                        validation_msg += (
                            f"Faltan los nodos: {', '.join(missing_precia_ids)}.\n\n"
                        )
                return is_valid_df, validation_msg
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "No se logro validar los nodos (ids precia)"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(error_msg) from gen_exc

        def calculate_days(self, process_df: pd.DataFrame) -> pd.DataFrame:
            try:
                logger.info("Calculando dias...")
                process_df["maturity_date"] = pd.to_datetime(
                    process_df["maturity_date"],
                    format="%d-%b-%Y %H:%M:%S",
                ).dt.date
                process_df["days_fwd"] = (
                    process_df["maturity_date"] - process_df["valuation_date"]
                ).dt.days
                process_df.sort_values("days_fwd", inplace=True)
                logger.info("Calculo de dias exitoso")
                return process_df
            except (Exception,) as ad_exc:
                global first_error_msg
                raise_msg = "No se logro calcular los dias"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from ad_exc

        def post_validate(
            self, process_df: pd.DataFrame, validation_msg: str = ""
        ) -> tuple:
            try:
                logger.info("Realizando validacion final del df...")
                df_is_valid = True
                there_are_negative_days = False
                process_df["days_fwd"] = process_df["days_fwd"].astype(int)
                there_are_negative_days = (process_df["days_fwd"] < 0).any()
                if there_are_negative_days:
                    df_is_valid = False
                    error_msg = "- Los siguientes nodos tienes dias negativos:\n\n"
                    negative_rows = process_df.loc[
                        process_df["days_fwd"] < 0, ["precia_id", "days_fwd"]
                    ]
                    error_msg += f"{negative_rows.to_string(index=False)}\n\n"
                    validation_msg += error_msg
                duplicates = process_df["days_fwd"].duplicated()
                if duplicates.any():
                    df_is_valid = False
                    duplicate_rows = process_df.groupby("days_fwd").filter(
                        lambda x: len(x) > 1
                    )[["precia_id", "days_fwd"]]
                    error_msg = "- Los siguientes nodos tienes dias repetidos:\n\n"
                    error_msg += f"{duplicate_rows.to_string(index=False)}\n\n"
                    validation_msg += error_msg
                logger.info("Validacion final del df exitosa")
                return df_is_valid, validation_msg
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la validacion final del dataframe"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def run_trans_manager(
            self,
        ) -> pd.DataFrame:
            try:
                process_df = self.translate(self.input_df)
                process_df = self.filter(process_df)
                process_df = self.add_simple_curves(
                    nodes_to_repeat=self.nodes_to_repeat,
                    process_df=process_df,
                    simple_curves=self.simple_curves,
                    large_curves=self.large_curves,
                )
                logger.info("Agregando columnas al df...")
                process_df = process_df.assign(**self.new_cols_dict)
                logger.info("Adicion de columnas al df exitosa")
                process_df = self.calculate_days(process_df)
                curve_df_list = self.split_curves(process_df)
                expexted_ids_by_curve = self.split_expected_ids(
                    self.expected_precia_ids
                )
                curve_df_list = list(
                    map(
                        lambda x: self.process_curve(
                            process_df=x, expexted_ids_by_curve=expexted_ids_by_curve
                        ),
                        curve_df_list,
                    )
                )
                correct_curves = [item for item in curve_df_list if item[1]]
                wrong_curves = [item for item in curve_df_list if not item[1]]

                return correct_curves, wrong_curves
            except (Exception,) as trans_exc:
                global first_error_msg
                raise_msg = "Fallo la transformacion de informacion del dataframe"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from trans_exc

    class LoadManager:
        def __init__(
            self,
            db_secret: dict,
            output_df_list: list[tuple[pd.DataFrame, bool, str]],
        ) -> None:
            try:
                self.output_df = self.join_dfs(tuple_list=output_df_list)
                self.publish_engine = create_engine(
                    db_secret["conn_string_publish"] + db_secret["schema_publish"],
                    connect_args={"connect_timeout": FwiFenicsETL.DB_TIMEOUT},
                )
                self.insert_df()
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto LoadManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def join_dfs(
            self, tuple_list: list[tuple[pd.DataFrame, bool, str]]
        ) -> pd.DataFrame:
            try:
                df_list = [num for num, _, _, _ in tuple_list]
                output_df = pd.concat(df_list, ignore_index=True)
                return output_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la conversion de lista de dataframes en dataframe"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def insert_df(self) -> None:
            try:
                logger.info("Insertando df en BD publish...")

                def insert_row(row: pd.Series) -> None:
                    _, row_serie = row
                    sql = text(
                        f"""
                        INSERT INTO {FwiFenicsETL.FWI_TABLE}
                        (mid_fwd,bid_fwd,ask_fwd,days_fwd,id_precia,valuation_date,
                        instrument_fwd,tenor_fwd) VALUES
                        ({row_serie['mid_fwd']},{row_serie['bid_fwd']},{row_serie['ask_fwd']},
                        {row_serie['days_fwd']}, '{row_serie['precia_id']}',
                        '{row_serie['valuation_date']}',
                        '{row_serie['instrument_fwd']}',
                        '{row_serie['tenor_fwd']}') ON DUPLICATE KEY UPDATE
                        mid_fwd = {row_serie['mid_fwd']},bid_fwd =
                        {row_serie['bid_fwd']},ask_fwd =
                        {row_serie['ask_fwd']}, instrument_fwd =
                        '{row_serie['instrument_fwd']}', tenor_fwd =
                        '{row_serie['tenor_fwd']}', days_fwd =
                        '{row_serie['days_fwd']}';"""
                    )
                    self.publish_engine.execute(sql)

                list(map(insert_row, self.output_df.iterrows()))
                logger.info("Insercion de df en BD publish exitosa")
            except (Exception,) as ins_exc:
                global first_error_msg
                raise_msg = "Fallo la insercion del dataframe en BD publish"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from ins_exc

    def run_etl(
        self,
    ) -> None:
        """Ejecuta el proceso completo de ETL:
            1. Extracion (ExtractManager)
            2. Transformacion (TransformManager)
            3. Carga (LoadManager)

        Raises:
            PlataformError: Cuando falla el proceso de ETL
        """
        try:
            extract_manager = FwiFenicsETL.ExtractManager(
                s3_path=self.s3_path,
                db_secret=self.db_secret,
            )
            self.filename = extract_manager.filename
            transform_manager = FwiFenicsETL.TransformManager(
                cols_dict=extract_manager.cols_dict,
                ids_dict=extract_manager.ids_dict,
                input_df=self.input_df,
                valuation_date=extract_manager.metainfo["valuation_date"],
                nodes_to_repeat=extract_manager.nodes_to_repeat,
                simple_curves=extract_manager.simple_curves,
                large_curves=extract_manager.large_curves,
                scales=extract_manager.scales,
            )
            self.valuation_date = transform_manager.valuation_date.strftime("%Y-%m-%d")
            self.correct_curves = transform_manager.correct_curves
            self.wrong_curves = transform_manager.wrong_curves
            if transform_manager.correct_curves:
                FwiFenicsETL.LoadManager(
                    db_secret=self.db_secret,
                    output_df_list=transform_manager.correct_curves,
                )
        except (Exception,) as etl_exc:
            global first_error_msg
            raise_msg = "Fallo el proceso de ETL para el insumo"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from etl_exc


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


class LambdaManager:
    @staticmethod
    def update_report_process(
        status,
        description,
        technical_description,
        lambda_name,
        curve_name_list: list[str],
        filename="",
        valuation_date="No identificado",
    ):
        try:
            if filename != "":
                filename_list = filename.split(".")[0].split("_")
                filename_list.pop(2)
                filename_list.pop(2)
                input_id = "_".join(filename_list)
            else:
                input_id = "No identificado"
            report_payload = {
                "input_id": input_id,
                "output_id": curve_name_list,
                "process": "Derivados OTC",
                "product": "fwd_inter",
                "stage": "Normalizacion",
                "status": status,
                "aws_resource": "glue-p-fwi-etl-fenics",
                "type": "insumo",
                "description": description,
                "technical_description": technical_description,
                "valuation_date": valuation_date,
            }
            logger.info("Evento a enviar a la lambda:\n%s", json_dumps(report_payload))
            lambda_client = aws_client("lambda")
            lambda_response = lambda_client.invoke(
                FunctionName=lambda_name,
                InvocationType="RequestResponse",
                Payload=json_dumps(report_payload),
            )
            lambda_response_decoded = json_loads(
                lambda_response["Payload"].read().decode()
            )
            logger.info(
                "Respuesta de la lambda:\n%s", json_dumps(lambda_response_decoded)
            )
            logger.info("Se envia el reporte de estado del proceso")
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
    def validate_structure(lambda_name, payload: dict) -> dict:
        try:
            logger.info("Validando la estructura del archivo...")
            logger.info("Evento a enviar a la lambda:\n%s", json_dumps(payload))
            logger.info("Lanzando ejecucion de lambda %s...", lambda_name)
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
            if (
                "statusCode" in lambda_response_decoded
                and lambda_response_decoded["statusCode"] == 500
            ):
                body = lambda_response_decoded["body"]
                raise PlataformError(
                    f"Fallo la ejecucion de la lambda de validacion de archivos csv: {body}"
                )
            elif "errorMessage" in lambda_response_decoded:
                error_message = lambda_response_decoded["errorMessage"]
                if "stackTrace" in lambda_response_decoded:
                    stack_trace = lambda_response_decoded["stackTrace"]
                    error_message += "\n".join(stack_trace)
                raise PlataformError(
                    f"Fallo la ejecucion de la lambda de validacion de archivos csv: {error_message}"
                )
            logger.info("Validacion de la estructura del archivo exitosa")
            return lambda_response_decoded
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


class MailManager:
    @staticmethod
    def send_email(body: str, smtp_secret: dict, subject: str) -> None:
        """Envia un correo

        Args:
            body (str): Cuerpo del correo a enviar
            smtp_secret (dict): Informacion necesaria para conectarse al
            servidor SMTP y enviar el correo
            subject (str): Asunto del correo

        Raises:
            PlataformError: Cuando falla el envio del correo
        """
        try:
            logger.debug("smtp_secret:\n%s", smtp_secret)
            smpt_credentials = {
                "server": smtp_secret["smtp_server"],
                "port": smtp_secret["smtp_port"],
                "user": smtp_secret["smtp_user"],
                "password": smtp_secret["smtp_password"],
            }
            mail_from = smtp_secret["mail_from"]
            mail_to = smtp_secret["mail_to"]
            # Eliminando las tabulaciones de la identacion de python
            body = "\n".join(line.strip() for line in body.splitlines())
            email = ReportEmail(subject, body)
            smtp_connection = email.connect_to_smtp(smpt_credentials)
            message = email.create_mail_base(mail_from, mail_to)
            email.send_email(smtp_connection, message)
        except (Exception,) as see_exc:
            raise_msg = "Fallo la construccion y envio del correo"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from see_exc

    @staticmethod
    def send_error_email(
        error_msg: str, smtp_secret: dict, path: str, env: str
    ) -> None:
        """Envia un correo de error

        Args:
            error_msg (str): Mensaje de error a enviar
            smtp_secret (dict): Informacion necesaria para conectarse al
            servidor SMTP y enviar el correo
            path (str): Ruta del archivo procesado
            env (str): Ambiente de ejecucion: dev, qa o prod

        Raises:
            PlataformError: Cuando falla el envio del correo
        """
        try:
            logger.info("Enviando correo de error...")
            subject = "Megatron: Fwd inter: Fenics: Fallo el proceso de ETL de insumo"
            body = f"""
                Cordial saludo. 

                El proceso de transformacion, extraccion y carga (ETL) del insumo fallo.
                
                Error: {error_msg}
                
                Insumo: {path}
                
                Glue Job: glue-{env}-fwi-etl-fenics

                Megatron.

                Enviado por el servicio automatico de notificaciones de Precia PPV S.A. en AWS"""
            MailManager.send_email(body=body, smtp_secret=smtp_secret, subject=subject)
            logger.info("Correo de error enviado con exito")
        except (Exception,) as see_exc:
            raise_msg = "Fallo la construccion y envio del correo de error"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from see_exc


class GlueManager:
    OPI_DEPENDENCIES = {
        "USDBRL": ["USDBRL"],
        "USDCLP": ["USDCLP"],
        "USDMXN": ["USDMXN"],
        "USDPEN": ["USDBRL", "USDPEN"],
        "EURUSD": ["EURUSD"],
        "GBPUSD": ["GBPUSD"],
        "USDCAD": ["USDCAD"],
        "USDCHF": ["USDCHF"],
        "USDJPY": ["USDJPY"],
    }
    PARAMS = [
        "DB_SECRET",
        "MAIL_SECRET",
        "LBD_CSV_VALIDATOR",
        "GLUE_OPI",
        "SM_FWI_ARN",
        "S3_SCHEMA_BUCKET",
        "S3_FILE_PATH",
        "S3_FWI_BUCKET",
        "PRECIA_API",
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



    @staticmethod
    def start_job_run(glue_name: str, opi_curve: str, valuation_date: str) -> None:
        try:
            arguments = {
                "--JOB_NAME": glue_name,
                "--VALUATION_DATE": valuation_date,
                "--CURRENCY": opi_curve,
            }
            glue_client = aws_client("glue")
            response = glue_client.start_job_run(JobName=glue_name, Arguments=arguments)
            job_run_id = response["JobRunId"]
            logger.info(
                "El Glue Job se lanzo bajo el id: %s. Para la curva: %s",
                job_run_id,
                opi_curve,
            )
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = (
                f"Fallo el lanzamiento de la ejecucion del job de glue: {glue_name}"
            )
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    @staticmethod
    def launch_opi_process(
        fwi_curves_set: set, valuation_date: str, glue_name: str
    ) -> None:
        try:
            logger.info(
                "Lanzando procesamiento de curvas opt inter que dependen de fwd inter..."
            )
            fwi_dependencies_set = {
                curve
                for sublist in GlueManager.OPI_DEPENDENCIES.values()
                for curve in sublist
            }
            fwi_to_run_opt = fwi_curves_set & fwi_dependencies_set
            opi_curves_to_run = [
                key
                for key, curves in GlueManager.OPI_DEPENDENCIES.items()
                if any(curve in fwi_to_run_opt for curve in curves)
            ]
            logger.info("Curvas de opciones a lanzar: %s", opi_curves_to_run)
            for curve in opi_curves_to_run:
                for i in range(5):
                    try:
                        logger.info("Intento numero %s (%s)", i + 1, curve)
                        GlueManager.start_job_run(
                            glue_name=glue_name,
                            valuation_date=valuation_date,
                            opi_curve=curve,
                        )
                        break
                    except (Exception,):
                        error_msg = (
                            "Ocurrio una excepcion en el lanzamiento del job de glue"
                        )
                        error_msg += (
                            ". Es muy problable que sea por superacion de la tasa "
                        )
                        error_msg += (
                            "maxima de lanzamientos o ejecuciones del job de glue"
                        )
                        logger.error(create_log_msg(error_msg))
                        sleep(2 + 2 * i)  # Para evitar error de concurrencia en glue
                        if i == 4:
                            raise PlataformError(
                                "Maximos reintentos de lanzamiento de glue opi alcanzados"
                            )
            logger.info(
                "Lanzamiento de procesamiento de curvas opt inter que dependen de fwd inter exitoso"
            )
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo el lanzamiento de las ejecuciones del glue de opciones internacionales"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc


class MachineManager:
    @staticmethod
    def start_execution(
        instrument: str,
        valuation_date: str,
        precia_api: str,
        machine_arn: str,
        mail_list: list[str],
    ):
        try:
            client_step_functions = aws_client("stepfunctions")
            now_time_str = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            valoration_date = now_time_str.replace(" ", "_").replace(":", ".")
            payload = {
                "valuation_date": valuation_date,
                "instrument": instrument,
                "precia_api": precia_api,
                "recipients": mail_list,
            }
            client_step_functions.start_execution(
                stateMachineArn=machine_arn,
                name=f"Fwd_{instrument}_{valoration_date}",
                input=json_dumps(payload),
            )
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo el lanzamiento de la maquina de estados"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    @staticmethod
    def launch_fwi_process(
        instrument_list: list[str],
        valuation_date: str,
        precia_api: str,
        machine_arn: str,
        mail_list: list[str],
    ) -> None:
        try:
            logger.info(
                "Lanzando maquina de estados de fwd inter para todos los instrumentos procesados..."
            )
            for curve_name in instrument_list:
                sleep(0.4)  # Para evitar error de concurrencia en glue de step function
                MachineManager.start_execution(
                    curve_name,
                    valuation_date=valuation_date,
                    precia_api=precia_api,
                    machine_arn=machine_arn,
                    mail_list=mail_list,
                )
            logger.info("Lanzamiento de la maquina de estados de fwd inter exitoso")
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo el lanzamiento de la maquina de estados"
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




class DbManager:
    TRANSLATE_DICT_TABLE = "precia_utils_suppliers_api_dictionary"
    DB_TIMEOUT = 2

    def __init__(self, db_secret) -> None:
        try:
            self.utils_engine = create_engine(
                db_secret["conn_string_sources"] + db_secret["schema_utils"],
                connect_args={"connect_timeout": FwiFenicsETL.DB_TIMEOUT},
            )
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion del engine para BD utils"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def get_fenics_curves(self) -> list[str]:
        try:
            logger.info("Consultando lista de curvas que nos provee Fenics...")
            select_query = text(
                f"""
                SELECT DISTINCT name_input FROM {DbManager.TRANSLATE_DICT_TABLE} WHERE id_supplier = 'Fenics' AND name_input <> 'all';
                """
            )
            with self.utils_engine.connect() as conn:
                results = conn.execute(select_query).fetchall()
            fenics_curves = [row[0] for row in results]
            logger.info("fenics_curves:\n%s", fenics_curves)
            return fenics_curves
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la consulta de las curvas esperadas de Fenics"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc


class S3Manager:
    @staticmethod
    def get_object(s3_bucket_name: str, s3_path: str) -> pd.DataFrame:
        """Descarga el archivo de insumo de S3 y lo convierte en un dataframe

        Args:
            extension (str): Extension del archivo de insumo

        Raises:
            PlataformError: Cuando el archivo supera el tamanio maximo aceptado
            PlataformError: Cuando el archivo de insumo esta vacio
            PlataformError: Cuando la estructura del archivo esta corrupta
            PlataformError: Cuando el archivo no se encuentra en la ruta especificada
            PlataformError: Cuando falla la descarga del archivo

        Returns:
            pd.DataFrame: Contiene la informacion del archivo de insumo
        """
        FILE_SIZE_MAX = 30e6
        global first_error_msg
        raise_msg = f"Fallo la obtencion del insumo desde el bucket {s3_bucket_name}"
        try:
            logger.info(
                "Descargando archivo de insumo del bucket %s...",
                s3_bucket_name,
            )
            s3 = aws_client("s3")
            s3_object = s3.get_object(Bucket=s3_bucket_name, Key=s3_path)
            file_size = s3_object["ContentLength"]
            if file_size > FILE_SIZE_MAX:
                raise_msg = f"El archivo {s3_path} ({file_size}) supera el tamanio maximo aceptado. "
                raise_msg += f"Maximo: {FILE_SIZE_MAX}B"
                raise PlataformError(raise_msg)
            input_file = s3_object["Body"].read().decode("utf-8")
            input_file = StringIO(input_file)
            input_df = pd.read_csv(input_file, skiprows=[0], skipfooter=1)
            logger.debug("Dataframe crudo sin procesamiento:\n%s", input_df)
            logger.info("Archivo de insumo descargado con exito")
            return input_df
        except pd.errors.EmptyDataError as ept_exc:
            raise_msg += "El archivo de insumo esta vacio"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from ept_exc
        except pd.errors.ParserError as ets_exc:
            raise_msg += "La estructura del archivo esta corrupta: "
            raise_msg += "Verifique comas, filas, columnas, etc"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from ets_exc
        except FileNotFoundError as file_exc:
            raise_msg += ". El archivo no se encuentra en la ruta especificada"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from file_exc
        except (Exception,) as gen_exc:
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc



    @staticmethod
    def put_object(file_df: pd.DataFrame, s3_bucket_name: str, s3_path: str):
        try:
            logger.info("Cargando archivo en s3...")
            csv_buffer = StringIO()
            file_df.to_csv(csv_buffer, index=False)
            s3_client = aws_client("s3")
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=path_join("Megatron", path_basename(s3_path)),
                Body=csv_buffer.getvalue(),
            )
            logger.info("Carga de archivo en s3 exitosa")
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la carga del archivo en el bucket"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc




class Main:
    @staticmethod
    def report_and_send_mail(
        exc,
        filename,
        valuation_date,
        curve_name_list,
        smtp_secret,
        path,
        env,
        report_lbd_name,
    ):
        
        try:
            LambdaManager.update_report_process(
                status="Fallido",
                description=first_error_msg,
                technical_description=str(exc),
                lambda_name=report_lbd_name,
                filename=filename,
                valuation_date=valuation_date,
                curve_name_list=curve_name_list,
            )
        except (Exception,) as gen_exc:
            rpt_error_msg = "Fallo el reporte de estado 'Fallido' en BD"
            mail_raise_msg = create_log_msg(rpt_error_msg)
            logger.critical(rpt_error_msg)
            raise PlataformError(rpt_error_msg) from gen_exc
        try:
            MailManager.send_error_email(
                error_msg=first_error_msg,
                smtp_secret=smtp_secret,
                path=path,
                env=env,
            )
        except (Exception,) as gen_exc:
            mail_error_msg = "Fallo el envio del correo de error"
            mail_raise_msg = create_log_msg(mail_error_msg)
            logger.critical(mail_raise_msg)
            raise PlataformError(mail_raise_msg) from gen_exc

    @staticmethod
    def validate_input(
        s3_path: str, s3_schema_bucket: str, s3_fwi_bucket: str, lbd_csv_validator: str
    ) -> None:
        try:
            payload = {
                "s3CsvPath": path_join("Megatron", path_basename(s3_path)),
                "s3SchemaPath": "forwards/international/fwi_fenics_schema.csvs",
                "s3SchemaDescriptionPath": "forwards/international/fwi_fenics_schema_description.txt",
                "s3SchemaBucket": s3_schema_bucket,
                "s3CsvBucket": s3_fwi_bucket,
            }
            validation_result = LambdaManager.validate_structure(
                lambda_name=lbd_csv_validator, payload=payload
            )
            struct_is_valid = validation_result["validCsv"]
            if not struct_is_valid:
                error_msg = "La estructura del archivo no es valida. "
                valid_struct = validation_result["validStruct"]
                error_msg += f"La estructura valida del archivo es:\n\n{valid_struct}"
                raise PlataformError(error_msg)
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la validacion del archivo insumo"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    @staticmethod
    def main() -> None:
        global first_error_msg
        try:
            logger.info("Ejecutando el main del job de glue...")
            params_dict = GlueManager.get_params()
            FORMAT_DATE = "%Y-%m-%d"
            wrong_curve_name_list = []
            filename = path_basename(params_dict["S3_FILE_PATH"])
            try:
                valuation_date = (
                    dt.strptime(filename.split("_")[2], "%Y%m%d")
                    .date()
                    .strftime(FORMAT_DATE)
                )
            except (Exception,):
                valuation_date = dt.strftime(dt.today(), FORMAT_DATE)

            report_lbd_name = ParameterManager.get_parameter_from_ssm() 
            db_manager = DbManager(SecretsManager.get_secret(params_dict["DB_SECRET"]))
            fenics_curves = db_manager.get_fenics_curves()

            input_df = S3Manager.get_object(
                s3_bucket_name=params_dict["S3_FWI_BUCKET"],
                s3_path=params_dict["S3_FILE_PATH"],
            )
            S3Manager.put_object(
                file_df=input_df,
                s3_bucket_name=params_dict["S3_FWI_BUCKET"],
                s3_path=params_dict["S3_FILE_PATH"],
            )

            
            Main.validate_input(
                s3_path=params_dict["S3_FILE_PATH"],
                s3_schema_bucket=params_dict["S3_SCHEMA_BUCKET"],
                s3_fwi_bucket=params_dict["S3_FWI_BUCKET"],
                lbd_csv_validator=params_dict["LBD_CSV_VALIDATOR"],
            )

            fwi_fenics_etl = FwiFenicsETL(
                input_df=input_df,
                s3_path=params_dict["S3_FILE_PATH"],
                db_secret=SecretsManager.get_secret(params_dict["DB_SECRET"]),
            )

            correct_curves_name_list = [
                curve_name for _, _, _, curve_name in fwi_fenics_etl.correct_curves
            ]

            
            if fwi_fenics_etl.correct_curves:
                mail_list = (
                    SecretsManager.get_secret(params_dict["MAIL_SECRET"])["mail_to"]
                    .replace(" ", "")
                    .split(",")
                )
                MachineManager.launch_fwi_process(
                    instrument_list=correct_curves_name_list,
                    valuation_date=fwi_fenics_etl.valuation_date,
                    precia_api=params_dict["PRECIA_API"],
                    machine_arn=params_dict["SM_FWI_ARN"],
                    mail_list=mail_list,
                )
                GlueManager.launch_opi_process(
                    fwi_curves_set={
                        curve_name
                        for _, _, _, curve_name in fwi_fenics_etl.correct_curves
                    },
                    valuation_date=fwi_fenics_etl.valuation_date,
                    glue_name=params_dict["GLUE_OPI"],
                )
            LambdaManager.update_report_process(
                status="Exitoso",
                description="Proceso Finalizado",
                technical_description="",
                lambda_name=report_lbd_name,
                filename=filename,
                valuation_date=valuation_date,
                curve_name_list=correct_curves_name_list,
            )

            if fwi_fenics_etl.wrong_curves:
                validation_msg = "Las siguientes curvas no pudieron procesarse porque "
                validation_msg += "no cumplieron con los criterios de validacion:\n\n"
                validation_msg += "".join(
                    [msg for _, _, msg, _ in fwi_fenics_etl.wrong_curves]
                )
                wrong_curve_name_list = [
                    curve_name for _, _, _, curve_name in fwi_fenics_etl.wrong_curves
                ]
                raise WrongCurvesError(validation_msg)

            logger.info("Ejecucion del main del job de glue exitosa!!!")
        except WrongCurvesError as wc_error:
            main_main_msg = "Fallo la ejecucion del job de glue"
            main_raise_msg = create_log_msg(main_main_msg)
            logger.critical(main_raise_msg)
            if not first_error_msg:
                first_error_msg = main_raise_msg
            Main.report_and_send_mail(
                exc=wc_error,
                report_lbd_name=report_lbd_name,
                filename=filename,
                valuation_date=valuation_date,
                curve_name_list=wrong_curve_name_list,
                smtp_secret=SecretsManager.get_secret(params_dict["MAIL_SECRET"]),
                path=params_dict["S3_FILE_PATH"],
                env=params_dict["LBD_CSV_VALIDATOR"].split("-")[1],
            )
            raise PlataformError(first_error_msg) from wc_error
        except (Exception,) as main_exc:
            main_main_msg = "Fallo la ejecucion del job de glue"
            main_raise_msg = create_log_msg(main_main_msg)
            logger.critical(main_raise_msg)
            if not first_error_msg:
                first_error_msg = main_raise_msg
            Main.report_and_send_mail(
                exc=main_exc,
                report_lbd_name=report_lbd_name,
                filename=filename,
                valuation_date=valuation_date,
                curve_name_list=fenics_curves,
                smtp_secret=SecretsManager.get_secret(params_dict["MAIL_SECRET"]),
                path=params_dict["S3_FILE_PATH"],
                env=params_dict["LBD_CSV_VALIDATOR"].split("-")[1],
            )
            raise PlataformError(first_error_msg) from main_exc


if __name__ == "__main__":
    Main().main() 

