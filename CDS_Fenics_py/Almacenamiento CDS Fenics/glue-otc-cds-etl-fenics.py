"""
===============================================================================

Nombre: glue-otc-cds-etl-fenics.py

Tipo: Glue Job

Autor:
    - Hector Augusto Daza Roa
    Tecnologia - Precia

Ultima modificacion: 30/11/2023

Este es el script de la ETL de insumos de cds para los archivos de
Fenics: 
- Descarga el archivo insumo en S3 (fue previamiente transferido desde el
FTP de Fenics por el glue de coleccion glue-*-cds-collector-fenics).
- Valida la estructura del archivo de insumo (para esto se apoya en la lambda
lbd-*-csv-validator)
- Realiza un proceso de ETL sobre la información de estos archivos (Division de precios y restructuración del informacion)
- Carga la informacion resultante de la ETL en BD process
- Si ocurre una excepcion envia un correo reportando el error

Parametros del Glue Job (Job parameters o Input arguments):
    "--DB_SECRET"= <<Nombre del secreto de BD>>
    "--LBD_CSV_VALIDATOR"= "lbd-%env-csv-validator"
    "--S3_SCHEMA_BUCKET"= "s3-%env-csv-validator-schematics"
    "--S3_FILE_PATH"= <<Ruta del archivo de insumo en el bucket de S3 (Viene de
    la lambda lbd-%env-trigger-etl-fenics)>>
    "--S3_CDS_BUCKET"= <<Nombre del bucket de S3 donde esta el archivo de
    insumo (Viene de la lambda lbd-%env-trigger-etl-fenics)>>
    "--LBD_PRC_ETL"= "lbd-%env-otc-cds-trigger-etl-process"
    %env: Ambiente: dev, qa o p

===============================================================================
"""
# NATIVAS DE PYTHON
import logging
from os.path import join as path_join, basename as path_basename
from json import loads as json_loads, dumps as json_dumps
from re import match
from datetime import datetime as dt, date
from io import StringIO
from sys import argv, exc_info, stdout
from base64 import b64decode

# AWS
from awsglue.utils import getResolvedOptions 
from boto3 import client as aws_client

# DE TERCEROS
import pandas as pd
from sqlalchemy import create_engine, engine
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


class MissingCdsError(Exception):
    def __init__(
        self,
        error_message="No hay informacion ni en el insumo ni en BD para algunos CDS",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)


# EXCEPCIONES PERSONALIZADAS: FIN-----------------------------------------------


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
            input_df = pd.read_csv(
                input_file, skiprows=[0], skipfooter=1, engine="python"
            )
            logger.info("Dataframe crudo sin procesamiento:\n%s", input_df)
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
            logger.info("Cargando archivo en s3 sin encabezado y última fila...")
            csv_buffer = StringIO()
            file_df.to_csv(csv_buffer, index=False)
            s3_client = aws_client("s3")
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=path_join("Megatron", path_basename(s3_path)),
                Body=csv_buffer.getvalue(),
            )
            logger.info("Carga de archivo en s3 (/Megatron) exitosa")
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la carga del archivo en el bucket"
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
    def validate_structure(lambda_name, payload: dict) -> dict:
        try:
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

    @staticmethod
    def update_report_process(
        status,
        lambda_name,
        valuation_date,
        curve_name_list: list[str],
        technical_description="",
        description="",
        filename="",
    ):
        try:
            if filename != "":
                input_id = filename.split(".")[0][:-6]
            else:
                input_id = "No identificado"
            report_payload = {
                "input_id": input_id,
                "output_id": curve_name_list,
                "process": "Derivados OTC",
                "product": "cds",
                "stage": "Normalizacion",
                "status": status,
                "aws_resource": "glue-p-otc-cds-etl-fenics",
                "type": "insumo",
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


class CdsFenicsETL:
    TRANSLATE_DICT_TABLE = "precia_utils_cds_suppliers_dictionary"
    MATURITY_PARAMS_TABLE = "precia_utils_cds_maturity_parameters"
    NAMES_TABLE = "precia_utils_cds_institution_name"
    CDS_TABLE = "prc_otc_cds"
    DB_TIMEOUT = 2
    NODES = [
        "1Y",
        "2Y",
        "3Y",
        "4Y",
        "5Y",
        "7Y",
        "10Y",
    ]

    def __init__(self, s3_path: str, input_df: str, db_secret: dict) -> None:
        try:
            self.input_df = input_df
            self.s3_path = s3_path
            self.db_secret = db_secret
            self.filename = "'No identificado'"
            self.counterparts_loaded_in_bd = {}
            self.missing_intitutions = []
            self.missing_counterparts = {}
            self.run_etl()
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion del objeto CdsFenicsETL"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    class ExtractManager:
        def __init__(self, db_secret: dict, s3_path: str) -> None:
            try:
                self.utils_engine = create_engine(
                    db_secret["conn_string_sources"] + db_secret["schema_utils"],
                    connect_args={"connect_timeout": CdsFenicsETL.DB_TIMEOUT},
                )
                self.s3_path = s3_path
                self.filename = path_basename(s3_path)
                self.metainfo = self.get_metainfo()
                self.valuation_date = dt.strptime(
                    self.metainfo["valuation_date"], "%d%m%y"
                ).date()
                fields_df, self.curves_df = self.get_translate_dfs()
                self.fields_dict = fields_df.set_index("supplier_alias")[
                    "precia_alias"
                ].to_dict()
                logger.info("fields_dict:\n%s", self.fields_dict)
                self.names_dict = self.get_institutions_names(self.curves_df)
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto ExtractManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_metainfo(
            self,
        ) -> dict:
            try:
                logger.info("Obteniendo informacion del nombre del insumo...")
                INPUT_ID_RE = r"\w+"
                DATE_RE = r"\d{6}"
                EXTENSION_RE = r"[A-Za-z]+"
                FILE_REGEX = r"^({input_id_re})({date_re})\.({extension_re})$"
                # ^(\\w+)(\\d{6})\\.([A-Za-z]+)$
                # GFICGMCUR3D181023.csv
                name_match = match(
                    FILE_REGEX.format(
                        input_id_re=INPUT_ID_RE,
                        date_re=DATE_RE,
                        extension_re=EXTENSION_RE,
                    ),
                    self.filename,
                )
                if name_match:
                    metainfo = {
                        "input_id": name_match.group(1),
                        "valuation_date": name_match.group(2),
                        "extension": name_match.group(3),
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
                return metainfo
            except (Exception,) as file_exc:
                global first_error_msg
                raise_msg = "Fallo la obtencion de la metainfo del insumo"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from file_exc

        def get_translate_dfs(self) -> tuple[pd.DataFrame]:
            try:
                logger.info(
                    "Consultando equivalencias de terminos entre Precia y Fenics en BD..."
                )
                select_query = text(
                    f"""
                    SELECT supplier_alias,precia_alias, alias_type
                    FROM {CdsFenicsETL.TRANSLATE_DICT_TABLE} WHERE id_byproduct
                    = 'cds' AND id_supplier = 'Fenics' AND status_info = 1
                    """
                )
                translate_df = pd.read_sql(select_query, con=self.utils_engine)
                fields_df = translate_df[translate_df["alias_type"] == "field"].drop(
                    ["alias_type"], axis=1
                )
                curves_df = (
                    translate_df[translate_df["alias_type"] == "id"]
                    .drop(["alias_type"], axis=1)
                    .rename(
                        columns={
                            "precia_alias": "counterparty",
                            "supplier_alias": "institution",
                        }
                    )
                )
                curves_df["institution"] = curves_df["institution"].str.lower()
                logger.info("Curvas esperadas de Fenics:\n%s", curves_df)
                return fields_df, curves_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la obtencion del diccionario de equivalencias de campos entre Fenics a Precia"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_institutions_names(self, fenics_names_df: pd.DataFrame) -> dict:
            try:
                logger.info(
                    "Consultando las contrapartes vs nombres de instituciones oficiales en BD..."
                )
                select_query = text(
                    f"""
                    SELECT counterparty,institution FROM
                    {CdsFenicsETL.NAMES_TABLE}
                    """
                )
                names_df = pd.read_sql(select_query, con=self.utils_engine)
                all_names_df = names_df.merge(
                    fenics_names_df.rename(
                        columns={"institution": "fenics_institution"}
                    ),
                    on="counterparty",
                    how="inner",
                    validate=None,
                )
                names_dict = all_names_df.set_index("fenics_institution")[
                    "institution"
                ].to_dict()
                logger.info("names_dict:\n%s", names_dict)
                return names_dict
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la obtencion del diccionario de equivalencias de campos entre Fenics a Precia"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

    class TransformManager:
        def __init__(
            self,
            cols_dict: dict,
            fenics_names_vs_ids_df: pd.DataFrame,
            input_df: pd.DataFrame,
            valuation_date: dt.date,
            utils_engine: engine,
            db_secret: dict,
            names_dict: dict,
        ) -> None:
            try:
                self.missing_intitutions = []
                self.missing_counterparts = {}
                self.cols_dict = cols_dict
                self.fenics_names_vs_ids_df = fenics_names_vs_ids_df
                self.input_df = input_df
                self.utils_engine = utils_engine
                self.prc_engine = create_engine(
                    db_secret["conn_string_process"] + db_secret["schema_process"],
                    connect_args={"connect_timeout": CdsFenicsETL.DB_TIMEOUT},
                )
                self.names_dict = names_dict
                self.expected_cols = list(self.cols_dict.values())
                self.expected_intitutions = self.fenics_names_vs_ids_df[
                    "institution"
                ].to_list()
                self.expected_couterparts = self.fenics_names_vs_ids_df[
                    "counterparty"
                ].to_list()
                self.valuation_date = valuation_date
                self.valuation_date_str = valuation_date.strftime("%Y-%m-%d")
                self.new_cols_dict = {
                    "valuation_date": self.valuation_date,
                    "id_precia": lambda x: x["counterparty"] + "_" + x["tenor"],
                    "origin_date": self.valuation_date,  # Esto aplica para la info que viene en el insumo
                }
                self.output_df = self.run_trans_manager()
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto TransformManager"
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
                logger.debug("self.expected_intitutions: %s", self.expected_intitutions)
                if "institution" in process_df.columns:
                    process_df = process_df[  # Filtro de filas
                        process_df["institution"].isin(self.expected_intitutions)
                    ]
                logger.info("Dataframe filtrado:\n%s", process_df)
                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "No se logro extraer la informacion de interes"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def scale(self, process_df: pd.DataFrame) -> pd.DataFrame:
            try:
                FACTOR = 10000
                logger.info("Dividiendo precios en %s...", FACTOR)
                process_df = process_df.copy()
                process_df[CdsFenicsETL.NODES] = process_df[CdsFenicsETL.NODES].div(
                    FACTOR
                )
                logger.info("Dataframe Escalado:\n%s", process_df)
                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "No se logro dividir los precios"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def unpivot(self, process_df: pd.DataFrame) -> pd.DataFrame:
            try:
                process_df = process_df.melt(
                    id_vars=["institution", "counterparty"],
                    value_vars=CdsFenicsETL.NODES,
                    var_name="tenor",
                    value_name="mid_price",
                )
                logger.info("Dataframe Pivoteado:\n%s", process_df)
                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "No se logro dividir los precios"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_cds_parameters(self, counterparty_list: list) -> pd.DataFrame:
            try:
                select_query = text(
                    f"""
                    SELECT counterparty, month_ini, month_end, maturity_month_1,
                    maturity_month_2 FROM {CdsFenicsETL.MATURITY_PARAMS_TABLE} WHERE
                    counterparty IN :counterparty_list
                    """
                )
                query_params = {"counterparty_list": counterparty_list}
                parameters_df = pd.read_sql(
                    select_query, con=self.utils_engine, params=query_params
                )
                return parameters_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Falla la consulta de los parametros para calculo de fecha de maduracion"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def get_last_cds_prices(self, counterparty_list: list) -> pd.DataFrame:
            try:
                logger.info(
                    "Consultando en BD los precios mas recientes para los cds que no vienen en el insumo..."
                )
                select_query = text(
                    f"""
                    SELECT counterparty, id_precia,tenor,institution,maturity_date,days,mid_price,valuation_date,origin_date
                    FROM {CdsFenicsETL.CDS_TABLE}
                    WHERE status_info = '1' 
                    AND (counterparty, valuation_date) IN
                    (SELECT counterparty,max(valuation_date) FROM {CdsFenicsETL.CDS_TABLE}
                    where counterparty IN :counterparty_list 
                    AND valuation_date < '{self.valuation_date_str}'
                    AND status_info = '1'
                    GROUP BY counterparty)
                    """
                )
                query_params = {"counterparty_list": counterparty_list}
                last_cds_prices_df = pd.read_sql(
                    select_query, con=self.prc_engine, params=query_params
                )
                logger.info(
                    "Ultimos precios de CDS faltantes en BD:\n%s", last_cds_prices_df
                )
                return last_cds_prices_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = (
                    "Falla la consulta de los precios mas recientes para los cds en BD"
                )
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        @staticmethod
        def maturity_cds(cds_info, fecha_dt):
            try:
                """
                @author: SebastianVelezHernan
                Genera la fecha de vencimiento de un cds dado el tenor, la fecha de valoracion y el conjunto de paramatros del ciclo

                Params:
                    cds_info (pd.dataframe): Dataframe con las caracteristicas del ciclo del cds
                        tenor: tenor del cds
                        month_ini: mes de inicio del primer ciclo
                        month_end: mes de finalizacion del primer ciclo
                        maturity_month_1: mes vencimiento primer ciclo
                        maturity_month_2: mes vencimiento segundo ciclo
                    fecha_dt (datetime.date): fecha de valoracion desde la cual se calcula la fecha

                Return:
                    mat_date (datetime.date): Fecha de vencimiento del cds
                    cds_info["tenor"] (str): Tenor sobre el cual se calculo la fecha de vencimiento

                """
                if cds_info["tenor"] == "6M":
                    if fecha_dt >= date(
                        fecha_dt.year, cds_info["month_ini"], 20
                    ) and fecha_dt < date(fecha_dt.year, cds_info["month_end"], 20):
                        month_mat = cds_info["maturity_month_2"]
                    else:
                        month_mat = cds_info["maturity_month_1"]
                    if fecha_dt < date(fecha_dt.year, cds_info["month_end"], 20):
                        year_mat = fecha_dt.year
                    else:
                        year_mat = fecha_dt.year + 1
                else:
                    tenor_years = int(
                        "".join([char for char in cds_info["tenor"] if char.isdigit()])
                    )
                    if fecha_dt >= date(
                        fecha_dt.year, cds_info["month_ini"], 20
                    ) and fecha_dt < date(fecha_dt.year, cds_info["month_end"], 20):
                        month_mat = cds_info["maturity_month_1"]
                    else:
                        month_mat = cds_info["maturity_month_2"]
                    if fecha_dt < date(fecha_dt.year, cds_info["month_ini"], 20):
                        year_mat = fecha_dt.year + tenor_years - 1
                    else:
                        year_mat = fecha_dt.year + tenor_years
                mat_date = date(year_mat, month_mat, 20)
                return mat_date, cds_info["id_precia"]
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo el calculo de fecha de maduracion"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def add_maturity_and_days(self, cds_df: pd.DataFrame):
            try:
                logger.info("Calculando fechas de maduracion y dias...")
                cds_df = cds_df.copy()
                cds_parameters_df = self.get_cds_parameters(
                    cds_df["counterparty"].to_list()
                )
                maturity_dates_serie = cds_df.merge(cds_parameters_df).apply(
                    lambda row: self.maturity_cds(row, self.valuation_date), axis=1
                )
                maturity_dates_df = pd.DataFrame(
                    maturity_dates_serie.to_list(),
                    columns=["maturity_date", "id_precia"],
                )
                cds_df = cds_df.merge(maturity_dates_df)
                cds_df["days"] = (
                    cds_df["maturity_date"] - cds_df["valuation_date"]
                ).dt.days
                logger.info("Df con fechas de maduracion y dias:\n%s", cds_df)
                return cds_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo el calculo de fechas de maduracion y dias"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def add_missing_counterparts(self, process_df):
            try:
                missing_counterparts_in_input = set(self.expected_couterparts) - set(
                    process_df["counterparty"]
                )
                missing_counterparts = missing_counterparts_in_input
                logger.warning(
                    "Contrapartes faltantes en el insumo (%s): %s",
                    len(missing_counterparts_in_input),
                    missing_counterparts_in_input,
                )
                last_cds_prices_df = None
                if len(missing_counterparts_in_input) != 0:
                    last_cds_prices_df = self.get_last_cds_prices(
                        list(missing_counterparts_in_input)
                    )
                if (
                    isinstance(last_cds_prices_df, pd.DataFrame)
                    and not last_cds_prices_df.empty
                ):
                    missing_counterparts = missing_counterparts_in_input - set(
                        last_cds_prices_df["counterparty"]
                    )
                    last_cds_prices_df["valuation_date"] = self.valuation_date
                    last_cds_prices_df["days"] = (
                        last_cds_prices_df["maturity_date"]
                        - last_cds_prices_df["valuation_date"]
                    ).dt.days
                    process_df = pd.concat(
                        [process_df, last_cds_prices_df], ignore_index=True
                    )
                    logger.info(
                        "Dataframe con curvas faltantes agregadas:\n%s", process_df
                    )
                return process_df, missing_counterparts
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la adicion de contrapartes faltantes en el insumo"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def run_trans_manager(self) -> None:
            try:
                logger.info("Traduciendo df...")
                process_df = self.input_df.rename(columns=self.cols_dict)
                process_df["institution"] = process_df["institution"].str.lower()
                logger.info("Dataframe traducido:\n%s", process_df)
                process_df = self.filter(process_df)
                process_df = self.scale(process_df)
                process_df = process_df.merge(
                    self.fenics_names_vs_ids_df, on="institution"
                )
                logger.info("Dataframe con columna counterparty:\n%s", process_df)
                process_df = process_df.replace(self.names_dict)
                logger.info(
                    "Dataframe con nombres oficiales de instituciones:\n%s", process_df
                )
                process_df = self.unpivot(process_df)
                logger.info("Agregando columnas al df...")
                process_df = process_df.assign(**self.new_cols_dict)
                logger.info("Dataframe con columnas agregadas:\n%s", process_df)
                process_df = self.add_maturity_and_days(process_df)
                (
                    process_df,
                    self.missing_counterparts,
                ) = self.add_missing_counterparts(process_df)
                if len(self.missing_counterparts) != 0:
                    missing_intitutions_df = self.fenics_names_vs_ids_df[
                        self.fenics_names_vs_ids_df["counterparty"].isin(
                            self.missing_counterparts
                        )
                    ]
                    logger.error(
                        "Contrapartes que no estan ni en el insumo ni en BD:\n%s",
                        missing_intitutions_df,
                    )
                    self.missing_intitutions = missing_intitutions_df[
                        "institution"
                    ].to_list()

                return process_df
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la transformacion de informacion del dataframe"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

    class LoadManager:
        def __init__(
            self,
            db_secret: dict,
            output_df: pd.DataFrame,
            valuation_date: dt.date,
        ) -> None:
            try:
                self.output_df = output_df
                self.valuation_date = valuation_date
                self.prc_engine = create_engine(
                    db_secret["conn_string_process"] + db_secret["schema_process"],
                    connect_args={"connect_timeout": CdsFenicsETL.DB_TIMEOUT},
                )
                self.disable_previous_info()
                self.insert_df()
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la creacion del objeto LoadManager"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def disable_previous_info(self) -> None:
            """
            Deshabilita la informacion previa cargada en BD para la fecha y la
            curva (intitucion

            Raises:
                PlataformError: Cuando la deshabilitacion falla
            """
            try:
                logger.info("Deshabilitando informacion previa en BD process...")
                update_query = text(
                    f"""
                    UPDATE {CdsFenicsETL.CDS_TABLE} SET status_info= '0' WHERE
                    valuation_date = '{self.valuation_date}' AND counterparty IN
                    :curve_list
                    """
                )
                logger.debug(
                    'self.output_df["counterparty"].tolist(): %s',
                    list(set(self.output_df["counterparty"])),
                )
                logger.debug(
                    "self.valuation_date: %s",
                    self.valuation_date,
                )
                query_params = {
                    "curve_list": list(set(self.output_df["counterparty"])),
                }

                with self.prc_engine.connect() as conn:
                    conn.execute(update_query, query_params)
                logger.info(
                    "Dehabilitacion de informacion previa en BD process exitosa"
                )
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la deshabilitacion de informacion en BD process"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

        def insert_df(self) -> None:
            """Inserta en BD el dataframe de la curva de insumo

            Raises:
                PlataformError: Cuando la insercion falla
            """
            try:
                logger.info("Insertando df en BD process...")
                with self.prc_engine.connect() as conn:
                    self.output_df.to_sql(
                        CdsFenicsETL.CDS_TABLE,
                        con=conn,
                        if_exists="append",
                        index=False,
                    )
                logger.info("Insercion de df en BD process exitosa")
            except (Exception,) as gen_exc:
                global first_error_msg
                raise_msg = "Fallo la insercion del dataframe en BD process"
                error_msg = create_log_msg(raise_msg)
                logger.error(error_msg)
                if not first_error_msg:
                    first_error_msg = error_msg
                raise PlataformError(raise_msg) from gen_exc

    def run_etl(self) -> None:
        try:
            extract_manager = CdsFenicsETL.ExtractManager(
                db_secret=self.db_secret, s3_path=self.s3_path
            )
            transform_manager = CdsFenicsETL.TransformManager(
                cols_dict=extract_manager.fields_dict,
                fenics_names_vs_ids_df=extract_manager.curves_df,
                input_df=self.input_df,
                valuation_date=extract_manager.valuation_date,
                utils_engine=extract_manager.utils_engine,
                db_secret=self.db_secret,
                names_dict=extract_manager.names_dict,
            )
            self.missing_intitutions = transform_manager.missing_intitutions
            self.missing_counterparts = transform_manager.missing_counterparts
            load_manager = CdsFenicsETL.LoadManager(
                db_secret=self.db_secret,
                output_df=transform_manager.output_df,
                valuation_date=extract_manager.valuation_date,
            )
            self.counterparts_loaded_in_bd = list(
                set(transform_manager.output_df["counterparty"])
            )
            logger.info(
                "Contrapartes cargadas en BD (%s): %s",
                len(self.counterparts_loaded_in_bd),
                self.counterparts_loaded_in_bd,
            )

        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion del objeto CdsFenicsETL"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc


class GlueManager:
    PARAMS = [
        "DB_SECRET",
        "LBD_CSV_VALIDATOR",
        "S3_SCHEMA_BUCKET",
        "S3_FILE_PATH",
        "S3_CDS_BUCKET",
        "LBD_PRC_ETL",
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
            logger.info("Parametros obtenidos del Glue:%s", params)
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


class DbManager:
    TRANSLATE_DICT_TABLE = "precia_utils_cds_suppliers_dictionary"
    DB_TIMEOUT = 2

    def __init__(self, db_secret) -> None:
        try:
            self.utils_engine = create_engine(
                db_secret["conn_string_sources"] + db_secret["schema_utils"],
                connect_args={"connect_timeout": DbManager.DB_TIMEOUT},
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
                    SELECT DISTINCT precia_alias FROM
                    {DbManager.TRANSLATE_DICT_TABLE}
                    WHERE id_supplier = 'Fenics' AND alias_type = 'id'
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


class Main:
    @staticmethod
    def validate_input(
        s3_path: str, s3_schema_bucket: str, s3_cds_bucket: str, lbd_csv_validator: str
    ) -> None:
        try:
            payload = {
                "s3CsvPath": path_join("Megatron", path_basename(s3_path)),
                "s3SchemaPath": "cds/international/cds_fenics_schema.csvs",
                "s3SchemaDescriptionPath": "cds/international/cds_fenics_schema_description.txt",
                "s3SchemaBucket": s3_schema_bucket,
                "s3CsvBucket": s3_cds_bucket,
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
    def main():
        global first_error_msg
        try:
            logger.info("Ejecutando el job de glue...")
            fenics_curves = []
            report_lbd_name = ParameterManager.get_parameter_from_ssm() 

            filename = ""
            params_dict = GlueManager.get_params()
            try:
                filename = path_basename(params_dict["S3_FILE_PATH"])
                original_date_str = filename.split(".")[0][-6:]
                valuation_date = dt.strptime(original_date_str, "%d%m%y").date()
                valuation_date_str = valuation_date.strftime("%Y-%m-%d")
            except (Exception,):
                raise_msg = "Fallo la extraccion de la fecha a partir del filename. Se tomara la fecha de hoy en su lugar"
                logger.warning(create_log_msg(raise_msg))
                today_date = dt.now().date()
                valuation_date_str = today_date.strftime("%Y-%m-%d")
            db_manager = DbManager(SecretsManager.get_secret(params_dict["DB_SECRET"]))
            fenics_curves = db_manager.get_fenics_curves()
            input_df = S3Manager.get_object(
                s3_bucket_name=params_dict["S3_CDS_BUCKET"],
                s3_path=params_dict["S3_FILE_PATH"],
            )
            S3Manager.put_object(
                file_df=input_df,
                s3_bucket_name=params_dict["S3_CDS_BUCKET"],
                s3_path=params_dict["S3_FILE_PATH"],
            )
            
            Main.validate_input(
                s3_path=params_dict["S3_FILE_PATH"],
                s3_schema_bucket=params_dict["S3_SCHEMA_BUCKET"],
                s3_cds_bucket=params_dict["S3_CDS_BUCKET"],
                lbd_csv_validator=params_dict["LBD_CSV_VALIDATOR"],
            )
            cds_fenics_etl = CdsFenicsETL(
                s3_path=params_dict["S3_FILE_PATH"],
                input_df=input_df,
                db_secret=SecretsManager.get_secret(params_dict["DB_SECRET"]),
            )
            payload = {
                "instrument": cds_fenics_etl.counterparts_loaded_in_bd,
                "valuation_date": valuation_date_str,
            }
            
            LambdaManager.invoke(
                lambda_name=params_dict["LBD_PRC_ETL"], payload=payload
            )
            if len(cds_fenics_etl.missing_counterparts) != 0:
                raise_msg = "Los siguientes contrapartes no se encuentran ni en el insumo ni en BD: "
                raise_msg += f"{cds_fenics_etl.missing_counterparts} ({cds_fenics_etl.missing_intitutions})"
                raise MissingCdsError(raise_msg)
            logger.info("Ejecucion del job de glue exitosa!!!")
            LambdaManager.update_report_process(
                status="Exitoso",
                lambda_name=report_lbd_name,
                valuation_date=valuation_date_str,
                curve_name_list=fenics_curves,
                description="Proceso Finalizado",
                filename=filename,
            )
        except MissingCdsError as mcd_exc:
            main_main_msg = "Fallo la ejecucion del job de glue"
            main_raise_msg = create_log_msg(main_main_msg)
            logger.critical(main_raise_msg)
            if not first_error_msg:
                first_error_msg = main_raise_msg
            LambdaManager.update_report_process(
                status="Fallido",
                lambda_name=report_lbd_name,
                valuation_date=valuation_date_str,
                curve_name_list=list(cds_fenics_etl.missing_counterparts),
                technical_description=first_error_msg,
                description=str(mcd_exc),
                filename=filename,
            )
            raise PlataformError(first_error_msg) from mcd_exc
        except (Exception,) as gen_exc:
            main_main_msg = "Fallo la ejecucion del job de glue"
            main_raise_msg = create_log_msg(main_main_msg)
            logger.critical(main_raise_msg)
            if not first_error_msg:
                first_error_msg = main_raise_msg
            LambdaManager.update_report_process(
                status="Fallido",
                lambda_name=report_lbd_name,
                valuation_date=valuation_date_str,
                curve_name_list=fenics_curves,
                technical_description=first_error_msg,
                description=str(gen_exc),
                filename=filename,
            )
            raise PlataformError(first_error_msg) from gen_exc


if __name__ == "__main__":
    Main().main() 

