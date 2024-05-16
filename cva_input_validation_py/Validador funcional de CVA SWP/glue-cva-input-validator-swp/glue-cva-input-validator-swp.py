"""
===============================================================================

Nombre: glu-cva-validator.py

Tipo: Glue Job

Autor:
    - Hector Augusto Daza Roa
    Tecnologia - Precia

Ultima modificacion: 25/08/2023

Este script realiza la validacion funcional de los archivos de portafolios
swaps y swaps_cf de CVA. Y algunas validaciones de nombre y estructura. Luego
lanza una ejecucion del validador generico de estructura de archivos csv
(LBD_CSV_VALIDATOR). Con el resultado de ambas validaciones toma una decision:
Deteniene su ejecucion con error, o envia correo con el resultado de la
validacion al cliente, o carga el archivo al datanfs.

Parametros del Glue Job (Job parameters o Input arguments):
    "--DB_SECRET"= <<Nombre del secreto de BD>>
    "--SMTP_SECRET"= <<Nombre del secreto del servidor de correos>>
    "--DATANFS_SECRET"= <<Nombre del secreto del servidor de FTP datanfs>>
    "--LBD_CSV_VALIDATOR"= "lbd-%env-glue-trigger-cva-validator"
    "--S3_SCHEMA_BUCKET"= "s3-%env-csv-validator"
    "--S3_CVA_PATH"= <<Ruta del archivo de insumo en el bucket de S3 (Viene de
    la lambda lbd-%env-cva-glue-trigger-validator)>>
    "--S3_CVA_BUCKET"= <<Nombre del bucket de S3 donde esta el archivo de
    insumo (Viene de la lambda lbd-%env-swi-glue-trigger-etl): s3-%env-cva-inputs>>
    %env: Ambiente: dev, qa o p

===============================================================================
"""
import boto3, re, io, json, logging, os
from datetime import datetime

import pandas as pd, sqlalchemy as sa, paramiko

from common_library_email_report.ReportEmail import ReportEmail
from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError, UserError

logger = setup_logging(logging.INFO)
first_error_msg = None


class FunctionalValidator:
    SWP_CF_HEADER = ["ID", "FECHA", "PATA", "AMORT", ""]
    SWP_HEADER = [
        "ID",
        "TIPO",
        "INICIO",
        "VTO",
        "MONEDA1",
        "MONEDA2",
        "PATA1",
        "PATA2",
        "STRIKE1",
        "STRIKE2",
        "NOCIONAL1",
        "NOCIONAL2",
        "PR1",
        "PR2",
        "CONV1",
        "CONV2",
        "PROY1",
        "PROY2",
        "DESC1",
        "DESC2",
        "GRUPO_NETEO",
        "CONTRAPARTE",
        "ID_DEFAULT",
        "COLATERAL_MONEDA",
    ]
    EXPECTED_DATE_COLS = ["INICIO", "VTO", "FECHA"]

    def __init__(
        self,
        s3_filepath: str,
        swaps_file: io.StringIO,
        swaps_cf_file: io.StringIO,
        clients_df: pd.DataFrame,
        patas_df: pd.DataFrame,
        previous_amount_swaps_cf_df: pd.DataFrame,
    ) -> None:
        try:
            self.swaps_file = swaps_file
            self.swaps_cf_file = swaps_cf_file
            self.clients_df = clients_df
            self.patas_df = patas_df
            self.previous_amount_swaps_cf_df = previous_amount_swaps_cf_df
            self.today_amount_swaps_cf_df = None
            self.new_swaps_cf_df = None
            self.nit_list = clients_df["nit"].to_list()
            self.nit_swp_list = clients_df.loc[
                self.clients_df["swaps_type"] != "NA", "nit"
            ].to_list()
            self.s3_filepath = s3_filepath
            self.swaps_filename, self.swaps_cf_filename = self.set_filenames(
                base_filename=s3_filepath.split("/")[-1]
            )
            logger.info("swaps_filename: %s", self.swaps_filename)
            logger.info("swaps_cf_filename: %s", self.swaps_cf_filename)
            self.swaps_df = None
            self.all_swaps_info, self.swaps_df = self.scan_and_validate(
                filename=self.swaps_filename, file=self.swaps_file
            )
            self.all_swaps_cf_info, self.swaps_cf_df = self.scan_and_validate(
                filename=self.swaps_cf_filename, file=self.swaps_cf_file
            )
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion del objeto FunctionalValidator"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def validate_ids(
        self, swaps_df: pd.DataFrame, swaps_cf_df: pd.DataFrame, scan_result_dict: dict
    ):
        try:
            logger.info("Validando IDs de swaps vs IDs de swaps_cf...")
            swaps_ids_set = set(swaps_df["ID"].to_list())
            swaps_cf_ids_set = set(swaps_cf_df["ID"].to_list())
            if swaps_ids_set == swaps_cf_ids_set:
                msg = "Todos los IDs del archivo swaps estan contenidos en el"
                msg += "archivo swaps_cf y viceversa"
                scan_result_dict["ids_swaps_vs_ids_swaps_cf"] = {
                    "is_valid": True,
                    "message": msg,
                }
            else:
                missing_swaps_ids = list(swaps_cf_ids_set - swaps_ids_set)
                missing_swaps_cf_ids = list(swaps_ids_set - swaps_cf_ids_set)
                missing_ids_list = missing_swaps_ids + missing_swaps_cf_ids
                msg = "NO todos los IDs del archivo swaps estan contenidos en el"
                msg += " archivo swaps_cf o viceversa. Verifique los siguientes"
                msg += f" IDs en ambos archivos: {missing_ids_list}\n"
                scan_result_dict["ids_swaps_vs_ids_swaps_cf"] = {
                    "is_valid": False,
                    "message": msg,
                }
            logger.info("IDs de swaps vs IDs de swaps_cf validados con exito")
            return scan_result_dict
        except (Exception,):
            raise_msg = "Fallo la comparacion de ids de swaps vs ids de swaps_cf"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            return scan_result_dict

    def validate_id_vs_patas(self, swaps_cf_df: pd.DataFrame, scan_result_dict: dict):
        try:
            logger.info(
                "Validando existencia de PATA 1 y 2 en swaps_cf para todos los IDs..."
            )
            id_group = swaps_cf_df.groupby("ID")["PATA"]
            contains_1_and_2 = id_group.apply(lambda x: {1, 2} == set(x))
            if contains_1_and_2.all():
                msg = "Todos los IDs del archivo swaps_cf tienen valores"
                msg += '"1" y "2" en la columna PATA'
                scan_result_dict["patas_are_complete"] = {
                    "is_valid": True,
                    "message": msg,
                }
            else:
                missing_patas = id_group.apply(
                    lambda x: [f"'{i}'" for i in list({1, 2} - set(x))]
                ).reset_index()
                contains_1_and_2_df = contains_1_and_2.reset_index()
                incompleted_ids = contains_1_and_2_df.reset_index().loc[
                    contains_1_and_2_df["PATA"] == False, ["ID"]
                ]
                incompleted_ids["PATAS_FALTANTES"] = missing_patas.loc[
                    contains_1_and_2_df["PATA"] == False, ["PATA"]
                ]
                msg = 'NO todos los IDs del archivo swaps_cf tienen valores "1" '
                msg += ' y "2" en la  columna PATA. Los siguientes IDs tienen '
                msg += f"incompletas sus patas:\n\n{incompleted_ids.to_string(index=False)}\n"
                scan_result_dict["patas_are_complete"] = {
                    "is_valid": False,
                    "message": msg,
                }
            logger.info(
                "Existencia de PATA 1 y 2 en swaps_cf para todos los IDs validada con exito"
            )
            return scan_result_dict
        except (Exception,):
            raise_msg = (
                "Fallo la validacion de la existencia de patas 1 y 2 para cada ID"
            )
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            return scan_result_dict

    def set_filenames(self, base_filename):
        try:
            logger.info("Estableciendo nombres de los archivos...")
            swaps_cf_filename = None
            swaps_filename = None
            if "swaps_cf" in base_filename:
                swaps_cf_filename = base_filename
                swaps_filename = base_filename.replace("swaps_cf", "swaps")
            else:
                swaps_cf_filename = base_filename.replace("swaps_", "swaps_cf_")
                swaps_filename = base_filename
            return swaps_filename, swaps_cf_filename
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la determinacion de los nombres de archivo"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def scan_and_validate(self, filename: str, file: io.StringIO):
        try:
            logger.info("Realizando escaneo y validacion de %s", filename)
            metainfo = self.get_metainfo(filename)
            if metainfo:
                client_dict = self.get_client_info(metainfo)
            logger.debug("type(file): %s", type(file))
            scan_result_dict, input_df = self.scan(metainfo=metainfo, file=file)
            (
                validation_synthesis,
                send_email,
                transfer_file,
                generic_validation_needed,
            ) = self.fully_validate(scan_result_dict=scan_result_dict)
            logger.info("Escaneo y validacion de %s exitosa", filename)
            return {
                "metainfo": metainfo,
                "client_dict": client_dict,
                "scan_result_dict": scan_result_dict,
                "validation_synthesis": validation_synthesis,
                "send_email": send_email,
                "transfer_file": transfer_file,
                "generic_validation_needed": generic_validation_needed,
            }, input_df
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la determinacion de los nombres de archivo"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def count_amount_swaps_cf(self, input_df: pd.DataFrame, nit: int):
        try:
            logger.info("Contando los flujos por combinacion ID-PATA...")
            id_pata_df = input_df[["ID", "PATA"]]
            amount = id_pata_df.apply(tuple, axis=1).value_counts()
            amount_df = pd.DataFrame(
                amount.index.tolist(), columns=["id", "pata"]
            ).assign(amount=amount.values, nit=nit)
            logger.info("Conteo de flujos por combinacion ID-PATA exitoso")
            return amount_df
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo el conteo de los flujos por combinacion id-pata"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def get_metainfo(self, filename: str) -> dict:
        try:
            logger.info("Obteniendo informacion del nombre del insumo...")
            NIT_REGEX = r"\d+"
            PRODUCT_REGEX = r"[a-zA-Z]+(_[a-zA-Z]+)?"
            VALUATION_DATE_REGEX = r"\d{8}"
            EXTENSION_REGEX = r"[a-zA-Z]+"
            FILE_REGEX = r"^({nit_regex})_({product_regex})_({valuation_date_regex})\.({extension_regex})$"
            match = re.match(
                FILE_REGEX.format(
                    nit_regex=NIT_REGEX,
                    product_regex=PRODUCT_REGEX,
                    valuation_date_regex=VALUATION_DATE_REGEX,
                    extension_regex=EXTENSION_REGEX,
                ),
                filename,
            )
            metainfo = None
            if match:
                metainfo = {
                    "nit": match.group(1),
                    "product": match.group(2),
                    "valuation_date": match.group(4),
                    "extension": match.group(5),
                }
            logger.info("Metainfo: %s", json.dumps(metainfo))
            return metainfo
        except (Exception,) as file_exc:
            global first_error_msg
            raise_msg = f"Fallo la obtencion de la metainfo del insumo {self.filename}"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from file_exc

    def get_client_info(self, metainfo):
        try:
            logger.info("Obteniendo informacion del cliente...")
            client_df = self.clients_df.loc[self.clients_df["nit"] == metainfo["nit"]]
            client_dict = {}
            if not client_df.empty:
                client_dict["name"] = client_df["cva_client"].iloc[0]
                client_dict["client_type"] = client_df[
                    f"{metainfo['product']}_type"
                ].iloc[0]
                client_dict["product"] = metainfo["product"]
                client_dict["schema_type"] = metainfo["nit"]
                if client_dict["client_type"] == "generic":
                    client_dict["schema_type"] = client_dict["client_type"]
                client_dict["email"] = client_df["email"].iloc[0]
                logger.info("Informacion del cliente: %s", json.dumps(client_dict))
            return client_dict
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la obtencion de la informacion del cliente"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def scan_metainfo(self, scan_result_dict: dict, metainfo: dict) -> dict:
        try:

            def update_scan_result(key, condition, valid_msg, invalid_msg):
                if condition:
                    scan_result_dict[key] = {
                        "is_valid": True,
                        "message": valid_msg,
                    }
                else:
                    scan_result_dict[key] = {
                        "is_valid": False,
                        "message": invalid_msg,
                    }

            logger.info("Validando extension del archivo...")
            update_scan_result(
                "extension",
                metainfo["extension"] == "csv",
                "La extension del archivo es csv.",
                f"La extension del archivo es {metainfo['extension']} pero debe ser csv.",
            )
            logger.info("Extension del archivo validada con exito")
            logger.info("Validando NIT...")
            update_scan_result(
                "nit",
                metainfo["nit"] in self.nit_list,
                f"El nit {metainfo['nit']} se encuentra en la lista de clientes de Precia.",
                f"El nit {metainfo['nit']} NO se encuentra en la lista de clientes de Precia.",
            )
            logger.info("NIT validado con exito")
            logger.info("Validando si es cliente de swap...")
            update_scan_result(
                "is_swp_client",
                metainfo["nit"] in self.nit_swp_list,
                f"Actualmente si se valora el portafolio de swaps para este cliente: {metainfo['nit']}",
                f"Actualmente NO se valora el portafolio de swaps para este cliente: {metainfo['nit']}",
            )
            logger.info("Validacion de si es cliente de swap exitosa")
            logger.info("Validando si el producto es swap...")
            update_scan_result(
                "product",
                metainfo["product"] in ["swaps", "swaps_cf"],
                f"El producto ({metainfo['product']}) si se valida en este job de glue",
                f"El producto ({metainfo['product']}) NO se valida en este job de glue (solo valida swaps)",
            )
            logger.info("Validacion de si el producto es swap exitosa")
            return scan_result_dict
        except Exception as gen_exc:
            global first_error_msg
            raise_msg = f"Fallo el scaneo de la metainfo del insumo {self.filename}"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def detect_separator(self, input_file: io.StringIO, scan_result_dict: dict):
        try:
            logger.info("Detectando el separador del archivo...")
            input_file.seek(0)
            first_lines = [input_file.readline() for _ in range(2)]
            comma_count = sum(line.count(",") for line in first_lines)
            semicolon_count = sum(line.count(";") for line in first_lines)
            separator = None
            if comma_count > semicolon_count:
                separator = ","
            elif semicolon_count > comma_count:
                separator = ";"
            else:
                input_file.seek(0)
                content = input_file.getvalue()
                is_empty = len(content.strip()) == 0
                if is_empty:
                    scan_result_dict["empty_file"] = {
                        "is_valid": False,
                        "message": "El archivo esta vacio",
                    }
                else:
                    message = "El separador es invalido o el contenido del archivo "
                    message += "es ambiguo y no permite realizar la inferencia del "
                    message += 'separador. Los separadores aceptados son "," o ";"'
                    scan_result_dict["separator"] = {
                        "is_valid": False,
                        "message": message,
                    }
                return separator
            scan_result_dict["separator"] = {
                "is_valid": True,
                "message": f"El separador del archivo es '{separator}'",
            }
            logger.info("Deteccion de separador del archivo exitosa")
            return separator
        except (Exception,) as file_exc:
            global first_error_msg
            raise_msg = "Fallo la determinacion del separador del insumo"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from file_exc

    def has_header(self, input_file, separator):
        try:
            logger.info("Identificando si el archivo tiene encabezado...")
            input_file.seek(0)
            first_line = input_file.readline()
            elements = first_line.strip().split(separator)
            if all(elem != "" and not elem.isnumeric() for elem in elements):
                logger.info("El archivo tiene encabezado")
                return True
            logger.info("El archivo NO tiene encabezado")
            return False
        except (Exception,) as file_exc:
            global first_error_msg
            raise_msg = f"Fallo la validacion del encabezado del insumo {self.filename}"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from file_exc

    def convert_to_dataframe(
        self, separator: str, scan_result_dict: dict, metainfo: dict, file: io.StringIO
    ) -> pd.DataFrame:
        input_df = None
        global first_error_msg
        raise_msg = "Fallo la conversion del insumo a dataframe"
        try:
            logger.info("Convirtiendo el archivo en dataframe...")
            file_has_header = self.has_header(input_file=file, separator=separator)
            file.seek(0)
            if not file_has_header and metainfo["product"] == "swaps":
                input_df: pd.DataFrame = pd.read_csv(
                    file, sep=separator, names=FunctionalValidator.SWP_HEADER
                )
            elif not file_has_header and metainfo["product"] == "swaps_cf":
                input_df: pd.DataFrame = pd.read_csv(
                    file,
                    sep=separator,
                    names=FunctionalValidator.SWP_CF_HEADER,
                )
            else:
                input_df: pd.DataFrame = pd.read_csv(file, sep=separator)
            scan_result_dict["empty_file"] = {
                "is_valid": True,
                "message": "El archivo NO esta vacio",
            }
            scan_result_dict["file_to_dataframe"] = {
                "is_valid": True,
                "message": "El archivo tiene una estructura convertible en dataframe",
            }
            logger.debug("Dataframe a validar:\n%s", input_df)
            logger.info("Conversion del archivo a dataframe exitosa")
            return scan_result_dict, input_df
        except pd.errors.EmptyDataError:
            raise_msg += "El archivo de insumo esta vacio"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            scan_result_dict["empty_file"] = {
                "is_valid": False,
                "message": "El archivo esta vacio",
            }
            return scan_result_dict, input_df
        except pd.errors.ParserError:
            raise_msg += "La estructura del archivo esta corrupta: "
            raise_msg += "Verifique comas, filas, columnas, llaves, etc"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            scan_result_dict["file_to_dataframe"] = {
                "is_valid": False,
                "message": "El archivo no tiene una estructura valida",
            }
            return scan_result_dict, input_df
        except (Exception,) as dfb_exc:
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from dfb_exc

    def validate_patas(self, input_df: pd.DataFrame, scan_result_dict: dict) -> dict:
        try:
            logger.info("Validando combinaciones de patas...")
            merged = input_df[["PATA1", "PATA2"]].merge(
                self.patas_df,
                how="left",
                left_on=["PATA1", "PATA2"],
                right_on=["pata1", "pata2"],
                indicator=True,
            )
            rows_not_in_reference = merged[merged["_merge"] == "left_only"]
            if rows_not_in_reference.empty:
                scan_result_dict["valid_patas"] = {
                    "is_valid": True,
                    "message": "Todas las combinaciones de patas son validas",
                }
            else:
                invalid_patas_df = (
                    input_df[["PATA1", "PATA2"]]
                    .iloc[rows_not_in_reference.index.to_list()]
                    .drop_duplicates()
                )
                message = "Algunas combinaciones de patas NO son validas:\n"
                message += f"\n{invalid_patas_df.to_string(index=False)}\n"
                scan_result_dict["valid_patas"] = {
                    "is_valid": False,
                    "message": message,
                }
            logger.info("Combinaciones de patas validadas con exito")
            return scan_result_dict
        except (Exception,):
            raise_msg = "Fallo la validacion de combinaciones de patas"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            return scan_result_dict

    def validate_date(self, fecha_str: str):
        try:
            datetime.strptime(fecha_str, "%d/%m/%Y")
            return True
        except Exception:
            return False

    def validate_date_cols(self, input_df: pd.DataFrame, scan_result_dict: dict):
        try:
            logger.info("Validando si las fechas son reales...")
            date_cols = list(
                set(input_df.columns).intersection(
                    FunctionalValidator.EXPECTED_DATE_COLS
                )
            )
            is_invalid_date_df = input_df[date_cols].apply(
                lambda date_serie: date_serie.apply(
                    lambda date: not self.validate_date(date)
                )
            )
            if is_invalid_date_df.any().any():
                invalid_dates_df = input_df.loc[
                    is_invalid_date_df.any(axis=1),
                    date_cols,
                ].drop_duplicates()
                message = "Hay fechas invalidas:\n"
                message += f"\n{invalid_dates_df.to_string(index=False)}\n"
                scan_result_dict["valid_dates"] = {
                    "is_valid": False,
                    "message": message,
                }
            else:
                scan_result_dict["valid_dates"] = {
                    "is_valid": True,
                    "message": "Las fechas del archivo son validas",
                }
            logger.info("Fechas validadas con exito")
            return scan_result_dict
        except (Exception,):
            raise_msg = "Fallo la validacion de las fechas"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            return scan_result_dict

    def validate_neteo_vs_contraparte(
        self, input_df: pd.DataFrame, scan_result_dict: dict
    ):
        try:
            logger.info("Validando GRUPO_NETEO vs CONTRAPARTE...")
            duplicates = input_df.groupby("GRUPO_NETEO")["CONTRAPARTE"].nunique().gt(1)
            invalid_values = duplicates[duplicates].index.tolist()
            if invalid_values:
                gn_vs_cp_df = input_df.loc[
                    input_df["GRUPO_NETEO"].isin(invalid_values),
                    ["GRUPO_NETEO", "CONTRAPARTE"],
                ].drop_duplicates()
                message = "Hay valores en 'GRUPO_NETEO' con mas de un valor diferente en 'CONTRAPARTE':\n"
                message += f"\n{gn_vs_cp_df.to_string(index=False)}\n"
                scan_result_dict["neteo_vs_contraparte"] = {
                    "is_valid": False,
                    "message": message,
                }
            else:
                scan_result_dict["neteo_vs_contraparte"] = {
                    "is_valid": True,
                    "message": "NO hay valores en 'GRUPO_NETEO' con mas de un valor diferente en 'CONTRAPARTE'.",
                }
            logger.info("GRUPO_NETEO vs CONTRAPARTE validado con exito")
            return scan_result_dict
        except (Exception,):
            raise_msg = "Fallo la validacion de la columna GRUPO_NETEO vs CONTRAPARTE"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            return scan_result_dict

    def scan_functionally(
        self, input_df: pd.DataFrame, scan_result_dict: dict, nit: str
    ):
        try:
            logger.info("Escaneando aspectos de negocio del archivo...")
            if "GRUPO_NETEO" in input_df and "CONTRAPARTE" in input_df:
                scan_result_dict = self.validate_neteo_vs_contraparte(
                    input_df, scan_result_dict
                )
            if input_df.columns.isin(FunctionalValidator.EXPECTED_DATE_COLS).any():
                scan_result_dict = self.validate_date_cols(
                    input_df=input_df, scan_result_dict=scan_result_dict
                )
            if "PATA1" in input_df and "PATA2" in input_df:
                scan_result_dict = self.validate_patas(
                    input_df=input_df, scan_result_dict=scan_result_dict
                )
            if (
                "ID" in input_df
                and "PATA" in input_df
                and isinstance(self.swaps_df, pd.DataFrame)
                and not self.swaps_df.empty
            ):
                scan_result_dict = self.validate_ids(
                    swaps_df=self.swaps_df,
                    swaps_cf_df=input_df,
                    scan_result_dict=scan_result_dict,
                )
                scan_result_dict = self.validate_id_vs_patas(
                    swaps_cf_df=input_df,
                    scan_result_dict=scan_result_dict,
                )
            if "ID" in input_df and "PATA" in input_df:
                self.validate_amount_swaps_cf(
                    input_df=input_df, scan_result_dict=scan_result_dict, nit=nit
                )
            logger.info("Escaneo de aspectos de negocio del archivo exitoso")
            return scan_result_dict
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la el escaneo funcional"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def validate_amount_swaps_cf(
        self, input_df: pd.DataFrame, scan_result_dict: dict, nit: str
    ):
        try:
            logger.info("Validando cantidad de flujos por combinacion ID-PATA...")
            self.today_amount_swaps_cf_df = self.count_amount_swaps_cf(
                input_df=input_df, nit=nit
            )
            if not self.previous_amount_swaps_cf_df.empty:
                data_types = {"id": str, "pata": int, "amount": int}
                old_amount_flag = (
                    self.today_amount_swaps_cf_df["id"]
                    .astype("str")
                    .isin(self.previous_amount_swaps_cf_df["id"].to_list())
                )
                old_amount_df = self.today_amount_swaps_cf_df[old_amount_flag].astype(
                    data_types
                )
                self.new_swaps_cf_df = self.today_amount_swaps_cf_df[~old_amount_flag]

                self.previous_amount_swaps_cf_df = (
                    self.previous_amount_swaps_cf_df.astype(data_types)
                )
                compare_amount_df = old_amount_df.merge(
                    self.previous_amount_swaps_cf_df, on=["id", "pata"], how="left"
                )
                compare_amount_df["delta_amount"] = (
                    compare_amount_df["amount_x"] - compare_amount_df["amount_y"]
                )
                amount_is_diff = compare_amount_df["delta_amount"].apply(
                    lambda x: False if x == 0 else True
                )
                if amount_is_diff.any():
                    invalid_id_pata_df = old_amount_df.loc[
                        amount_is_diff, ["id", "pata"]
                    ]
                    msg = "Las cantidades de flujos NO son las esperadas para las siguientes "
                    msg += f"combinaciones ID-PATA:\n\n{invalid_id_pata_df.to_string(index=False)}\n"
                    scan_result_dict["amount_of_swaps_cf"] = {
                        "is_valid": False,
                        "message": msg,
                    }
                else:
                    msg = "Las cantidades de flujos por combinaciones ID-PATA  son las esperadas"
                    scan_result_dict["amount_of_swaps_cf"] = {
                        "is_valid": True,
                        "message": msg,
                    }
            logger.info("Cantidad de flujos por combinacion ID-PATA validada con exito")
            return scan_result_dict
        except (Exception,):
            raise_msg = "Fallo la el escaneo de las cantidades de flujos por id-pata"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            return scan_result_dict

    def scan(self, metainfo, file: io.StringIO):
        try:
            logger.info("Escaneando archivo...")
            scan_result_dict = {}
            if metainfo:
                scan_result_dict["name_struct"] = {
                    "is_valid": True,
                    "message": "La estructura del nombre del archivo es valida",
                }
                scan_result_dict = self.scan_metainfo(
                    scan_result_dict=scan_result_dict, metainfo=metainfo
                )
                input_df = None
                if scan_result_dict["extension"]["is_valid"]:
                    self.separator = self.detect_separator(
                        input_file=file, scan_result_dict=scan_result_dict
                    )
                    if self.separator:
                        scan_result_dict, input_df = self.convert_to_dataframe(
                            scan_result_dict=scan_result_dict,
                            separator=self.separator,
                            metainfo=metainfo,
                            file=file,
                        )
                if isinstance(input_df, pd.DataFrame) and not input_df.empty:
                    scan_result_dict = self.scan_functionally(
                        input_df=input_df,
                        scan_result_dict=scan_result_dict,
                        nit=metainfo["nit"],
                    )

            else:
                scan_result_dict["name_struct"] = {
                    "is_valid": False,
                    "message": "La estructura del nombre del archivo no es valida",
                }
            logger.info("Escaneo del archivo exitoso")
            return scan_result_dict, input_df
        except (Exception,) as init_exc:
            global first_error_msg
            raise_msg = "Fallo el escaneo del archivo y de su nombre"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from init_exc

    def fully_meta_validate(self, scan_result_dict: dict):
        try:
            logger.info(
                "Validando el impacto de la metainfo y sintetisando el resultado de su escaneo..."
            )
            validation_synthesis = None
            transfer_file = None
            for key in ["name_struct", "product"]:
                if not scan_result_dict[key][
                    "is_valid"
                ]:  # Primera validacion que no cumpla detiene el proceso
                    validation_synthesis = scan_result_dict[key]["message"]
                    transfer_file = False
                    logger.error(validation_synthesis)
                    return validation_synthesis, transfer_file
            for key in [
                "is_swp_client",
                "extension",
            ]:  # Se Hacen todas las validaciones
                if not scan_result_dict[key]["is_valid"]:
                    message = scan_result_dict[key]["message"]
                    validation_synthesis = (
                        f"{validation_synthesis}\n{message}"
                        if validation_synthesis
                        else message
                    )
                    transfer_file = False
                    logger.error(message)
            if not validation_synthesis:
                validation_synthesis = "La metainformacion es correcta"
            logger.info(
                "Validacion del impacto de la metainfo y sintesis del resultado de su escaneo exitosa"
            )
            return validation_synthesis, transfer_file
        except Exception as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la validacion de la metainformacion"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def fully_content_validate(self, scan_result_dict):
        try:
            logger.info(
                "Validando el impacto del contenido y sintetisando el resultado de su escaneo..."
            )
            validation_synthesis = None
            send_email = False
            transfer_file = False
            generic_validation_needed = False

            for key in ["separator", "empty_file", "file_to_dataframe"]:
                if key in scan_result_dict and not scan_result_dict[key]["is_valid"]:
                    send_email = True
                    validation_synthesis = scan_result_dict[key]["message"]
                    logger.error(validation_synthesis)
                    return (
                        validation_synthesis,
                        send_email,
                        transfer_file,
                        generic_validation_needed,
                    )

            generic_validation_needed = True
            validation_synthesis, send_email = self.fully_functional_validate(
                scan_result_dict=scan_result_dict
            )
            if not validation_synthesis:
                validation_synthesis = "El archivo esta listo para cargar al FTP"
                transfer_file = True
            logger.info(
                "Validacion del impacto del contenido y sintesis del resultado de su escaneo exitosa"
            )
            return (
                validation_synthesis,
                send_email,
                transfer_file,
                generic_validation_needed,
            )

        except Exception as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la validacion del contenido del archivo"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)

            if not first_error_msg:
                first_error_msg = error_msg

            raise PlataformError(raise_msg) from gen_exc

    def fully_functional_validate(self, scan_result_dict: dict):
        try:
            logger.info(
                "Validando el impacto de la informacion de negocio y sintetisando el resultado de su escaneo..."
            )
            validation_synthesis = None
            send_email = False
            for key in [
                "neteo_vs_contraparte",
                "valid_dates",
                "valid_patas",
                "amount_of_swaps_cf",
                "patas_are_complete",
                "ids_swaps_vs_ids_swaps_cf",
            ]:
                if key in scan_result_dict and not scan_result_dict[key]["is_valid"]:
                    send_email = True
                    message = scan_result_dict[key]["message"]
                    validation_synthesis = (
                        f"{validation_synthesis}\n{message}"
                        if validation_synthesis
                        else message
                    )
                    logger.error(message)
            logger.info(
                "Validacion del impacto de la informacion de negocio y sintesis del resultado de su escaneo exitosa"
            )
            return validation_synthesis, send_email

        except Exception as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la validacion de negocio del archivo"
            erro_msg = create_log_msg(raise_msg)
            logger.error(erro_msg)
            if not first_error_msg:
                first_error_msg = erro_msg
            raise PlataformError(raise_msg) from gen_exc

    def fully_validate(self, scan_result_dict):
        try:
            logger.info(
                "Validando todos los aspectos del archivo y sintetisando el resultado de su escaneo..."
            )
            generic_validation_needed = False
            send_email = False
            validation_synthesis, transfer_file = self.fully_meta_validate(
                scan_result_dict=scan_result_dict
            )
            if transfer_file == None:
                (
                    validation_synthesis,
                    send_email,
                    transfer_file,
                    generic_validation_needed,
                ) = self.fully_content_validate(scan_result_dict=scan_result_dict)
            logger.info(
                "Validacion de todos los aspectos del archivo y sintesis del resultado de su escaneo exitosa"
            )
            return (
                validation_synthesis,
                send_email,
                transfer_file,
                generic_validation_needed,
            )
        except (Exception,) as see_exc:
            global first_error_msg
            raise_msg = (
                "Fallo la validacion integral del archivo (metainfo mas contenido)"
            )
            valid_struct = create_log_msg(raise_msg)
            logger.error(valid_struct)
            if not first_error_msg:
                first_error_msg = valid_struct
            raise PlataformError(raise_msg) from see_exc


class FileManager:
    def __init__(self, s3_bucket_name, s3_filepath, datanfs_secret) -> None:
        self.s3_bucket_name = s3_bucket_name
        self.s3_filepath = s3_filepath
        self.datanfs_secret = datanfs_secret
        self.filename = s3_filepath.split("/")[-1]

    def detect_encoding(self, data):
        logger.info("Detectando la codificacion del archivo...")
        encodings = ["utf-8", "latin-1"]
        for encoding in encodings:
            try:
                data.decode(encoding)
                return encoding
            except UnicodeDecodeError:
                continue
        logger.info("Detectaccion de la codificacion del archivo exitosa")
        return None

    def download_from_bucket(self, s3_filepath) -> io.StringIO:
        FILE_SIZE_MAX = 30e6
        global first_error_msg
        raise_msg = f"Fallo la obtencion del insumo {s3_filepath} desde el bucket {self.s3_bucket_name}"
        try:
            logger.info(
                "Descargando archivo de insumo %s del bucket %s...",
                s3_filepath,
                self.s3_bucket_name,
            )
            s3 = boto3.client("s3")
            s3_object = s3.get_object(Bucket=self.s3_bucket_name, Key=s3_filepath)
            file_size = s3_object["ContentLength"]
            if file_size > FILE_SIZE_MAX:
                raise_msg = f"El archivo {s3_filepath} ({file_size}) supera el tamanio maximo aceptado. "
                raise_msg += f"Maximo: {FILE_SIZE_MAX}B"
                raise PlataformError(raise_msg)
            input_file = s3_object["Body"].read()
            encoding = self.detect_encoding(input_file)
            logger.debug("Codificacion: %s", encoding)
            if encoding:
                input_file = input_file.decode(encoding)
            else:
                raise UserError(
                    "NO se reconoce la codificacion del archivo. Deberia ser utf-8 o latin-1"
                )
            input_file = io.StringIO(input_file)
            logger.debug("Contenido del archivo:\n%s", input_file)
            logger.info("Archivo de insumo descargado con exito")
            return input_file
        except FileNotFoundError as file_exc:
            raise_msg += (
                f". El archivo no se encuentra en la ruta especificada: {s3_filepath}"
            )
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

    def download_all_from_bucket(self) -> io.StringIO:
        raise_msg = "Fallo la descarga de los archivos desde el bucket"
        try:
            logger.info("Descargando ambos archivos...")
            s3_swaps_cf_path = None
            s3_swaps_path = None
            if "swaps_cf" in self.s3_filepath:
                logger.info("El archivo recibido de la lambda es swaps_cf")
                s3_swaps_cf_path = self.s3_filepath
                s3_swaps_path = self.s3_filepath.replace("swaps_cf", "swaps")
            else:
                logger.info("El archivo recibido de la lambda es swaps")
                s3_swaps_cf_path = self.s3_filepath.replace("swaps_", "swaps_cf_")
                s3_swaps_path = self.s3_filepath
            swaps_file = self.download_from_bucket(s3_filepath=s3_swaps_path)
            swaps_cf_file = self.download_from_bucket(s3_filepath=s3_swaps_cf_path)
            logger.info("Ambos archivos descargados con exito")
            return swaps_file, swaps_cf_file
        except FileNotFoundError as file_exc:
            raise_msg += f". El archivo no se encuentra en la ruta especificada: {self.s3_filepath}"
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

    def transfer_file_to_ftp(
        self, file_string_io: io.StringIO, nit: str, filename: str
    ):
        global first_error_msg
        try:
            logger.info("Transfiriendo archivo al datanfs...")
            file_string_io.seek(0)
            sftp_secret = {
                "host": self.datanfs_secret["sftp_host"],
                "port": int(self.datanfs_secret["sftp_port"]),
                "username": self.datanfs_secret["sftp_user"],
                "password": self.datanfs_secret["sftp_password"],
            }
            datanfs_path = self.datanfs_secret["cva_input_path"]
            remote_path = os.path.join(datanfs_path, nit)

            with paramiko.Transport(
                (sftp_secret["host"], sftp_secret["port"])
            ) as transport:
                transport.connect(
                    username=sftp_secret["username"], password=sftp_secret["password"]
                )
                sftp = paramiko.SFTPClient.from_transport(transport)
                try:
                    sftp.mkdir(remote_path)
                except IOError:
                    pass
                with sftp.open(os.path.join(remote_path, filename), "w") as file:
                    file.write(file_string_io.getvalue())
            logger.info("Transferencia del archivo al datanfs exitosa")
        except (Exception,) as gen_exc:
            raise_msg = f"Fallo la transferencia del archivo {self.filename} al FTP"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc


class DbManager:
    DB_TIMEOUT = 2
    CVA_CLIENTS_TABLE = "precia_utils_cva_clients"
    PATAS_TABLE = "precia_utils_cva_swap_patas"
    AMOUNT_OF_SWAPS_CF_TABLE = "precia_utils_amount_of_swaps_cf"

    def __init__(self, db_secret) -> None:
        try:
            self.utils_engine = sa.create_engine(
                db_secret["conn_string_sources"] + db_secret["schema_utils"],
                connect_args={"connect_timeout": DbManager.DB_TIMEOUT},
            )
        except (Exception,) as gsc_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion del objeto DbManager"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gsc_exc

    def get_cva_clients(self):
        try:
            logger.info("Consultando lista de clientes de CVA...")
            select_query = sa.sql.text(
                f"""
                SELECT cva_client, nit, email,swaps_type,swaps_cf_type FROM
                {DbManager.CVA_CLIENTS_TABLE}
                """
            )
            with self.utils_engine.connect() as conn:
                cva_clients_df = pd.read_sql(select_query, conn)
            logger.info("Lista de clientes de CVA consultada con exito")
            return cva_clients_df
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la consulta en BD de la informacion de clientes de CVA"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def get_swap_patas(self):
        try:
            logger.info("Consultando combinaciones validas de patas...")
            select_query = sa.sql.text(
                f"""
                SELECT pata1, pata2 FROM {DbManager.PATAS_TABLE}
                """
            )
            with self.utils_engine.connect() as conn:
                patas_df = pd.read_sql(select_query, conn)
            logger.info("Combinaciones validas de patas consultadas con exito")
            return patas_df
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la consulta en BD de las patas validas de swaps"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def get_amount_of_swaps_cf(self, nit):
        try:
            logger.info("Consultando cantidad de flujos esperados...")
            select_query = sa.sql.text(
                f"""
                SELECT id, pata,amount FROM
                {DbManager.AMOUNT_OF_SWAPS_CF_TABLE} WHERE nit = :nit
                """
            )
            query_params = {"nit": nit}
            with self.utils_engine.connect() as conn:
                amount_df = pd.read_sql(select_query, conn, params=query_params)
            logger.info("Consulta de cantidad de flujos esperados exitosa")
            return amount_df
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = (
                "Fallo la consulta en BD de las cantidades de swaps_cf por id-pata"
            )
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    def insert_amount_of_swaps_cf(self, amount_df):
        try:
            logger.info("Insertando cantidades de flujos por id-pata (ids nuevos)...")
            with self.utils_engine.connect() as conn:
                amount_df.to_sql(
                    DbManager.AMOUNT_OF_SWAPS_CF_TABLE,
                    con=conn,
                    if_exists="append",
                    index=False,
                )
            logger.info(
                "Insercion de cantidades de flujos por id-pata (ids nuevos) exitosa"
            )
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = (
                "Fallo la insersion en BD de las cantidades de swaps_cf por id-pata"
            )
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc


class LambdaManager:
    def __init__(
        self,
        lambda_name: str,
        product,
        schema_type,
        csv_path,
        schema_bucket,
        csv_bucket,
    ) -> None:
        self.lambda_name = lambda_name
        self.product = product
        self.schema_type = schema_type
        self.csv_path = csv_path
        if product == "swaps":
            self.csv_path = self.csv_path.replace("swaps_cf", product)
        elif product not in self.csv_path:
            self.csv_path = self.csv_path.replace("swaps", product)
        self.schema_bucket = schema_bucket
        self.csv_bucket = csv_bucket
        self.payload = self.build_payload()
        self.lambda_response = self.launch_lambda(payload=self.payload)

    def launch_lambda(self, payload: dict) -> dict:
        try:
            logger.info("Lanzando ejecucion de lambda %s...", self.lambda_name)
            lambda_client = boto3.client("lambda")
            lambda_response = lambda_client.invoke(
                FunctionName=self.lambda_name,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            lambda_response_decoded = json.loads(
                lambda_response["Payload"].read().decode()
            )
            logger.info(
                "Respuesta de la lambda:\n%s", json.dumps(lambda_response_decoded)
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
        except (Exception,) as ll_exc:
            global first_error_msg
            raise_msg = (
                f"Fallo el lanzamiento de la ejecucion de la lambda: {self.lambda_name}"
            )
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from ll_exc

    def build_payload(self):
        try:
            schema_path = f"cva/{self.product}/{self.schema_type}_{self.product}_schema"
            schema_validition_path = f"{schema_path}.csvs"
            schema_description_path = f"{schema_path}_description.txt"
            payload = {
                "s3CsvPath": self.csv_path,
                "s3SchemaPath": schema_validition_path,
                "s3SchemaDescriptionPath": schema_description_path,
                "s3SchemaBucket": self.schema_bucket,
                "s3CsvBucket": self.csv_bucket,
            }
            logger.info("Evento a enviar a la lambda:\n%s", json.dumps(payload))
            return payload
        except (Exception,) as ll_exc:
            global first_error_msg
            raise_msg = "Fallo la construccion del payload"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from ll_exc


class Main:
    @staticmethod
    def get_params() -> dict:
        try:
            logger.info("Obteniendo parametros del glue job ...")
            params_key = [
                "DB_SECRET",
                "SMTP_SECRET",
                "DATANFS_SECRET",
                "S3_CVA_PATH",  # Viene de la lambda
                "S3_CVA_BUCKET",  # Viene de la lambda
                "S3_SCHEMA_BUCKET",
                "LBD_CSV_VALIDATOR",
            ]
            params = get_params(params_key)
            logger.info("Obtencion de parametros del glue job exitosa")
            return params
        except (Exception,) as gp_exc:
            global first_error_msg
            raise_msg = "Fallo la obtencion de los parametros del job de glue"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gp_exc @ staticmethod

    @staticmethod
    def send_email(body: str, smtp_secret: dict, subject: str, mail_to: str) -> None:
        try:
            smpt_credentials = {
                "server": smtp_secret["smtp_server"],
                "port": smtp_secret["smtp_port"],
                "user": smtp_secret["smtp_user"],
                "password": smtp_secret["smtp_password"],
            }
            mail_from = smtp_secret["mail_from"]
            # Eliminando las tabulaciones de la identacion de python
            body = "\n".join(line.strip() for line in body.splitlines())
            email = ReportEmail(subject, body)
            smtp_connection = email.connect_to_smtp(smpt_credentials)
            message = email.create_mail_base(mail_from, mail_to)
            email.send_email(smtp_connection, message)
        except (Exception,) as see_exc:
            global first_error_msg
            raise_msg = "Fallo la construccion y envio del correo"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from see_exc

    @staticmethod
    def send_validation_email(
        smtp_secret: dict,
        filename: str,
        mail_to: str,
        valid_struct: str = "",
        functional_validation: str = "",
    ) -> None:
        try:
            logger.info("Enviando correo con resultado de validacion...")
            subject = "Valoracion de portafolio CVA: Estructura del archivo"
            sl = "\n"
            body = f"""
                Cordial saludo,

                Identificamos un error en la carga de la informacion que nos esta enviando a traves de la pagina web de Precia (www.precia.co): {filename}
                {f"{sl}Nuestro sistema identifico lo siguiente: {functional_validation}" if functional_validation != "" else ""}
                {f"Nuestro sistema solo permite la siguiente estructura:{sl}{sl}{valid_struct}" if valid_struct != ""  else ""}
                Por consiguiente, solicitamos validar el archivo y volver a cargar la informacion"""
            Main.send_email(
                body=body, smtp_secret=smtp_secret, subject=subject, mail_to=mail_to
            )
            logger.info("Correo de validacion enviado con exito")
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la construccion y envio del correo de validacion"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    @staticmethod
    def manage_validation(
        params_dict: dict,
        all_validation_info: dict,
        filename: str,
        amount_swaps_cf_df: pd.DataFrame,
        db_manager: DbManager,
        today_amount_swaps_cf_df: pd.DataFrame,
        new_swaps_cf_df: pd.DataFrame,
        file_manager: FileManager,
        file_string_io: io.StringIO,
    ):
        try:
            logger.info("Aplicando logica de validacion a %s...", filename)
            validation_result = all_validation_info
            logger.info(
                "Resultado de todas las validaciones (previas y/o funcionales):\n%s",
                json.dumps(validation_result),
            )
            validation_synthesis = all_validation_info["validation_synthesis"]
            validation_synthesis_copy = validation_synthesis
            logger.info("Resumen de validacion funcional: %s", validation_synthesis)
            send_email = all_validation_info["send_email"]
            transfer_file = all_validation_info["transfer_file"]
            generic_validation_needed = all_validation_info["generic_validation_needed"]
            valid_struct = ""
            if (
                not send_email
            ):  # Cuando no hay que enviar correo para validacion funcional
                validation_synthesis = ""
            if (
                generic_validation_needed
            ):  # Cuando es necesario validar la estructura con la lambda java
                logger.info(
                    "Lanzando ejecucion del validador de archivos CSV (validacion no funcional)..."
                )
                lambda_manager = LambdaManager(
                    lambda_name=params_dict["LBD_CSV_VALIDATOR"],
                    product=all_validation_info["metainfo"]["product"],
                    schema_type=all_validation_info["client_dict"]["schema_type"],
                    csv_path=params_dict["S3_CVA_PATH"],
                    schema_bucket=params_dict["S3_SCHEMA_BUCKET"],
                    csv_bucket=params_dict["S3_CVA_BUCKET"],
                )
                valid_struct = lambda_manager.lambda_response["validStruct"]
                struct_is_valid = lambda_manager.lambda_response["validCsv"]
                send_email = not struct_is_valid or send_email 
                if struct_is_valid: # Cuando la estructura del archivo es valida
                    valid_struct = ""
               
                logger.info("Validador de archivos CSV ejecutado con exito")
            if send_email:
                logger.info("Enviando correo al cliente...")
                Main.send_validation_email(
                    valid_struct=valid_struct,
                    smtp_secret=get_secret(params_dict["SMTP_SECRET"]),
                    filename=filename,
                    functional_validation=validation_synthesis,
                    mail_to=all_validation_info["client_dict"]["email"],
                )
                logger.info("Correo al cliente enviado con exito")
            elif transfer_file:
                logger.info("Transfiriendo archivo al datanfs...")
                file_manager.transfer_file_to_ftp(
                    file_string_io=file_string_io,
                    nit=all_validation_info['metainfo']["nit"],
                    filename=filename
                )
                logger.info("Archivo cargado al datanfs con exito")
                is_swaps_cf = all_validation_info["metainfo"]["product"] == "swaps_cf"
                if amount_swaps_cf_df.empty and is_swaps_cf:
                    db_manager.insert_amount_of_swaps_cf(
                        amount_df=today_amount_swaps_cf_df
                    )
                elif (
                    isinstance(new_swaps_cf_df, pd.DataFrame)
                    and not new_swaps_cf_df.empty
                    and is_swaps_cf
                ):
                    db_manager.insert_amount_of_swaps_cf(amount_df=new_swaps_cf_df)
            else:
                raise UserError(validation_synthesis_copy)
            logger.info("Logica de validacion a %s aplicada con exito", filename)
        except (Exception,) as gen_exc:
            global first_error_msg
            raise_msg = "Fallo la adminstracion del archivo (envio de correo, "
            raise_msg += "llamado de lambda validadora de csv y transferencia)"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gen_exc

    @staticmethod
    def main() -> None:
        try:
            logger.info("Ejecutando el main del job de glue...")
            params_dict = Main.get_params()
            file_manager = FileManager(
                s3_bucket_name=params_dict["S3_CVA_BUCKET"],
                s3_filepath=params_dict["S3_CVA_PATH"],
                datanfs_secret=get_secret(params_dict["DATANFS_SECRET"]),
            )
            swaps_file, swaps_cf_file = file_manager.download_all_from_bucket()
            db_manager = DbManager(db_secret=get_secret(params_dict["DB_SECRET"]))
            cva_clients_df = db_manager.get_cva_clients()
            patas_df = db_manager.get_swap_patas()
            nit = params_dict["S3_CVA_PATH"].split("/")[-1].split("_")[0]
            logger.debug("NIT: %s", nit)
            amount_swaps_cf_df = db_manager.get_amount_of_swaps_cf(nit=nit)
            logger.info("Realizando las validaciones previas y funcionales...")
            functional_validator = FunctionalValidator(
                swaps_file=swaps_file,
                swaps_cf_file=swaps_cf_file,
                s3_filepath=params_dict["S3_CVA_PATH"],
                clients_df=cva_clients_df,
                patas_df=patas_df,
                previous_amount_swaps_cf_df=amount_swaps_cf_df,
            )
            Main.manage_validation(
                params_dict=params_dict,
                all_validation_info=functional_validator.all_swaps_info,
                filename=functional_validator.swaps_filename,
                amount_swaps_cf_df=amount_swaps_cf_df,
                db_manager=db_manager,
                today_amount_swaps_cf_df=functional_validator.today_amount_swaps_cf_df,
                new_swaps_cf_df=functional_validator.new_swaps_cf_df,
                file_manager=file_manager,
                file_string_io=swaps_file,
            )
            Main.manage_validation(
                params_dict=params_dict,
                all_validation_info=functional_validator.all_swaps_cf_info,
                filename=functional_validator.swaps_cf_filename,
                amount_swaps_cf_df=amount_swaps_cf_df,
                db_manager=db_manager,
                today_amount_swaps_cf_df=functional_validator.today_amount_swaps_cf_df,
                new_swaps_cf_df=functional_validator.new_swaps_cf_df,
                file_manager=file_manager,
                file_string_io=swaps_cf_file,
            )
            logger.info("Ejecucion del main del job de glue exitosa!")
        except (Exception,) as main_exc:
            global first_error_msg
            main_error_msg = create_log_msg("Fallo la ejecucion del job de glue")
            logger.critical(main_error_msg)
            if not first_error_msg:
                first_error_msg = main_error_msg
            raise PlataformError(first_error_msg) from main_exc


if __name__ == "__main__":
    Main().main()