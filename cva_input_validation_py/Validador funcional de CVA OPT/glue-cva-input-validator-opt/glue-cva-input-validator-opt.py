import boto3, re, io, json, logging, os
import pandas as pd, sqlalchemy as sa, paramiko

from common_library_email_report.ReportEmail import ReportEmail
from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError, UserError

logger = setup_logging(logging.INFO)

first_error_msg = None
empty_file_msg = "El archivo se encuentra vacio"

class FunctionalValidator:
    OPT_HEADER = [
        "ID",
        "TIPO",
        "SUBYACENTE",
        "NOCIONAL",
        "VTO",
        "CV",
        "C_P",
        "STRIKE",
        "GRUPO_NETEO",
        "CONTRAPARTE",
        "MERCADO",
        "ID_DEFAULT",
    ]

    def __init__(
        self, s3_filepath: str, input_file: io.StringIO, clients_df: pd.DataFrame
    ) -> None:
        try:
            self.input_file = input_file
            self.clients_df = clients_df
            self.nit_list = clients_df["nit"].to_list()
            logger.debug("len(self.nit_list): %s", len(self.nit_list))
            self.nit_opc_list = clients_df.loc[
                self.clients_df["opciones_type"] != "NA", "nit"
            ].to_list()
            logger.debug("len(self.nit_opc_list): %s", len(self.nit_opc_list))																	  
            self.s3_filepath = s3_filepath
            self.filename = s3_filepath.split("/")[-1]
            logger.info("filename: %s", self.filename)
            self.metainfo = self.get_metainfo()
            if self.metainfo:
                self.client_dict = self.get_client_info(self.metainfo)
            self.scan_result_dict = self.scan(self.metainfo)
            (
                self.validation_synthesis,
                self.send_email,
                self.transfer_file,
                self.generic_validation_needed,
            ) = self.fully_validate(scan_result_dict=self.scan_result_dict)
        except (Exception,) as file_exc:
            global first_error_msg
            raise_msg = "Fallo la creacion del objeto FunctionalValidator"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from file_exc

    def get_metainfo(
        self,
    ) -> dict:
        """Obtiene la metainformacion del archivo de insumo

        Raises:
            UserError: Cuando la estrutura del nombre del archivo no es la esperada
            UserError: Cuando el tipo de archivo recibido no puede ser procesado
            PlataformError: Cuando falla la obtencion de informacion

        Returns:
            dict:
                nit : informacion del cliente
                product_regex : nobmre del producto
                extension: Extension del archivo: csv 
        """
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
                self.filename,
            )
            metainfo = None

            if match:
                metainfo = {
                    "nit": match.group(1),
                    "product": match.group(2),
                    "valuation_date": match.group(4),
                    "extension": match.group(5),
                }
            logger.debug("Metainfo: %s", json.dumps(metainfo))
            logger.info("Informacion del nombre del insumo obtenida con exito")
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
            if metainfo["extension"] != "csv":
                scan_result_dict["extension"] = {
                    "is_valid": False,
                    "message": f"La extension del archivo es {metainfo['extension']} pero debe ser csv. ",
                }
            else:
                scan_result_dict["extension"] = {
                    "is_valid": True,
                    "message": f"La extension del archivo es {metainfo['extension']}. ",
                }
            if metainfo["nit"] not in self.nit_list:
                scan_result_dict["nit"] = {
                    "is_valid": False,
                    "message": f"El nit {metainfo['nit']} NO se encuentra en la lista de clientes de Precia. ",
                }
            else:
                scan_result_dict["nit"] = {
                    "is_valid": True,
                    "message": f"El nit {metainfo['nit']} se encuentra en la lista de clientes de Precia. ",
                }
            if metainfo["nit"] not in self.nit_opc_list:
                scan_result_dict["is_opc_client"] = {
                    "is_valid": False,
                    "message": f"Actualmente NO se valora el portafolio de opciones para este cliente: {metainfo['nit']}",
                }
            else:
                scan_result_dict["is_opc_client"] = {
                    "is_valid": True,
                    "message": f"Actualmente si se valora el portafolio de opciones para este cliente: {metainfo['nit']}",
                }
            if metainfo["product"] != "opciones":
                scan_result_dict["product"] = {
                    "is_valid": False,
                    "message": f"El producto ({metainfo['product']}) NO se valida en este job de glue (solo valida opciones)",
                }
            else:
                scan_result_dict["product"] = {
                    "is_valid": True,
                    "message": f"El producto ({metainfo['product']}) si se valida en este job de glue",
                }
            return scan_result_dict
        except (Exception,) as file_exc:
            global first_error_msg
            raise_msg = f"Fallo el scaneo de la metainfo del insumo {self.filename}"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from file_exc

    def detect_separator(self, input_file: io.StringIO, scan_result_dict: dict):
        try:
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
                        "message": empty_file_msg,
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
            return separator
        except (Exception,) as file_exc:
            global first_error_msg
            raise_msg = (
                f"Fallo la determinacion del separador del insumo {self.filename}"
            )
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from file_exc

    def has_header(self, input_file, separator):
        try:
            input_file.seek(0)
            first_line = input_file.readline()
            elements = first_line.strip().split(separator)
            if all(elem != "" and not elem.isnumeric() for elem in elements):
                logger.info("El archivo tiene encabezado")
                return True
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
        self, separator: str, scan_result_dict: dict, metainfo: dict
    ) -> pd.DataFrame:
        input_df = None
        global first_error_msg
        raise_msg = f"Fallo la conversion del insumo {self.filename} a dataframe"
        try:
            file_has_header = self.has_header(
                input_file=self.input_file, separator=separator
            )
            self.input_file.seek(0)

            if not file_has_header and metainfo["product"] == "opciones" and metainfo["nit"] == "800138188":
                input_df: pd.DataFrame = pd.read_csv(
                    self.input_file, sep=separator, names=FunctionalValidator.OPT_HEADER
                )
            else:
                input_df: pd.DataFrame = pd.read_csv(self.input_file, sep=separator)
            columns = list(input_df.columns.values)
            if "CONTRAPARTE" in columns  or "GRUPO_NETEO" in columns:
                scan_result_dict["header"] = {
                    "is_valid": True,
                    "message": "El archivo trae encabezado",
                }
            else:
                scan_result_dict["header"] = {
                    "is_valid": False,
                    "message": "El archivo no trae encabezado",
                }     
            scan_result_dict["empty_file"] = {
                "is_valid": True,
                "message": "El archivo NO esta vacio",
            }
            scan_result_dict["file_to_dataframe"] = {
                "is_valid": True,
                "message": "El archivo tiene una estructura convertible en dataframe",
            }
            logger.debug("Dataframe a validar:\n%s", input_df)
            logger.info("Archivo de insumo descargado con exito")
            return scan_result_dict, input_df
        except pd.errors.EmptyDataError:
            raise_msg += "El archivo de insumo esta vacio"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            scan_result_dict["empty_file"] = {
                "is_valid": False,
                "message": empty_file_msg,
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

    def validate_if_date_exists(self, input_df: pd.DataFrame, scan_result_dict: dict):
        data_nan = 0
        try:
            logger.info("Se intenta realizar la validación del la columna VTO")
            input_df['VTO'] = pd.to_datetime(input_df['VTO'], errors='coerce')
            input_df['VTO'] = input_df['VTO'].dt.strftime('%m/%d/%Y')
            data_nan = input_df['VTO'].isna().sum()
            if data_nan !=0:
                data_nan = False
                message = "Algunas fechas no existen o no se encuentran en el formato establecido"
            else: 
                message = "Las fechas cumplen con el formato correcto"
                data_nan = True
            scan_result_dict["validate_date"] = {
                "is_valid": data_nan,
                "message": message,
            }
            logger.debug(scan_result_dict)
            return scan_result_dict
        except (Exception,) as init_exc:
            global first_error_msg
            raise_msg = "Fallo la validacion de la columna VTO"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from init_exc

    def validate_neteo_vs_contraparte(
        self, input_df: pd.DataFrame, scan_result_dict: dict
    ):
        try:
            logger.debug("input_df:\n%s", input_df[["GRUPO_NETEO", "CONTRAPARTE"]])
            duplicates = input_df.groupby("GRUPO_NETEO")["CONTRAPARTE"].nunique().gt(1)
            logger.debug("duplicates:\n%s", duplicates)
            invalid_values = duplicates[duplicates].index.tolist()
            if invalid_values:
                gn_vs_cp_df = input_df.loc[
                    input_df["GRUPO_NETEO"].isin(invalid_values),
                    ["GRUPO_NETEO", "CONTRAPARTE"],
                ]
                message = "Hay valores en 'GRUPO_NETEO' con mas de un valor diferente en 'CONTRAPARTE':\n"
                message += f"\n{gn_vs_cp_df.to_string(index=False)}\n"
                scan_result_dict["neteo_vs_contraparte"] = {
                    "is_valid": False,
                    "message": message,
                }
            else:
                scan_result_dict["neteo_vs_contraparte"] = {
                    "is_valid": True,
                    "message": "No hay valores en 'GRUPO_NETEO' con mas de un valor diferente en 'CONTRAPARTE'.",
                }
            return scan_result_dict
        except (Exception,) as init_exc:
            global first_error_msg
            raise_msg = "Fallo la validacion de la columna GRUPO_NETEO vs CONTRAPARTE"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from init_exc

    def scan(self, metainfo):
        try:
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
															  
                        input_file=self.input_file, scan_result_dict=scan_result_dict
                    )
                    if self.separator:
                        scan_result_dict, input_df = self.convert_to_dataframe(
                            scan_result_dict=scan_result_dict,
                            separator=self.separator,
                            metainfo=metainfo,
                        )
                if (
                    isinstance(input_df, pd.DataFrame)
                    and not input_df.empty
                    and "GRUPO_NETEO" in input_df
                    and "CONTRAPARTE" in input_df
                ):
                    self.validate_if_date_exists(input_df, scan_result_dict)														
                    logger.debug("Se puede validar columna de Neteo")
                    scan_result_dict = self.validate_neteo_vs_contraparte(
                        input_df, scan_result_dict
                    )
            else:
                scan_result_dict["name_struct"] = {
                    "is_valid": False,
                    "message": "La estructura del nombre del archivo no es valida",
                }
            return scan_result_dict
        except (Exception,) as init_exc:
            global first_error_msg
            raise_msg = f"Fallo el escaneo del archivo y de su nombre: {self.filename}"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from init_exc

    def fully_meta_validate(self, scan_result_dict):
        try:
            validation_synthesis = ""
            transfer_file = False
            if not scan_result_dict["name_struct"]["is_valid"]:
                validation_synthesis = scan_result_dict["name_struct"]["message"]
                logger.error(validation_synthesis)
                return validation_synthesis, transfer_file
            if not scan_result_dict["product"]["is_valid"]:
                validation_synthesis = scan_result_dict["product"]["message"]
                logger.error(validation_synthesis)
                return validation_synthesis, transfer_file
            if not (
                scan_result_dict["is_opc_client"]["is_valid"]
                and scan_result_dict["extension"]["is_valid"]
            ):
                if not scan_result_dict["is_opc_client"]["is_valid"]:
                    validation_synthesis = scan_result_dict["is_opc_client"]["message"]
                if not scan_result_dict["extension"]["is_valid"]:
                    validation_synthesis += scan_result_dict["extension"]["message"]
                logger.error(validation_synthesis)
                return validation_synthesis, transfer_file
            transfer_file = None
            validation_synthesis = "La metainformacion es correcta"
            return validation_synthesis, transfer_file
        except (Exception,) as see_exc:
            global first_error_msg
            raise_msg = "Fallo la validacion de la metainformacion"
            valid_struct = create_log_msg(raise_msg)
            logger.error(valid_struct)
            if not first_error_msg:
                first_error_msg = valid_struct
            raise PlataformError(raise_msg) from see_exc

    def fully_content_validate(self, scan_result_dict):
        try:
            generic_validation_needed = False
            validation_synthesis = None
            transfer_file = False
            send_email = False
            if (
                "separator" in scan_result_dict
                and not scan_result_dict["separator"]["is_valid"]
            ):
                send_email = True
                validation_synthesis = scan_result_dict["separator"]["message"]
                logger.error(validation_synthesis)
                return (
                    validation_synthesis,
                    send_email,
                    transfer_file,
                    generic_validation_needed,
                )
            if (
                "empty_file" in scan_result_dict
                and not scan_result_dict["empty_file"]["is_valid"]
            ):
                send_email = True
                validation_synthesis = scan_result_dict["empty_file"]["message"]
                logger.error(validation_synthesis)
                return (
                    validation_synthesis,
                    send_email,
                    transfer_file,
                    generic_validation_needed,
                )
            # Para las dos siguientes validaciones es necesario realizar la
            # validacion generica en la lambda csv validator para obtener la
            # estructura valida del archivo
            generic_validation_needed = True
            if (
                "file_to_dataframe" in scan_result_dict
                and not scan_result_dict["file_to_dataframe"]["is_valid"]
            ):
                send_email = True
                validation_synthesis = scan_result_dict["file_to_dataframe"]["message"]
                logger.error(validation_synthesis)
                return (
                    validation_synthesis,
                    send_email,
                    transfer_file,
                    generic_validation_needed,
                )
            if (
                "neteo_vs_contraparte" in scan_result_dict
                and not scan_result_dict["neteo_vs_contraparte"]["is_valid"]
            ):
                send_email = True
                validation_synthesis = scan_result_dict["neteo_vs_contraparte"][
                    "message"
                ]
                logger.error(validation_synthesis)
                return (
                    validation_synthesis,
                    send_email,
                    transfer_file,
                    generic_validation_needed,
                )
            if (
                "validate_date" in scan_result_dict
                and not scan_result_dict["validate_date"]["is_valid"]
            ):
                send_email = True
                validation_synthesis = scan_result_dict["validate_date"][
                    "message"
                ]
                logger.error(validation_synthesis)
                return (
                    validation_synthesis,
                    send_email,
                    transfer_file,
                    generic_validation_needed,
                )
            if (
                "header" in scan_result_dict
                and not scan_result_dict["header"]["is_valid"]
            ):
                send_email = True
                validation_synthesis = scan_result_dict["header"][
                    "message"
                ]
                logger.error(validation_synthesis)
                return (
                    validation_synthesis,
                    send_email,
                    transfer_file,
                    generic_validation_needed,
                )
            validation_synthesis = "El archivo esta listo para cargar al FTP"
            transfer_file = True
            return (
                validation_synthesis,
                send_email,
                transfer_file,
                generic_validation_needed,
            )
        except (Exception,) as see_exc:
            global first_error_msg
            raise_msg = "Fallo la validacion del contenido del archivo"
            valid_struct = create_log_msg(raise_msg)
            logger.error(valid_struct)
            if not first_error_msg:
                first_error_msg = valid_struct
            raise PlataformError(raise_msg) from see_exc

    def fully_validate(self, scan_result_dict):
        try:
            generic_validation_needed = False
            validation_synthesis, transfer_file = self.fully_meta_validate(
                scan_result_dict=scan_result_dict
            )
            send_email = False
            if transfer_file == None:
                (
                    validation_synthesis,
                    send_email,
                    transfer_file,
                    generic_validation_needed,
                ) = self.fully_content_validate(scan_result_dict=scan_result_dict)
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
        encodings = ["utf-8", "latin-1"]
        for encoding in encodings:
            try:
                data.decode(encoding)
                return encoding
            except UnicodeDecodeError:
                continue
        return None

    def download_from_bucket(self) -> io.StringIO:
        FILE_SIZE_MAX = 30e6
        global first_error_msg
        raise_msg = f"Fallo la obtencion del insumo {self.s3_filepath} desde el bucket {self.s3_bucket_name}"
        try:
            logger.info(
                "Descargando archivo de insumo del bucket %s...",
                self.s3_bucket_name,
            )
            s3 = boto3.client("s3")
            s3_object = s3.get_object(Bucket=self.s3_bucket_name, Key=self.s3_filepath)
            file_size = s3_object["ContentLength"]
            if file_size > FILE_SIZE_MAX:
                raise_msg = f"El archivo {self.s3_filepath} ({file_size}) supera el tamanio maximo aceptado. "
                raise_msg += f"Maximo: {FILE_SIZE_MAX}B"
                raise PlataformError(raise_msg)
            input_file = s3_object["Body"].read()
            encoding = self.detect_encoding(input_file)
            if encoding:
                input_file = input_file.decode(encoding)
            else:
                raise UserError(
                    "No se reconoce la codificacion del archivo. Deberia ser utf-8 o latin-1"
                )
            input_file = io.StringIO(input_file)
            logger.debug("Contenido del archivo:\n%s", input_file)
            logger.info("Archivo de insumo descargado con exito")
            return input_file
        except FileNotFoundError as file_exc:
            raise_msg += f". El archivo no se encuentra en la ruta especificada: {self.s3_filepath}"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from file_exc
        except (Exception,) as dfb_exc:
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from dfb_exc

    def transfer_file_to_ftp(self, file_string_io: io.StringIO, nit: str):
        global first_error_msg
        try:
            file_string_io.seek(0)
            sftp_secret = {
                "host": self.datanfs_secret["sftp_host"],
                "port": int(self.datanfs_secret["sftp_port"]),
                "username": self.datanfs_secret["sftp_user"],
                "password": self.datanfs_secret["sftp_password"],
            }
            datanfs_path = self.datanfs_secret["cva_input_path"]
            remote_path = os.path.join(datanfs_path, nit)
            
            
									
            with paramiko.Transport((sftp_secret["host"], sftp_secret["port"])) as transport:
                transport.connect(username=sftp_secret["username"], password=sftp_secret["password"])
                sftp = paramiko.SFTPClient.from_transport(transport)
                try:
                    sftp.mkdir(remote_path)
                except IOError:
                    pass
                with sftp.open(
                    os.path.join(remote_path, self.filename), "w"
                ) as file:
                    file.write(file_string_io.getvalue())
        except (Exception,) as catf_exc:
            raise_msg = f"Fallo la transferencia del archivo {self.filename} al FTP"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from catf_exc


class DbManager:
    DB_TIMEOUT = 2
    CVA_CLIENTS_TABLE = "precia_utils_cva_clients"

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
            select_query = sa.sql.text(
                f"""
                SELECT cva_client, nit,
                email,opciones_type,swaps_cf_type FROM
                {DbManager.CVA_CLIENTS_TABLE}
                """
            )
            with self.utils_engine.connect() as conn:
                cva_clients_df = pd.read_sql(select_query, conn)
            logger.debug("Clientes CVA:\n%s", cva_clients_df)
            return cva_clients_df
        except (Exception,) as gsc_exc:
            global first_error_msg
            raise_msg = "Fallo la consulta en BD de la informacion de clientes de CVA"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from gsc_exc


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
            logger.debug("smtp_secret:\n%s", smtp_secret)
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
        validation_synthesis: str = ""
    ) -> None:
        try:
            logger.info("Enviando correo con resultado de validacion...")
            subject = "Valoracion de portafolio CVA: Estructura del archivo"
            sl = "\n"
            body = f"""
                Cordial saludo,

                Identificamos un error en la carga de la informacion que nos esta enviando a traves de la pagina web de Precia (www.precia.co): {filename}
                {f"{sl}Nuestro sistema identifico lo siguiente: {validation_synthesis}" if validation_synthesis != "" else ""}
                
                {f"Nuestro sistema solo permite la siguiente estructura, por favor valide el nombre y cantidad de las columnas y los tipos de datos:{sl}{sl}{valid_struct}" if valid_struct != ""  else ""}
				
                Por consiguiente, solicitamos validar el archivo y volver a cargar la informacion"""
            Main.send_email(
                body=body, smtp_secret=smtp_secret, subject=subject, mail_to=mail_to
            )
            logger.info("Correo de validacion enviado con exito")
        except (Exception,) as see_exc:
            global first_error_msg
            raise_msg = "Fallo la construccion y envio del correo de validacion"
            error_msg = create_log_msg(raise_msg)
            logger.error(error_msg)
            if not first_error_msg:
                first_error_msg = error_msg
            raise PlataformError(raise_msg) from see_exc

    @staticmethod
    def main() -> None:
        try:
            logger.info("Ejecutando el main del job de glue...")
            params_dict = Main.get_params()
            logger.debug("Parametros obtenidos del Glue:%s", params_dict)
									   
            file_manager = FileManager(
                s3_bucket_name=params_dict["S3_CVA_BUCKET"],
                s3_filepath=params_dict["S3_CVA_PATH"],

                datanfs_secret=get_secret(params_dict["DATANFS_SECRET"]),
            )
            file_string_io = file_manager.download_from_bucket()
														   
            db_manager = DbManager(db_secret=get_secret(params_dict["DB_SECRET"]))
            cva_clients_df = db_manager.get_cva_clients()
            logger.info("Realizando las validaciones previas y funcionales...")
            functional_validator = FunctionalValidator(
                input_file=file_string_io,
                s3_filepath=params_dict["S3_CVA_PATH"],
                clients_df=cva_clients_df,
            )
            validation_result = functional_validator.scan_result_dict
            logger.info(
                "Resultado de todas las validaciones (previas y/o funcionales):\n%s",
                json.dumps(validation_result),
            )
															   
            validation_synthesis = functional_validator.validation_synthesis
            validation_synthesis_copy = validation_synthesis
            logger.info("Resumen de validacion funcional: %s", validation_synthesis)
            send_email = functional_validator.send_email
            transfer_file = functional_validator.transfer_file
            generic_validation_needed = functional_validator.generic_validation_needed
            valid_struct = ""
            if not send_email:
                validation_synthesis = ""
            if generic_validation_needed:
                logger.info(
                    "Lanzando ejecucion del validador de archivos CSV (validacion no funcional)..."
                )
                lambda_manager = LambdaManager(
                    lambda_name=params_dict["LBD_CSV_VALIDATOR"],
                    product=functional_validator.metainfo["product"],
                    schema_type=functional_validator.client_dict["schema_type"],
                    csv_path=params_dict["S3_CVA_PATH"],
                    schema_bucket=params_dict["S3_SCHEMA_BUCKET"],
                    csv_bucket=params_dict["S3_CVA_BUCKET"],
                )										   
			 
                valid_struct = lambda_manager.lambda_response["validStruct"]
                struct_is_valid = lambda_manager.lambda_response["validCsv"]
                generic_validation_needed = lambda_manager.lambda_response["body"]
                send_email = not struct_is_valid or send_email
                if struct_is_valid:
                    valid_struct = ""
				   
                logger.info("Validador de archivos CSV ejecutado con exito")
            if send_email:
                logger.info("Enviando correo al cliente...")
                Main.send_validation_email(
                    valid_struct=valid_struct,
                    smtp_secret=get_secret(params_dict["SMTP_SECRET"]),
                    filename=functional_validator.filename,
                    validation_synthesis=validation_synthesis,
                    mail_to=functional_validator.client_dict["email"]
				)
                logger.info("Correo al cliente enviado con exito")
            elif transfer_file:
                logger.info("Transfiriendo archivo al datanfs...")
										   
                file_manager.transfer_file_to_ftp(
                    file_string_io=file_string_io,
                    nit=functional_validator.metainfo["nit"],
                )
                logger.info("Archivo cargado al datanfs con exito")
            else:
                raise UserError(validation_synthesis_copy)
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
								   
								
