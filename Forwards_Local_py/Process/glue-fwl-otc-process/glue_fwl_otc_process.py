import sys
import logging

import numpy as np
import pandas as pd
import sqlalchemy as sa

from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_exceptions import PlataformError
from precia_utils import precia_sftp

logger = setup_logging(logging.INFO)


class DataBrokers():
    """Representa los datos a consultar e insertar a la
    las bases de datos
    Attributes:
    -----------
    url_connection: str 
        Es la url de conexión para conectarse a la base de datos
    """
    def __init__(self, url_connection) -> None:
        self.url_connection = url_connection
        self.engine = sa.create_engine(self.url_connection)
    
    
    def get_data_brokers(self, valuation_date, table_src_brokers):
        """Trae la información procesada que llego de los brokers
        y luego cierra la conexión a bd
        Parameters:
        ----------
        valuation_date: str
            Fecha de valoración
        table_src_brokers: str
            Tabla de base de datos a consultar la información de los
            brokers
            
        Return:
        -------
        DataFrame
            Contiene los datos proporcionados por los brokers que pasaron por 
            las etls
        """
        query_fwd_local_brokers = sa.sql.text(f"""SELECT broker, tenor, days as dias, mid_fwd as mid,
            bid, ask, valuation_date FROM {table_src_brokers}
            WHERE status_info = :status_info
            AND valuation_date = :valuation_date"""
        )
        query_params = {"status_info": 1, "valuation_date": valuation_date}
                                    
        try:
            connection = self.engine.connect()
        except(Exception, ):
            logger.error(create_log_msg("Falló la conexión a la base de datos"))
            raise PlataformError("Falló la conexión a base de datos")
        try:
            df_brokers_data = pd.read_sql(query_fwd_local_brokers, connection, params=query_params)
            df_brokers_data["dias"] = df_brokers_data['dias'].astype(int)
            logger.debug(df_brokers_data)
            logger.info("Se obtuvo la información correctamente")
            return df_brokers_data
        except(Exception, ):
            logger.error(create_log_msg("Falló la extracción de información de la base de datos"))
            raise PlataformError("Hubo un error en la exrtación de la base de datos")
        finally:
            connection.close()
    
    
    def insert_data(self, df_fwd, name_table):
        """Inserta la información tratada por la ETL a la base de datos

        Parameters:
        ----------
        df_fwd: DataFrame, required
            Contiene los datos del archivo que se genero
            
        name_table: str, required
            Nombre de la tabla para insertar la información
        """
        try:
            connection = self.engine.connect()
            df_fwd.to_sql(name_table, con=connection, if_exists='append', index=False)
        except(Exception, ):
            logger.error(create_log_msg("Falló la inserción de la información en la base de datos"))
            raise PlataformError("Hubo un error en la inserción de información")
        finally:
            connection.close()
    
    
    def disable_previous_info(self, df_fwd, table_db, valuation_date_db):
        """Actualiza el status de la información se encuentra en la base de datos
        Parameters:
        -----------
        df_fwd: DataFrame, required
            Contiene los datos a insertar a la base de datos
        
        table_db: str, required
            Nombre de la tabla en base de datos
        
        valuation_date_str: str
            Fecha de valoración
        """
        logger.debug(df_fwd)
        try:
            update_query = sa.sql.text(f"""UPDATE {table_db}
            SET status_info= :status_info WHERE 
            valuation_date = :valuation_date
            """)
            query_params = {
                'status_info': 0,
                'valuation_date': valuation_date_db
            }
            connection = self.engine.connect()
            connection.execute(update_query,query_params)
            connection.close()
            logger.info("Se a actualizado el estado de la data en base de datos")
        except (Exception,) as fpc_exc:
            raise_msg = "Fallo la conexion a la BD"
            logger.error(create_log_msg(raise_msg))
            if connection != None:
                connection.close()
            raise PlataformError() from fpc_exc
        


class LocalNDF():
    """Clase que contiene el metodo de formacion de la muestra de puntos forward USDCOP y los metodos de extracción de 
    información de lo brokers
    Args:
        self.brokers_info (pd.DataFrame): Informacion sobre puntos forwards USDCOP, enviada por los brokers
        self.tenors (list): Tenores estandar de la muestra forward
        self.logger (RootLogger root) = logger
    """
    def __init__(self, brokers_info, tenors, logger):
        self.brokers_info = brokers_info
        self.tenors = tenors
        self.logger = logger
    
    def info_gfi(self):
        """
        Extrae la información suministrada por el broker GFI 

        Returns:
            fwd_gfi  (pd.DataFrame): Puntos fwd del broker gfi con dias, mid, bid y ask
        """
        try:
            self.logger.info(create_log_msg('Inicio creacion archivo GFI'))
            is_gfi=np.in1d("GFI",pd.unique(self.brokers_info["broker"]))
            is_null=self.brokers_info.iloc[np.where(self.brokers_info["broker"]=="GFI")][["mid","bid","ask"]].isnull().values.any()
            if is_gfi and is_null :
                self.logger.info(create_log_msg('Se tienen campos nulos en la informacioón del broker GFI'))
                fwd_gfi=self.brokers_info.iloc[np.where(self.brokers_info["broker"]=="GFI")][["dias","mid","bid","ask"]]
            if is_gfi:
                fwd_gfi=self.brokers_info.iloc[np.where(self.brokers_info["broker"]=="GFI")][["dias","mid","bid","ask"]]
            else:
                self.logger.info(create_log_msg('No se tiene informacioón del broker GFI, el archivo saldra vacio'))
                fwd_gfi=pd.DataFrame(columns=["dias","mid","bid","ask"])
            self.logger.info(create_log_msg('Fin creacion archivo GFI'))
            return(fwd_gfi)
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la creación del archivo GFI'))
            raise PlataformError("Hubo un error en la creación del archivo GFI")
            
    def info_icap(self):
        """
        Extrae la información suministrada por el broker ICAP

        Returns
        -------
            fwd_icap  (pd.DataFrame): Puntos fwd del broker icap con dias, mid, bid y ask

        """
        try:
            self.logger.info(create_log_msg('Inicio creacion archivo ICAP'))
            is_icap=np.in1d("ICAP",pd.unique(self.brokers_info["broker"]))
            is_null=self.brokers_info.iloc[np.where(self.brokers_info["broker"]=="ICAP")][["mid","bid","ask"]].isnull().values.any()
            if is_icap and is_null :
                self.logger.info(create_log_msg('Se tienen campos nulos en la informacioón del broker ICAP'))
                fwd_icap=self.brokers_info.iloc[np.where(self.brokers_info["broker"]=="ICAP")][["dias","mid","bid","ask"]]
            elif is_icap:
                fwd_icap=self.brokers_info.iloc[np.where(self.brokers_info["broker"]=="ICAP")][["dias","mid","bid","ask"]]
            else:
                self.logger.info(create_log_msg('No se tiene informacioón del broker ICAP, el archivo saldra vacio'))
                fwd_icap=pd.DataFrame(columns=["dias","mid","bid","ask"])
            self.logger.info(create_log_msg('Fin creacion archivo ICAP'))
            return(fwd_icap)
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la creación del archivo ICAP'))
            raise PlataformError("Hubo un error en la creación del archivo ICAP")
            
    def info_tradition(self):
        """
        Extrae la información suministrada por el broker Tradition

        Returns
        -------
            fwd_tradition  (pd.DataFrame): Puntos fwd del broker tradition con dias, mid, bid y ask.

        """
        try:
            self.logger.info(create_log_msg('Inicio creacion archivo Tradition'))
            is_tradition=np.in1d("TRADITION",pd.unique(self.brokers_info["broker"]))
            is_null=self.brokers_info.iloc[np.where(self.brokers_info["broker"]=="TRADITION")][["mid","bid","ask"]].isnull().values.any()
            if is_tradition and is_null :
                self.logger.info(create_log_msg('Se tienen campos nulos en la informacioón del broker Tradition'))
                fwd_tradition=self.brokers_info.iloc[np.where(self.brokers_info["broker"]=="TRADITION")][["dias","mid","bid","ask"]]
            elif is_tradition:
                fwd_tradition=self.brokers_info.iloc[np.where(self.brokers_info["broker"]=="TRADITION")][["dias","mid","bid","ask"]]
            else:
                self.logger.info(create_log_msg('No se tiene informacioón del broker Tradition, el archivo saldra vacio'))
                fwd_tradition=pd.DataFrame(columns=["dias","mid","bid","ask"])
            self.logger.info(create_log_msg('Fin creacion archivo Tradition'))
            return(fwd_tradition)
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la creación del archivo Tradition'))
            raise PlataformError("Hubo un error en la creación del archivo Tradition")
            
    def local_sample(self):
        """
        Creacion de la muestra de puntos forwards para cada tenor, los cuales corresponden al precio mid calculado apartir del mayor precio de compra 
        y el menor precio de venta asociado a cada tenor conforme a los puntos recibidos de los brokers
        
        Returns:
            fwd_today (pd.DataFrame): Muestra Fwd con precio mid, bid y ask
            
        """
        try:
            self.logger.info(create_log_msg('Inicio creacion muestra forward USDCOP'))
            if self.brokers_info.empty:
                self.logger.info(create_log_msg('El dataframe con la información de los brokers viene vacio'))
            
            self.brokers_info['ask'] = np.where((self.brokers_info["tenor"]=="ON") & (self.brokers_info['ask'] == 0), 99999, self.brokers_info['ask'])
            self.brokers_info['bid'] = np.where((self.brokers_info["tenor"]=="ON") & (self.brokers_info['bid'] == 0), -99999, self.brokers_info['bid'])
            
            available_brokers=', '.join(map(str, pd.unique(self.brokers_info["broker"])))
            fwd_today = pd.DataFrame({'dias':pd.unique(self.brokers_info['dias'])})
            for i in ['mid', 'bid', 'ask']:
                fwd_today [i] = np.nan          
            fwd_today["bid"]=self.brokers_info[["tenor","bid"]].groupby(['tenor'],sort=False).max().bid.values
            fwd_today["ask"]=self.brokers_info[["tenor","ask"]].groupby(['tenor'],sort=False).min().ask.values
            fwd_today["mid"]=(fwd_today["bid"]+fwd_today["ask"])/2
            self.logger.info(create_log_msg(f'Se utilizó la información de los brokers: {available_brokers}'))
            self.logger.info(create_log_msg('Fin creación muestra forward USDCOP'))
            return fwd_today
        except(Exception,):
            self.logger.error(create_log_msg('Se genero un error en la creación de la muestra forward USDCOP'))
            raise PlataformError("Hubo un error en la creación de la muestra forward USDCOP")
           
            
class FilesFwdLocal():

    def load_files_to_sftp(self, route_sftp, sftp, df_file, file_name):  
        """Genera y carga los archivos en el SFTP

        Parameters:
        -----------
        route_sftp: str, required
            Contiene la ruta en donde se guardan los archivos en el SFTP
        sftp: SFTPClient, required 
            Es la conexión al SFTP
        df_file: Dataframe, required
            Datos que van en el archivo
        file_name: str, required
            Nombre del archivo a generar
        """
        logger.info('Comenzando a generar el archivo %s en el sftp', file_name)
        df_file.loc[:, ("mid", "bid", "ask")] = df_file.loc[:, ("mid", "bid", "ask")].round(3)
        try:
            with sftp.open(route_sftp + file_name, "w") as f:
                try:
                    f.write(df_file.to_csv(index=False, sep=','))
                except (Exception,):
                    logger.error(create_log_msg("Fallo la escritura del archivo"))
                
        except (Exception,):
            logger.error(create_log_msg(f"No se pudo generar el archivo en el SFTP: {file_name}"))
        
        logger.info('El archivo se creo correctamente: %s', file_name)


def transform_df(name_column, value, df_fwd, position):
    """Añade las columnas que se requieran para la tabla de 
    base de datos y renombra las columnas

        Parameters:
        -----------
        df_fwd: DataFram, required
            Datos ya transformados del archivo
        name_column: str, required
            Nombre de la columna a añadir
        value: 
            Valor que va contener la columna por cada fila
        position: int
            Número de la posición de la columna a insertar

        Return
        ------
        Dataframe
            Datos de archivo con la nueva columna insertada
    """

    try:
        df_fwd.insert(loc=position,column=name_column,value=value)
        df_fwd = df_fwd.rename(columns={"mid":"mid_fwd"})
        df_fwd = df_fwd.rename(columns={"dias":"days"})
        logger.info("Se transformo el DataFrame")
        return df_fwd
        
    except(Exception, ):
        logger.error(create_log_msg("Fallo en añadir las columnas en el DataFrame"))
        raise PlataformError("Hubo un error añadiendo las nuevas columnas")
            
            
def run():
    """Orquestación para el calculo de la muestra y generación de archivos
    para Forwards Local
    """
    params_key = [
        "SECRET_FWD",
        "VALUATION_DATE",
        "TENOR",
        #"ROUTE_SFTP_MUESTRA",
        "FILE_USDCOP",
        "FILE_ICAP",
        "FILE_GFI",
        "FILE_TRADITION",
        "AUTHOR",
        "TABLE_PRC_BROKER",
        "TABLE_PRC_LOCAL",
        "TABLE_SRC"
        #"ROUTE_SFTP_BROKERS"
        ]

    #Trae la información de los brokers que se encuentra en base de datos
    params_glue = get_params(params_key)
    valuation_date = params_glue["VALUATION_DATE"]
    valuation_date_db = params_glue["VALUATION_DATE"]
    secret_fwd = params_glue["SECRET_FWD"]
    secret_connection = get_secret(secret_fwd)
    url_connection = secret_connection["conn_string_sources"]
    table_src_brokers = params_glue["TABLE_SRC"]
    db = DataBrokers(url_connection)
    data_df = db.get_data_brokers(valuation_date, table_src_brokers)
    
    # Calculo de la muestra
    logger.info("DataFrame para el cálculo de la muestra")
    logger.info(data_df)
    tenor = params_glue["TENOR"].split(',')
    muestra = LocalNDF(data_df, tenor, logger)
    df_local = muestra.local_sample()
    df_tradition = muestra.info_tradition()
    df_icap = muestra.info_icap()
    df_gfi = muestra.info_gfi()
    
    # Formateo de los nombres de los archivos
    valuation_date = valuation_date.replace('-','')
    route_sftp_muestra = secret_connection["sample_sftp_path"]
    route_sftp_brokers = secret_connection["brokers_sftp_path"]
    file_usdcop = params_glue["FILE_USDCOP"]
    file_icap = params_glue["FILE_ICAP"].replace('YYYYMMDD',valuation_date)
    file_gfi = params_glue["FILE_GFI"].replace('YYYYMMDD',valuation_date)
    file_tradition = params_glue["FILE_TRADITION"].replace('YYYYMMDD',valuation_date)
    file_fwd = FilesFwdLocal()
    
    # Generación de los archivos
    try:
        logger.info("Trae el valor de los secretos para la conexión al SFTP")
        data_connection = {
            "host": secret_connection["sftp_host"],
            "port": secret_connection["sftp_port"],
            "username": secret_connection["sftp_user"],
            "password": secret_connection["sftp_password"]
        }
        client= precia_sftp.connect_to_sftp(data_connection)
        sftp_connect = client.open_sftp()
        file_fwd.load_files_to_sftp(route_sftp_muestra, sftp_connect, df_local, file_usdcop)
        file_fwd.load_files_to_sftp(route_sftp_brokers, sftp_connect, df_icap, file_icap)
        file_fwd.load_files_to_sftp(route_sftp_brokers, sftp_connect, df_gfi, file_gfi)
        file_fwd.load_files_to_sftp(route_sftp_brokers, sftp_connect, df_tradition, file_tradition)
        sftp_connect.close()
        client.close()
    except (Exception, ):
        logger.error(create_log_msg("Fallo la obtención del secreto y la conexión SFTP"))
    
    
    # Añade las columnas necesarias a los dataframes
    author = params_glue["AUTHOR"]
    df_icap_transform = transform_df("broker", "ICAP", df_icap, 4)
    df_icap_transform = transform_df("valuation_date", valuation_date, df_icap_transform, 5)
    df_icap_transform = transform_df("author", author, df_icap_transform, 6)
    
    df_gfi_transform = transform_df("broker", "GFI", df_gfi, 0)
    df_gfi_transform = transform_df("valuation_date", valuation_date, df_gfi_transform, 1)
    df_gfi_transform = transform_df("author", author, df_gfi_transform, 2)
    
    df_tradition_transform = transform_df("broker", "TRADITION", df_tradition, 4)
    df_tradition_transform = transform_df("valuation_date", valuation_date, df_tradition_transform, 5)
    df_tradition_transform = transform_df("author", author, df_tradition_transform, 6)
    logger.info(df_tradition_transform)
    
    df_local_transform = transform_df("valuation_date", valuation_date, df_local, 4)
    df_local_transform = transform_df("author", author, df_local_transform, 5)
    
    # Inserta la información del calculo de la muestra
    table_prc_broker = params_glue["TABLE_PRC_BROKER"]
    url_connection_prc = secret_connection["conn_string_process"]
    db_prc = DataBrokers(url_connection_prc)
    logger.info("Funciones de db")
    logger.info(table_prc_broker)
    db_prc.disable_previous_info(df_tradition_transform, table_prc_broker, valuation_date_db)
    db_prc.insert_data(df_icap_transform, table_prc_broker)
    db_prc.insert_data(df_gfi_transform, table_prc_broker)
    logger.info(df_tradition_transform)
    db_prc.insert_data(df_tradition_transform, table_prc_broker)
    
    table_prc_local = params_glue["TABLE_PRC_LOCAL"]
    db_prc.disable_previous_info(df_local_transform, table_prc_local, valuation_date_db)
    db_prc.insert_data(df_local_transform, table_prc_local)
    
if __name__ == "__main__":
    run()