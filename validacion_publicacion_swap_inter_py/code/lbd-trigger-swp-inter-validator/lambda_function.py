import json
import os
import boto3
import logging
import ast

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def add_suffix(element, date):
    return f"{element}_{date}.txt"


def transform_dictionary(dictionary, date):
    for key, value in dictionary.items():
        if isinstance(value, list):
            dictionary[key] = [add_suffix(element, date) for element in value]
        elif isinstance(value, dict):
            transform_dictionary(value, date)


def lambda_handler(event, context):
    ssm_client = boto3.client('ssm')
    s3 = boto3.client("s3")
    client = boto3.client("glue")
    
    glue_job_name_validator = os.environ["JOB_NAME_VALIDATOR"]
    glue_job_name_publish = os.environ["JOB_NAME_PUBLISHER"]
    
    ps_read = ssm_client.get_parameter(
            Name=os.environ["PARAMETER_SWAP_INTER"],
            WithDecryption=False
        )
    
    json_files = json.loads(ps_read['Parameter']['Value'])
    
    file_name = event["Records"][0]["s3"]["object"]["key"].split("/")[-1]
    file_name_without_extension, _ = os.path.splitext(file_name)
    parts = file_name_without_extension.split("_")
    swap_name = '_'.join(parts[:-1])
    swap_date = "".join(parts[-1])
    
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    prefix = "Publish/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = [obj["Key"][len(prefix) :] for obj in response.get("Contents", [])]
    if swap_name in json_files["PUBLISH"]:
        response = client.start_job_run(
            JobName=glue_job_name_publish,
            Arguments={"--FILE_LIST": add_suffix(swap_name, swap_date)},
        )
        job_run_id = response["JobRunId"]
        logger.info("Se lanzo el Glue - glue-otc-swp-inter-publish: %s.", job_run_id)
        return {"statusCode": 200, "body": json.dumps("Ejecutado exitosamente")}

    for key, values in json_files["VALIDATOR"].items():
        if swap_name in values:
            transform_dictionary(json_files, swap_date)
            big_group = set(files)
            small_group = set(json_files["VALIDATOR"][key])
            logger.info(f"{swap_name} encontrado en la clave {key}.")
            if small_group.issubset(big_group):
                result_chain = ",".join(small_group)
                response = client.start_job_run(
                    JobName=glue_job_name_validator,
                    Arguments={"--FILE_LIST": result_chain},
                )
                job_run_id = response["JobRunId"]
                logger.info(
                    "Se lanzo el Glue - glue-otc-swp-inter-validator: %s.", job_run_id
                )
                return {"statusCode": 200, "body": json.dumps("Ejecutado exitosamente")}
    
    logger.info(
        f"No hay insumos suficientes para validar o publicar la curva {swap_name}"
    )
    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
