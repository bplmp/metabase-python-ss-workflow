import os
import json
import requests
import gzip
import datetime

import pandas as pd
import numpy as np
from io import StringIO

import geojson
from geojson import Feature, Point, FeatureCollection

# bot that sends telegram messages
from bot import *

# mailer
import sendgrid
from sendgrid.helpers.mail import *

# library to upload to static server on aws s3
import tinys3

is_prod = os.environ.get('IS_PROD', None)

if is_prod:
    username = os.environ.get('METABASE_USERNAME')
    password = os.environ.get('METABASE_PASSWORD')
    api_endpoint = os.environ.get('METABASE_ENDPOINT')
    sendgrid_key = os.environ.get('SENDGRID_API_KEY')
    recipient_emails = os.environ.get('RECIPIENT_EMAILS')
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.environ.get("AWS_BUCKET_NAME")
else:
    from credentials import *

def get_data_and_upload():
    global log_message

    base_uri = api_endpoint
    session_token = False;

    payload = {
      "username": username,
      "password": password
    }
    headers = {"Content-Type": "application/json"}

    res = requests.post(base_uri + "session", data=json.dumps(payload), headers=headers)

    if res.status_code == requests.codes.ok:
        session_token = res.json()["id"]
        print("-->Authenticated successfully.")

    session_headers = {"Content-Type": "application/json", "X-Metabase-Session": session_token}

    # demanda
    demanda_endpoint = "card/173/query/csv"
    demanda_res = requests.post(base_uri + demanda_endpoint, headers=session_headers)
    if demanda_res.status_code == requests.codes.ok:
        msg = f"-->Downloaded {demanda_endpoint} successfully."
        print(msg)
        log_message += msg + "\n"
    demanda_csv = demanda_res.text
    demanda_cols = ["cd_unidade_educacao", "cd_serie_ensino", "count"]
    demanda_df = pd.read_csv(StringIO(demanda_csv), usecols=demanda_cols, index_col="cd_unidade_educacao")
    demanda_pivot = pd.pivot_table(demanda_df, values="count", index="cd_unidade_educacao", columns="cd_serie_ensino", aggfunc=np.sum)
    demanda_pivot = demanda_pivot.add_prefix("dem_")

    dem_1_total = demanda_pivot['dem_1'].sum()
    dem_4_total = demanda_pivot['dem_4'].sum()
    dem_27_total = demanda_pivot['dem_27'].sum()
    dem_28_total = demanda_pivot['dem_28'].sum()
    dem_total = dem_1_total + dem_4_total + dem_27_total + dem_28_total

    dem_msg = f"""-->Waits are:
    -dem_1: {dem_1_total}
    -dem_4: {dem_4_total}
    -dem_27: {dem_27_total}
    -dem_28: {dem_28_total}
    -dem_total: {dem_total}"""
    log_message += dem_msg + "\n"

    # matriculas
    matriculas_endpoint = "card/175/query/csv"
    matriculas_res = requests.post(base_uri + matriculas_endpoint, headers=session_headers)
    if matriculas_res.status_code == requests.codes.ok:
        msg = f"-->Downloaded {matriculas_endpoint} successfully."
        print(msg)
        log_message += msg + "\n"
    matriculas_csv = matriculas_res.text
    matriculas_cols = ["cd_escola", "cd_serie_ensino", "sum"]
    matriculas_df = pd.read_csv(StringIO(matriculas_csv), usecols=matriculas_cols, index_col="cd_escola")
    matriculas_pivot = pd.pivot_table(matriculas_df, values="sum", index="cd_escola", columns="cd_serie_ensino", aggfunc=np.sum)
    matriculas_pivot = matriculas_pivot.add_prefix("mat_")

    # vagas
    vagas_endpoint = "card/176/query/csv"
    vagas_res = requests.post(base_uri + vagas_endpoint, headers=session_headers)
    if vagas_res.status_code == requests.codes.ok:
        msg = f"-->Downloaded {vagas_endpoint} successfully."
        print(msg)
        log_message += msg + "\n"
    vagas_csv = vagas_res.text
    vagas_cols = ["cd_escola", "cd_serie_ensino", "sum"]
    vagas_df = pd.read_csv(StringIO(vagas_csv), usecols=vagas_cols, index_col="cd_escola")
    vagas_pivot = pd.pivot_table(vagas_df, values="sum", index="cd_escola", columns="cd_serie_ensino", aggfunc=np.sum)
    vagas_pivot = vagas_pivot.add_prefix("vag_")

    # join demanda, matriculas, vagas
    demanda_join = demanda_pivot.join(matriculas_pivot).join(vagas_pivot)
    demanda_join.index.names = ["cod"]
    demanda_join_json = demanda_join.to_json(orient='index')

    # data atualizacao demanda
    dem_atual_endpoint = "card/180/query/json"
    dem_atual_res = requests.post(base_uri + dem_atual_endpoint, headers=session_headers)
    if dem_atual_res.status_code == requests.codes.ok:
        msg = f"-->Downloaded {dem_atual_endpoint} successfully."
        print(msg)
        log_message += msg + "\n"
    dem_atual_json = json.loads(dem_atual_res.text)

    if len(dem_atual_json) > 0:
        dem_atual_data = dem_atual_json[0]['dt_status_solicitacao']
        msg = f"-->Schools wait last updated at {dem_atual_data} in Metabase."
        print(msg)
        log_message += msg + "\n"

    # join data atualizacao demanda to demanda json
    demanda_join_dict = json.loads(demanda_join_json)
    demanda_join_dict["updated_at"] = dem_atual_data
    demanda_join_json = json.dumps(demanda_join_dict)

    # save to file
    with open("demanda_join.json", "w") as text_file:
        print(demanda_join_json, file=text_file)

    # compress file
    with gzip.open('demanda_join_json.gz', 'wb') as f:
        f.write(demanda_join_json.encode('utf-8'))

    # escolas
    escolas_endpoint = "card/177/query/csv"
    escolas_res = requests.post(base_uri + escolas_endpoint, headers=session_headers)
    if escolas_res.status_code == requests.codes.ok:
        msg = f"-->Downloaded {escolas_endpoint} successfully."
        print(msg)
        log_message += msg + "\n"
    escolas_csv = escolas_res.text
    escolas_cols = ["cd_unidade_educacao","nm_exibicao_unidade_educacao","tp_escola","sg_tp_escola","cd_latitude","cd_longitude","endereco_completo"]
    escolas_df = pd.read_csv(StringIO(escolas_csv), usecols=escolas_cols, index_col="cd_unidade_educacao")
    escolas_df = escolas_df.rename(columns={"nm_exibicao_unidade_educacao": "nome", "tp_escola": "tipo_cd", "sg_tp_escola": "tipo", "cd_latitude": "lat", "cd_longitude": "lon", "endereco_completo": "end"})

    # contatos
    contatos_endpoint = "card/178/query/csv"
    contatos_res = requests.post(base_uri + contatos_endpoint, headers=session_headers)
    if contatos_res.status_code == requests.codes.ok:
        msg = f"-->Downloaded {contatos_endpoint} successfully."
        print(msg)
        log_message += msg + "\n"
    contatos_csv = contatos_res.text
    contatos_cols = ["cd_unidade_educacao","dc_tipo_dispositivo_comunicacao","dc_dispositivo","cd_ramal"]
    contatos_df = pd.read_csv(StringIO(contatos_csv), usecols=contatos_cols, index_col="cd_unidade_educacao")
    contatos_df = contatos_df.rename(columns={"dc_tipo_dispositivo_comunicacao": "tipo", "dc_dispositivo": "num", "cd_ramal": "ramal"})
    # contatos_groupby_esc = contatos_df.groupby("cd_unidade_educacao").apply(lambda x: x.to_json(orient='records'))
    # contatos_groupby_esc_df = pd.DataFrame({"cd_unidade_educacao":contatos_groupby_esc.index, "ct":contatos_groupby_esc.values}).set_index("cd_unidade_educacao")
    # only num:
    contatos_groupby_esc = contatos_df["num"].groupby("cd_unidade_educacao").apply(lambda x: x.to_json(orient='records'))
    contatos_groupby_esc_df = pd.DataFrame({"cd_unidade_educacao":contatos_groupby_esc.index, "ct":contatos_groupby_esc.values}).set_index("cd_unidade_educacao")

    # join escolas, contatos
    escolas_join = escolas_df.join(contatos_groupby_esc_df)
    escolas_df.index.names = ["cod"]
    escolas_join_json = escolas_join.to_json(orient='index')

    # convert escolas json to geojson
    escolas_features = []
    for index, row in escolas_join[escolas_join.lat.notnull()].iterrows():
        selected_columns = row[["nome", "tipo_cd", "tipo", "end", "ct"]]
        if pd.notnull(selected_columns["ct"]):
            selected_columns["ct"] = json.loads(selected_columns["ct"])
        else:
            selected_columns["ct"] = ""
        selected_columns["cod"] = str(index)
        properties = selected_columns.to_dict()
        feature = Feature(geometry=Point((row["lon"], row["lat"])), properties=properties)
        escolas_features.append(feature)

    # save to file
    escolas_join_collection = FeatureCollection(escolas_features)
    escolas_join_geojson = geojson.dumps(escolas_join_collection, sort_keys=True)
    with open("escolas_join.geojson", "w") as text_file:
        print(escolas_join_geojson, file=text_file)

    # compress
    with gzip.open('escolas_join_geojson.gz', 'wb') as f:
        f.write(escolas_join_geojson.encode('utf-8'))

    end_msg = "-->All downloaded, saved and compressed."
    print(end_msg)
    log_message += end_msg + "\n"

    # upload to static server
    upload_aws("demanda_join_json.gz", 86400)
    upload_aws("escolas_join_geojson.gz", 604800)

def upload_aws(filename, expiration):
    global log_message
    conn = tinys3.Connection(aws_access_key_id, aws_secret_access_key, tls=True)
    upload_msg = f"""-->Uploading {filename} to bucket {bucket_name} \n"""
    print(upload_msg)
    log_message += upload_msg
    f = open(filename, "rb")
    conn.upload(filename, f, bucket_name, public=True, expires=expiration, content_type="application/json", headers={'content-encoding': 'gzip'})
    done_msg = "-->Done uploading. \n"
    print(done_msg)
    log_message += done_msg

def send_log_email(message, time):
    for recipient in recipient_emails:
        sg = sendgrid.SendGridAPIClient(apikey=sendgrid_key)
        from_email = Email("logs@filadacreche.com")
        to_email = Email(recipient)
        subject = f"[Fila da Creche] Data logs {current_time}"
        content = Content("text/plain", message)
        mail = Mail(from_email, subject, to_email, content)
        response = sg.client.mail.send.post(request_body=mail.get())
        print(response.headers)

# set current time and log message
current_time = datetime.datetime.now()
log_message = f"-->Running script at {current_time} \n"

# actually run the script
try:
    get_data_and_upload()
except Exception as e:
    # catch errors
    print(log_message, '-->Error raised:\n', e)
    log_message += '-->Error raised:\n'
    log_message += str(e)
else:
    print(log_message, '-->Everything ok\n')
finally:
    done_msg = '\n-->All done.<--'
    log_message += done_msg
    send_log_email(log_message, current_time) # sends an email with log
    send_telegram_msg(log_message, group_chat_id) # sends a telegram message with log
    print(done_msg)
