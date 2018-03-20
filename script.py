import os
import json
import requests

import pandas as pd
import numpy as np
from io import StringIO

is_prod = os.environ.get('IS_PROD', None)

if is_prod:
	username = os.environ.get('METABASE_USERNAME')
	password = os.environ.get('METABASE_PASSWORD')
	api_endpoint = os.environ.get('METABASE_ENDPOINT')
else:
	from credentials import *

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

if session_token:
    session_headers = {"Content-Type": "application/json", "X-Metabase-Session": session_token}

    # demanda_endpoint = "card/173/query/json"
    # demanda_res = requests.post(base_uri + demanda_endpoint, headers=session_headers)
    # demanda_json = demanda_res.json()

    demanda_endpoint = "card/173/query/csv"
    demanda_res = requests.post(base_uri + demanda_endpoint, headers=session_headers)
    demanda_csv = demanda_res.text
    demanda_cols = ["cd_unidade_educacao", "cd_serie_ensino", "count"]
    demanda_df = pd.read_csv(StringIO(demanda_csv), usecols=demanda_cols, index_col="cd_unidade_educacao")
    demanda_pivot = pd.pivot_table(demanda_df, values="count", index="cd_unidade_educacao", columns="cd_serie_ensino", aggfunc=np.sum)
    demanda_pivot = demanda_pivot.add_prefix("dem_")

    matriculas_endpoint = "card/175/query/csv"
    matriculas_res = requests.post(base_uri + matriculas_endpoint, headers=session_headers)
    matriculas_csv = matriculas_res.text
    matriculas_cols = ["cd_escola", "cd_serie_ensino", "sum"]
    matriculas_df = pd.read_csv(StringIO(matriculas_csv), usecols=matriculas_cols, index_col="cd_escola")
    matriculas_pivot = pd.pivot_table(matriculas_df, values="sum", index="cd_escola", columns="cd_serie_ensino", aggfunc=np.sum)
    matriculas_pivot = matriculas_pivot.add_prefix("mat_")

    vagas_endpoint = "card/176/query/csv"
    vagas_res = requests.post(base_uri + vagas_endpoint, headers=session_headers)
    vagas_csv = vagas_res.text
    vagas_cols = ["cd_escola", "cd_serie_ensino", "sum"]
    vagas_df = pd.read_csv(StringIO(vagas_csv), usecols=vagas_cols, index_col="cd_escola")
    vagas_pivot = pd.pivot_table(vagas_df, values="sum", index="cd_escola", columns="cd_serie_ensino", aggfunc=np.sum)
    vagas_pivot = vagas_pivot.add_prefix("vag_")

    demanda_join = demanda_pivot.join(matriculas_pivot).join(vagas_pivot)
    demanda_join.index.names = ["cod"]
    demanda_join_json = demanda_join.to_json(orient='index')
    with open("demanda_join_json.json", "w") as text_file:
        print(demanda_join_json, file=text_file)
    demanda_join_csv = demanda_join.to_csv()
    with open("demanda_join_csv.csv", "w") as text_file:
        print(demanda_join_csv, file=text_file)

    escolas_endpoint = "card/177/query/csv"
    escolas_res = requests.post(base_uri + escolas_endpoint, headers=session_headers)
    escolas_csv = escolas_res.text
    escolas_cols = ["cd_unidade_educacao","nm_exibicao_unidade_educacao","tp_escola","sg_tp_escola","cd_latitude","cd_longitude","endereco_completo"]
    escolas_df = pd.read_csv(StringIO(escolas_csv), usecols=escolas_cols, index_col="cd_unidade_educacao")
    escolas_df = escolas_df.rename(columns={"nm_exibicao_unidade_educacao": "nome", "tp_escola": "tipo_cd", "sg_tp_escola": "tipo", "cd_latitude": "lat", "cd_longitude": "lon", "endereco_completo": "end"})

    contatos_endpoint = "card/178/query/csv"
    contatos_res = requests.post(base_uri + contatos_endpoint, headers=session_headers)
    contatos_csv = contatos_res.text
    contatos_cols = ["cd_unidade_educacao","dc_tipo_dispositivo_comunicacao","dc_dispositivo","cd_ramal"]
    contatos_df = pd.read_csv(StringIO(contatos_csv), usecols=contatos_cols, index_col="cd_unidade_educacao")
    contatos_df = contatos_df.rename(columns={"dc_tipo_dispositivo_comunicacao": "tipo", "dc_dispositivo": "num", "cd_ramal": "ramal"})
    # contatos_groupby_esc = contatos_df.groupby("cd_unidade_educacao").apply(lambda x: x.to_json(orient='records'))
    # contatos_groupby_esc_df = pd.DataFrame({"cd_unidade_educacao":contatos_groupby_esc.index, "ct":contatos_groupby_esc.values}).set_index("cd_unidade_educacao")
    # only num:
    contatos_groupby_esc = contatos_df["num"].groupby("cd_unidade_educacao").apply(lambda x: x.to_json(orient='records'))
    contatos_groupby_esc_df = pd.DataFrame({"cd_unidade_educacao":contatos_groupby_esc.index, "ct":contatos_groupby_esc.values}).set_index("cd_unidade_educacao")

    escolas_join = escolas_df.join(contatos_groupby_esc_df)
    escolas_df.index.names = ["cod"]
    escolas_join_json = escolas_join.to_json(orient='index')
    with open("escolas_join_json.json", "w") as text_file:
        print(escolas_join_json, file=text_file)

    escolas_join_csv = escolas_join.to_csv()
    with open("escolas_join_csv.csv", "w") as text_file:
        print(escolas_join_csv, file=text_file)

print('done.')
