"""Статистика по рекламе в VK по кабинетам: 11111111111, 222222222

@field ads_unique_key составной ключ для инкремента
@field ad_id Идентификатор объявления
@field ad_name Названия объявления
@field campaign_id Идентификатор кампании
@field campaign_name Название кампании
@field campaign_type Тип кампании
@field ad_type Тип объявления
@field project_name Название клиента
@field project_id Идентификатор клиента
@field date Дата за которую предоставлена статистика
@field spent Расходы
@field impressions Показы
@field clicks Клики
@field reach Охваты
"""

import requests
import pandas as pd
import logging
import datetime
import time
from datetime import datetime, timedelta


_TOKEN = '11111111111111111111111111111'
_VERSION = 5.131
_IDS_RK = [11111111111, 222222222]


def trying(func) -> list:
    """
    Декоратор для попыток получения данных и логирования процессов
    """

    def wrapper(*args, **kwargs):
        tryin = 0
        while tryin < 11:
            data = func(*args, **kwargs)
            if 'response' in data:
                logging.info(f"Response {func.__name__} true")
                return data['response']
            else:
                if 'error' in data and data['error']['error_code'] == 600:
                    break  # exclude error "Permission denied"
                tryin += 1
                logging.error(f'tryin: {tryin}')
                logging.error(data['error']['error_msg'])
                time.sleep(10)
                continue
        raise Exception('No response in data')

    return wrapper


@trying
def get_rk_list(token, id_rk) -> dict:
    """
    Функция возвращает список клиентов в рекламном кабинете
    """
    vk_accounts_url = 'https://api.vk.com/method/ads.getClients'
    params = {
        'access_token': token,
        'v': 5.131,
        'account_id': id_rk
    }
    resp = requests.post(vk_accounts_url, params=params)
    return resp.json()


@trying
def getAdsData(token, id_rk, client_id) -> dict:
    """
    Функция возвращает список рекламных объявлений для каждого клиента
    """
    vk_accounts_url = 'https://api.vk.com/method/ads.getAds'
    params = {
        'access_token': token,
        'v': 5.131,
        'account_id': id_rk,
        'include_deleted': 1,
        'client_id': client_id
    }
    resp = requests.post(vk_accounts_url, params=params)
    return resp.json()


@trying
def getStatistics(token, id_rk, ids, date_from, date_to) -> dict:
    """
    Функция возвращает статситку по объявлениям в разрезе дней
    """
    vk_accounts_url = 'https://api.vk.com/method/ads.getStatistics'
    params = {
        'access_token': token,
        'v': 5.131,
        'account_id': id_rk,
        'ids_type': 'ad',
        'period': 'day',
        'ids': ids,
        'date_from': date_from,
        'date_to': date_to
    }
    resp = requests.post(vk_accounts_url, params=params)
    return resp.json()


@trying
def getCampaigns(token, id_rk, client_id) -> dict:
    """
    Функция возвращает список рекламных кампаний
    """
    vk_accounts_url = 'https://api.vk.com/method/ads.getCampaigns'
    params = {
        'access_token': token,
        'v': 5.131,
        'account_id': id_rk,
        'include_deleted': 1,
        'client_id': client_id
    }
    resp = requests.post(vk_accounts_url, params=params)
    return resp.json()


def main():
    ads_init = pd.DataFrame(
        columns=['id', 'campaign_id', 'status', 'approved', 'create_time', 'update_time', 'goal_type', 'day_limit',
                 'all_limit', 'start_time', 'stop_time', 'category1_id', 'category2_id', 'age_restriction', 'name',
                 'events_retargeting_groups', 'cost_type', 'ad_format', 'cpc', 'ad_platform',
                 'ad_platform_no_ad_network', 'cpm', 'impressions_limit', 'project_id', 'project_name'])

    list_colum = ['id', 'name', 'campaign_id', 'campaign_name', 'campaign_type', 'type', 'project_name', 'project_id',
                  'day', 'spent', 'impressions', 'clicks', 'reach']
    all_df = pd.DataFrame(columns=list_colum)

    # Проходимся циклом по всем клиентам в РК
    for id_rk in _IDS_RK:
        for param in get_rk_list(_TOKEN, id_rk):
            # Получаем список объявлений
            dataAds = getAdsData(_TOKEN, id_rk, param['id'])
            # Получаем список кампаний (нужно для названий)
            dataCampaigns = getCampaigns(_TOKEN, id_rk, param['id'])
            df_campaigns = pd.DataFrame(dataCampaigns)
            df_campaigns = df_campaigns.rename(columns={'id': 'campaign_id',
                                                        'name': 'campaign_name',
                                                        'type': 'campaign_type'})
            df_campaigns = df_campaigns[['campaign_id', 'campaign_name', 'campaign_type']]
            df_campaigns['project_id'] = param['id']

            # Считаем количество чанок
            rng = (len(dataAds) // 2000) + 1
            logging.info(f"Len rng {rng}")
            start_chunk = 0
            finish_chunk = 2000
            res_ads = []

            # Проходимся по чанкам и получаем статистику по объявлениям
            for x in range(rng):
                logging.info(f"Start chunk {x}")
                d_ad = pd.DataFrame(dataAds[start_chunk:finish_chunk])
                d_ad['id'] = d_ad.id.astype(str)
                id_str = ','.join(d_ad['id'].tolist())

                # Статистика за последние 3 дня, не включая текущий
                data = getStatistics(_TOKEN, id_rk, id_str,
                                     (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
                                     (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))

                res_ads.extend(data)
                logging.info(f"start_chunk: {start_chunk}, finish_chunk: {finish_chunk}")
                start_chunk += 2000
                finish_chunk += 2000
                time.sleep(1)

            data_ad = pd.DataFrame(dataAds)
            data_ad['project_name'] = param['name']
            data_ad['project_id'] = param['id']
            ads_init = pd.concat([ads_init, data_ad], sort=False)
            statistics = list()
            for pr in res_ads:
                for st in pr['stats']:
                    dict_params = {
                        'id': pr['id'],
                        'type': pr['type'],
                        'project_id': param['id']
                    }
                    statistics.append({**dict_params, **st})
            stats = pd.DataFrame(statistics)
            # проверяем наличие данных в статистике
            if stats.shape[0] > 0:
                stats = stats.fillna(0)
                logging.info(f'stats {stats.shape}, ads_init {ads_init.shape}')

                stats = stats.astype('object')
                ads_init = ads_init.astype('object')
                ads_init['id'] = ads_init['id'].astype('int64').astype('object')

                df = pd.merge(stats, ads_init, on=['project_id', 'id'], how='left')
                df = pd.merge(df, df_campaigns, on=['project_id', 'campaign_id'], how='left')
                df = df.fillna('')
                if 'clicks' not in df.columns:
                    df['clicks'] = 0
                if 'spent' not in df.columns:
                    df['spent'] = 0.0
                if 'impressions' not in df.columns:
                    df['impressions'] = 0
                if 'reach' not in df.columns:
                    df['reach'] = 0
                all_df = all_df.append(df[list_colum])
            else:
                continue

    all_df = all_df.rename(columns={'id': 'ad_id',
                                    'name': 'ad_name',
                                    'type': 'ad_type'
                                    })
    all_df['spent'] = all_df.spent.astype(float)
    all_df['impressions'] = all_df.impressions.astype(int)
    all_df['clicks'] = all_df.clicks.astype(int)
    all_df['reach'] = all_df.reach.astype(int)
    logging.info(all_df.dtypes)
    return all_df


_target_sql = '''
SELECT 
	md5(ad_id+project_id+campaign_id+day::text) as ads_unique_key,
	ad_id::bigint, 
	ad_name, 
	campaign_id::bigint,
	campaign_name,
	campaign_type,
	ad_type,
	project_name,
	project_id::bigint,
	day::date,
	spent::bigint,
	impressions::bigint,
	clicks::bigint,
	reach::bigint
FROM [[vk_ads]];'''


tpl = EntityBuilderTemplate(
    User.Efremov,
    is_incremental=True,
    update_key='ads_unique_key',
    schedule_interval='0 0 * * *',
    fields={
        'ads_unique_key': Type.VARCHAR,
        'ad_id': Type.BIGINT,
        'ad_name': Type.VARCHAR,
        'campaign_id': Type.BIGINT,
        'campaign_name': Type.VARCHAR,
        'campaign_type': Type.VARCHAR,
        'ad_type': Type.VARCHAR,
        'project_name': Type.VARCHAR,
        'project_id': Type.BIGINT,
        'day': Type.DATE,
        'spent': Type.FLOAT,
        'impressions': Type.BIGINT,
        'clicks': Type.BIGINT,
        'reach': Type.BIGINT
    },
    query=_target_sql
)

tpl.add_stage(Stage([
    FetchListSourceOperator(name='vk_ads', python_callable=main,
                            types={
                                'ad_id': Type.BIGINT,
                                'ad_name': Type.VARCHAR,
                                'campaign_id': Type.BIGINT,
                                'campaign_name': Type.VARCHAR,
                                'campaign_type': Type.VARCHAR,
                                'ad_type': Type.VARCHAR,
                                'project_name': Type.VARCHAR,
                                'project_id': Type.BIGINT,
                                'day': Type.DATE,
                                'spent': Type.FLOAT,
                                'impressions': Type.BIGINT,
                                'clicks': Type.BIGINT,
                                'reach': Type.BIGINT
                            })
]))
dag = tpl.DAG()
