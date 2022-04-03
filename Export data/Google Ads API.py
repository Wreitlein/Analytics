""" Данные по Google-рекламе (обновленная выгрузка на новом API)
Описание полей можно найти в документации https://developers.google.com/google-ads/api/fields/v7/ad_group_ad
"""

import pandas as pd
import logging
from datetime import datetime, timedelta, date
from google.ads.googleads.client import GoogleAdsClient


def get_creeds(login_customer_id=None) -> dict:
    """
    Функция возвращает креды для создания клиента
    @param login_customer_id: Идентификатор зависит от аккаунта к которому осуществляются запросы
    @type login_customer_id: str
    @return: Словарь кредов
    @rtype: dict
    """
    hook = HttpHook(http_conn_id='google_ads_new', method='GET')
    conn = hook.get_connection('google_ads_new')

    cred_dict = {
        'developer_token': conn.extra_dejson.get('developer_token'),
        'refresh_token': conn.extra_dejson.get('refresh_token'),
        'client_id': conn.extra_dejson.get('client_id'),
        'client_secret': conn.extra_dejson.get('client_secret'),
        'access_token': conn.extra_dejson.get('access_token'),
        'use_proto_plus': True,
        'login_customer_id': login_customer_id
    }
    return cred_dict


def create_client(cred_dict, _version="v9") -> object:
    """
    Функция для создания класса клиента, по которому в дальнейшим осуществляется запросы
    @param cred_dict: Словарь кредов получается функцией get_creeds
    @type cred_dict: dict
    @param _version: Версия API
    @type _version: str
    @return: Класс для обращений к API
    @rtype: object
    """
    try:
        client = GoogleAdsClient.load_from_dict(cred_dict, version=_version)
        return client
    except Exception as e:
        logging.error(f"Create client Failed \nPlease check function 'create_client'\n{e}")


def _account_hierarchy(customer_client, customer_ids_to_child_accounts) -> dict:
    """
    Функция для парсинга иерархии аккаунта
    @param customer_client: Класс клиента
    @type customer_client: object
    @param customer_ids_to_child_accounts: Класс кабинетов клиента
    @type customer_ids_to_child_accounts: object
    @return: Словарь клиента со списком всех входящих кабинетов
    @rtype: dict
    """
    # проверка на наличие клиентов у аккаунта
    if len(customer_ids_to_child_accounts) > 0:
        dict_customer_client = {
            'client_id': customer_client.id,
            'client_name': customer_client.descriptive_name,
            'customers_client': []
        }
        for child_account in customer_ids_to_child_accounts[customer_client.id]:
            dict_child_account = {
                'id': child_account.id,
                'name': child_account.descriptive_name,
                'currency_code': child_account.currency_code,
                'time_zone': child_account.time_zone
            }
            dict_customer_client['customers_client'].append(dict_child_account)
    # Если у аккаунта нет клиентов - в качестве клиента возвращаем сам аккаунт
    else:
        dict_customer_client = {
            'client_id': customer_client.id,
            'client_name': customer_client.descriptive_name,
            'customers_client': [
                {
                    'id': customer_client.id,
                    'name': customer_client.descriptive_name,
                    'currency_code': customer_client.currency_code,
                    'time_zone': customer_client.time_zone
                }
            ]
        }
    return dict_customer_client


def get_account_list(client, login_customer_id=None) -> list:
    """
    Функция для получения иерархии аккаунта
    @param client: Клиент к которому осуществляются запросы, получается из create_client
    @type client: object
    @param login_customer_id: Идентификатор клиента, если нужно получить по конкретному,
     если None - возвращает по всем доступным
    @type login_customer_id: str
    @return: Список словарей с иерархией аккаунта
    @rtype: list
    """

    # Получает экземпляры клиентов GoogleAdsService и CustomerService.
    googleads_service = client.get_service("GoogleAdsService")
    customer_service = client.get_service("CustomerService")

    # Лист идентификаторов клиентов для обработки.
    seed_customer_ids = []

    # Создает запрос, который извлекает все дочерние учетные записи менеджера.
    query = """
        SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1"""

    # Если идентификатор менеджера был указан в параметре customerId, он будет единственным идентификатором в списке.
    # В противном случае мы отправим запрос для всех клиентов,
    # доступных для этой аутентифицированной учетной записи Google.
    if login_customer_id is not None:
        seed_customer_ids = [login_customer_id]
    else:
        logging.info(
            "No manager ID is specified. The example will print the "
            "hierarchies of all accessible customer IDs."
        )

        customer_resource_names = (
            customer_service.list_accessible_customers().resource_names
        )

        for customer_resource_name in customer_resource_names:
            customer_id = googleads_service.parse_customer_path(
                customer_resource_name
            )["customer_id"]
            logging.info(customer_id)
            seed_customer_ids.append(customer_id)
    list_hierarchy = []
    for seed_customer_id in seed_customer_ids:
        unprocessed_customer_ids = [seed_customer_id]
        customer_ids_to_child_accounts = dict()
        root_customer_client = None

        while unprocessed_customer_ids:
            customer_id = int(unprocessed_customer_ids.pop(0))
            response = googleads_service.search(
                customer_id=str(customer_id), query=query
            )

            # Выполняет итерацию по всем строкам на всех страницах,
            # чтобы получить все кабинеты клиентов
            for googleads_row in response:
                customer_client = googleads_row.customer_client

                # Кабинет клиента, который с уровнем 0 является указанным Кабинет
                if customer_client.level == 0:
                    if root_customer_client is None:
                        root_customer_client = customer_client
                    continue

                if customer_id not in customer_ids_to_child_accounts:
                    customer_ids_to_child_accounts[customer_id] = []

                customer_ids_to_child_accounts[customer_id].append(
                    customer_client
                )

                if customer_client.manager:
                    # Клиентом могут управлять несколько менеджеров, поэтому
                    # предотвратить посещение одного и того же клиента много раз, мы
                    # проверяем, есть ли он уже в словаре.
                    if (
                            customer_client.id not in customer_ids_to_child_accounts
                            and customer_client.level == 1
                    ):
                        unprocessed_customer_ids.append(customer_client.id)

        if root_customer_client is not None:
            logging.info(
                "The hierarchy of customer ID "
                f"{root_customer_client.id} is printed below:"
            )
            list_hierarchy.append(_account_hierarchy(root_customer_client, customer_ids_to_child_accounts))
        else:
            logging.info(
                f"Customer ID {login_customer_id} is likely a test "
                "account, so its customer client information cannot be "
                "retrieved."
            )
    return list_hierarchy


def get_ads_data(client, account_id, start_date, end_date) -> pd.DataFrame:
    """
    Функция для получения данных по аккаунту. Для добавление новых метрик нужно
    1. Добавить их в запрос (query)
    2. Добавить в словарь (tmp_dict)
    Документация https://developers.google.com/google-ads/api/fields/v7/ad_group_ad

    @param client: Объект клиента из функции create_client
    @type client: object
    @param account_id: Идентификатор РК
    @type account_id: str
    @param start_date: Дата начала в формате 'YYYY-MM-DD'
    @type start_date: str
    @param end_date: Дата окончания в формате 'YYYY-MM-DD'
    @type end_date: str
    @return: Возвращает pd.DataFrame с данными в разрезе объявлений
    @rtype: pd.DataFrame
    """
    ga_service = client.get_service("GoogleAdsService")

    query = """
    SELECT
      -- Кампании
      campaign.id,
      campaign.name,
      campaign.status,
      -- Группа объявлений
      ad_group.name,
      ad_group.id,
      ad_group.status,
      ad_group_ad.labels,
      -- Объявления
      ad_group_ad.ad.id,
      ad_group_ad.ad.type, 
      ad_group_ad.ad.tracking_url_template,
      ad_group_ad.ad.device_preference,
      ad_group_ad.ad.expanded_text_ad.description,
      ad_group_ad.ad.expanded_text_ad.description2,
      ad_group_ad.ad.display_url,
      ad_group_ad.ad.expanded_text_ad.headline_part1,
      ad_group_ad.ad.expanded_text_ad.headline_part2,
      ad_group_ad.ad.expanded_text_ad.headline_part3,
      -- Метрики
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      metrics.all_conversions,
      metrics.view_through_conversions,
      -- Сегменты
      segments.date,
      segments.ad_network_type,
      segments.device
    FROM ad_group_ad
    WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """' -- AND ad_group_ad.status != 'REMOVED'
    ORDER BY campaign.id
        """
    # Получение данных
    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = account_id
    search_request.query = query
    response = ga_service.search_stream(search_request)

    data_df = pd.DataFrame()
    try:
        for batch in response:
            for row in batch.results:
                tmp_dict = {'campaign_name': row.campaign.name,
                            'campaign_id': row.campaign.id,
                            'campaign_status': row.campaign.status.name,
                            'ad_group_id': row.ad_group.id,
                            'ad_group_name': row.ad_group.name,
                            'ad_group_status': row.ad_group.status.name,
                            'labels': row.ad_group.labels,
                            'ad_id': row.ad_group_ad.ad.id,
                            'ad_type': row.ad_group_ad.ad.type_.name,
                            'tracking_url_template': row.ad_group_ad.ad.tracking_url_template,
                            'description': row.ad_group_ad.ad.expanded_text_ad.description,
                            'description2': row.ad_group_ad.ad.expanded_text_ad.description2,
                            'display_url': row.ad_group_ad.ad.display_url,
                            'headline_part1': row.ad_group_ad.ad.expanded_text_ad.headline_part1,
                            'headline_part2': row.ad_group_ad.ad.expanded_text_ad.headline_part2,
                            'headline_part3': row.ad_group_ad.ad.expanded_text_ad.headline_part3,
                            'clicks': row.metrics.clicks,
                            'impressions': row.metrics.impressions,
                            'cost': row.metrics.cost_micros / 1000000,
                            'all_conversions': row.metrics.all_conversions,
                            'view_through_conversions': row.metrics.view_through_conversions,
                            'start_date': row.segments.date,
                            'ad_network_type': row.segments.ad_network_type.name,
                            'device': row.segments.device.name
                            }
                data_df = data_df.append(tmp_dict, ignore_index=True)

        # Приведение к нужным типам
        data_df['campaign_id'] = data_df['campaign_id'].astype(int)
        data_df['ad_group_id'] = data_df['ad_group_id'].astype(int)
        data_df['ad_id'] = data_df['ad_id'].astype(int)
        data_df['clicks'] = data_df['clicks'].astype(int)
        data_df['impressions'] = data_df['impressions'].astype(int)
        data_df['cost'] = data_df['cost'].astype(float)
        data_df['all_conversions'] = data_df['all_conversions'].astype(int)
        data_df['view_through_conversions'] = data_df['view_through_conversions'].astype(int)

        return data_df
    except:
        return None


def get_ads_data_performance(client, account_id, start_date, end_date) -> pd.DataFrame:
    """
    Функция для получения данных по аккаунту. Для добавление новых метрик нужно
    1. Добавить их в запрос (query)
    2. Добавить в словарь (tmp_dict)
    Документация https://developers.google.com/google-ads/api/fields/v7/ad_group_ad

    Это отдельная функция для выгрузки Performance Max т.к. они выгружаются только в разрезе кампаний
    поля с ad_group_id и ad_id сохранены, но будут пустые

    @param client: Объект клиента из функции create_client
    @type client: object
    @param account_id: Идентификатор РК
    @type account_id: str
    @param start_date: Дата начала в формате 'YYYY-MM-DD'
    @type start_date: str
    @param end_date: Дата окончания в формате 'YYYY-MM-DD'
    @type end_date: str
    @return: Возвращает pd.DataFrame с данными в разрезе объявлений
    @rtype: pd.DataFrame
    """
    ga_service = client.get_service("GoogleAdsService")

    query = """
    SELECT
      -- Кампании
      campaign.id,
      campaign.name,
      campaign.status,
      -- Метрики
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      metrics.all_conversions,
      metrics.view_through_conversions,
      -- Сегменты
      segments.date,
      segments.ad_network_type,
      segments.device
    FROM campaign
    WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """' 
    and campaign.advertising_channel_type = PERFORMANCE_MAX
    ORDER BY campaign.id
        """
    # Получение данных
    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = account_id
    search_request.query = query
    response = ga_service.search_stream(search_request)

    data_df = pd.DataFrame()
    try:
        for batch in response:
            for row in batch.results:
                tmp_dict = {'campaign_name': row.campaign.name,
                            'campaign_id': row.campaign.id,
                            'campaign_status': row.campaign.status.name,
                            'ad_group_id': row.ad_group.id,
                            'ad_group_name': row.ad_group.name,
                            'ad_group_status': row.ad_group.status.name,
                            'labels': row.ad_group.labels,
                            'ad_id': row.ad_group_ad.ad.id,
                            'ad_type': row.ad_group_ad.ad.type_.name,
                            'tracking_url_template': row.ad_group_ad.ad.tracking_url_template,
                            'description': row.ad_group_ad.ad.expanded_text_ad.description,
                            'description2': row.ad_group_ad.ad.expanded_text_ad.description2,
                            'display_url': row.ad_group_ad.ad.display_url,
                            'headline_part1': row.ad_group_ad.ad.expanded_text_ad.headline_part1,
                            'headline_part2': row.ad_group_ad.ad.expanded_text_ad.headline_part2,
                            'headline_part3': row.ad_group_ad.ad.expanded_text_ad.headline_part3,
                            'clicks': row.metrics.clicks,
                            'impressions': row.metrics.impressions,
                            'cost': row.metrics.cost_micros / 1000000,
                            'all_conversions': row.metrics.all_conversions,
                            'view_through_conversions': row.metrics.view_through_conversions,
                            'start_date': row.segments.date,
                            'ad_network_type': row.segments.ad_network_type.name,
                            'device': row.segments.device.name
                            }
                data_df = data_df.append(tmp_dict, ignore_index=True)

        # Приведение к нужным типам
        data_df['campaign_id'] = data_df['campaign_id'].astype(int)
        data_df['ad_group_id'] = data_df['ad_group_id'].astype(int)
        data_df['ad_id'] = data_df['ad_id'].astype(int)
        data_df['clicks'] = data_df['clicks'].astype(int)
        data_df['impressions'] = data_df['impressions'].astype(int)
        data_df['cost'] = data_df['cost'].astype(float)
        data_df['all_conversions'] = data_df['all_conversions'].astype(int)
        data_df['view_through_conversions'] = data_df['view_through_conversions'].astype(int)

        return data_df
    except:
        return None


def get_date_range(start_days=5, stop_days=1) -> list:
    """
    Генерация списка дат для итерации
    @param start_days: Выбирается начало относительно сегодня в количестве дней
    @type start_days: int
    @param stop_days: Выбираем конец, по дефолту - до вчера
    @type stop_days: int
    @return: Список дат
    @rtype: list
    """
    start = (datetime.now() - timedelta(days=start_days)).date()
    end = (datetime.now() - timedelta(days=stop_days)).date()
    date_generated = [(start + timedelta(days=x)).strftime("%Y-%m-%d") for x in range(0, (end - start).days)]
    return date_generated


def main():
    # Итоговый фрейм для добавления данных
    df = pd.DataFrame()
    # Получаем креды
    creds = get_creeds()
    # Создаем клиента для получения списка кабинетов
    client = create_client(creds)
    # Получаем кабинеты
    account_hierarchy = get_account_list(client)
    # Создаем лист дат для итерации
    list_date = get_date_range(start_days=3)
    # Итерация по датам
    for day in list_date:
        logging.info(f'Start day {day}')
        # Итерация по клиентам
        for account in account_hierarchy:
            logging.info(f"Start account {account['client_id']}")
            # Создаем клиента для конкретного кабинета
            account_creeds = get_creeds(login_customer_id=account['client_id'])
            account_client = create_client(account_creeds)
            # Итерация по кабинетам
            for cabinet in account['customers_client']:
                logging.info(f"Start cabinet {cabinet['id']}")
                # Датафрейм для записи результатов одной итерации
                cabinet_df = get_ads_data(account_client, str(cabinet['id']), day, day)
                # Отдельно выгружаются кампании "максимальной эффективности"
                cabinet_perf_max_df = get_ads_data_performance(account_client, str(cabinet['id']), day, day)
                # Объединяем данные
                if cabinet_df is not None or cabinet_perf_max_df is not None:
                    cabinet_all_df = pd.concat([cabinet_df, cabinet_perf_max_df])
                else:
                    continue
                if cabinet_all_df is not None:
                    cabinet_all_df['account_name'] = account['client_name']
                    cabinet_all_df['account_id'] = account['client_id']
                    cabinet_all_df['cabinet_name'] = cabinet['name']
                    cabinet_all_df['cabinet_id'] = cabinet['id']
                    cabinet_all_df['date'] = day
                    cabinet_all_df['currency_code'] = cabinet['currency_code']
                    cabinet_all_df['time_zone'] = cabinet['time_zone']

                    # Добавляем в финальный фрейм
                    df = pd.concat([df, cabinet_all_df])
                else:
                    continue
    return df


_target_sql = '''
select
    md5(account_id+cabinet_id+date::varchar+campaign_id+ad_group_id+ad_id) as upd_key,
    account_name,
    account_id,
    cabinet_name,
    cabinet_id,
    date,
    currency_code,
    time_zone,
    campaign_name,
    campaign_id,
    campaign_status,
    ad_group_id,
    ad_group_name,
    ad_group_status,
    labels,
    ad_id,
    ad_type,
    tracking_url_template,
    description,
    description2,
    display_url,
    headline_part1,
    headline_part2,
    headline_part3,
    clicks,
    impressions,
    cost,
    all_conversions,
    view_through_conversions,
    start_date,
    ad_network_type,
    device
from [[ads]]
;'''

fields_types = {
    'upd_key': Type.VARCHAR,
    'account_name': Type.VARCHAR,
    'account_id': Type.VARCHAR,
    'cabinet_name': Type.VARCHAR,
    'cabinet_id': Type.VARCHAR,
    'date': Type.DATE,
    'currency_code': Type.VARCHAR,
    'time_zone': Type.VARCHAR,
    'campaign_name': Type.VARCHAR,
    'campaign_id': Type.VARCHAR,
    'campaign_status': Type.VARCHAR,
    'ad_group_id': Type.VARCHAR,
    'ad_group_name': Type.VARCHAR,
    'ad_group_status': Type.VARCHAR,
    'labels': Type.VARCHAR,
    'ad_id': Type.VARCHAR,
    'ad_type': Type.VARCHAR,
    'tracking_url_template': Type.VARCHAR(32768),
    'description': Type.VARCHAR(32768),
    'description2': Type.VARCHAR(32768),
    'display_url': Type.VARCHAR(32768),
    'headline_part1': Type.VARCHAR(32768),
    'headline_part2': Type.VARCHAR(32768),
    'headline_part3': Type.VARCHAR(32768),
    'clicks': Type.INTEGER,
    'impressions': Type.INTEGER,
    'cost': Type.FLOAT,
    'all_conversions': Type.INTEGER,
    'view_through_conversions': Type.INTEGER,
    'start_date': Type.DATE,
    'ad_network_type': Type.VARCHAR,
    'device': Type.VARCHAR
}

tpl = EntityBuilderTemplate(
    User.Efremov,
    is_incremental=True,
    update_key='upd_key',
    schedule_interval='0 5 * * *',
    fields=fields_types,
    query=_target_sql
)

tpl.add_stage(Stage([
    FetchListSourceOperator(name='ads',
                            python_callable=main,
                            types=fields_types
                            )
]))
dag = tpl.DAG()
