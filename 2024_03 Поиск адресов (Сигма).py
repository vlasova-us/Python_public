#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().system('pip install requests')
get_ipython().system('pip install pandas')
import requests

import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import numpy as np
import json

pd.set_option("display.max_rows", 50)
pd.set_option("display.max_columns", 50)


# In[ ]:


# Загрузка из таблицы
df = pd.read_excel () # вставить адрес расположения файла
df


# # Фильтрация и преобразование записей

# In[ ]:


#Паттерны для поиска
pattern = '[_/\#\,\:\-\*]' # символы
type_of_building = ['БАНЯ','БАРАК', 'ПОДСОБНОЕ ХОЗЯЙСТВО', 'РАЗРУШЕН', 'РАССЕЛЕН, СНЕСЕН (М/ДОМ)', 
                    'РАССЕЛЕН, СНЕСЕН (Ч/СЕКТОР)', 'САРАЙ', 'СГОРЕЛ', 'СТРОИТЕЛЬСТВО', 'УСАДЬБА',
                   'ХОЗ.БЛОК', 'ХУТОP', 'ГАРАЖ']

pattern2 = '|'.join(type_of_building)
pattern3 = '[^\d+]' #поиск нецифровой строки
pattern4 = '^.{2,}$'#поиск более одного символа
pattern5 = '^$' #поиск пустой строки
pattern6 = '^0' #поиск строк, которые начинаются с 0
pattern7 = '\.' #поиск строк, которые содержат точку
pattern8 = '^\d+' #поиск цифровой строки

#Замены в квартире
df['KV_new'] = df['KV'].replace(' ', '').replace('б/н', '')

#df с данными, которые были забракованы
filtered_df_symb = df [ 
                       df['NASPUNKT'].str.contains(pattern, regex=True)| 
                       df['NASPUNKT'].str.contains('КМ')|
                       df['OBJTYPE_NAME'].str.contains(pattern2) |
                       df['DOM'].str.contains(pattern3, regex=True)|
                       df['DOM'].str.contains('-')|
                       df['DOM'].str.contains(pattern5, regex=True)|
                       df['DOM'].str.contains(pattern6, regex=True)|                   
                       df['KORP'].str.contains(pattern, regex=True)|
                       df['KORP'].str.contains(pattern4, regex=True)|
                       df['KORP'].str.contains(pattern7, regex=True)|
                       df['KORP'].str.contains(pattern6, regex=True)|
                       df['LITERA_DOM'].str.contains(pattern, regex=True)|
                       df['LITERA_DOM'].str.contains(pattern8, regex=True)|
                       df['LITERA_DOM'].str.contains(pattern4, regex=True)|
                       df['KV_new'].str.contains(pattern3, regex=True)|
                       df['KV_new'].str.contains(pattern, regex=True)|
                       df['KV_new'].str.contains(pattern7, regex=True)|
                       df['KV_new'].str.contains(pattern6, regex=True)
                      ]                      

filtered_df_symb.to_excel('Эти данные проверить в программе и потом отдать снова на обработку.xlsx', index=False)

#df с данными, которые были отобраны. 
filtered_df = df [ ~(
                    df['NASPUNKT'].str.contains(pattern, regex=True)| 
                       df['NASPUNKT'].str.contains('КМ')|
                       df['OBJTYPE_NAME'].str.contains(pattern2) |
                       df['DOM'].str.contains(pattern3, regex=True)|
                       df['DOM'].str.contains('-')|
                       df['DOM'].str.contains(pattern5, regex=True)|
                       df['DOM'].str.contains(pattern6, regex=True)|                   
                       df['KORP'].str.contains(pattern, regex=True)|
                       df['KORP'].str.contains(pattern4, regex=True)|
                       df['KORP'].str.contains(pattern7, regex=True)|
                       df['KORP'].str.contains(pattern6, regex=True)|
                       df['LITERA_DOM'].str.contains(pattern, regex=True)|
                       df['LITERA_DOM'].str.contains(pattern8, regex=True)|
                       df['LITERA_DOM'].str.contains(pattern4, regex=True)|
                       df['KV_new'].str.contains(pattern3, regex=True)|
                       df['KV_new'].str.contains(pattern, regex=True)|
                       df['KV_new'].str.contains(pattern7, regex=True)|
                       df['KV_new'].str.contains(pattern6, regex=True)
                )  ]                         

# Если в корпусе указана цифра, то она записывается в новую колонку, а если в корпусе буква, то она объединяется с домом, литерой дома
filtered_df = filtered_df.fillna('')
for i, row in filtered_df.iterrows():
    if re.fullmatch('\d', row['KORP']):
        filtered_df.loc [i,'KORP_new'] = row['KORP']
        filtered_df.loc [i,'house_new'] = str(row['DOM'])
    else: filtered_df.loc [i,'house_new'] = str(row['DOM']) + str(row['KORP']) + str(row['LITERA_DOM'])

# Преобразовываем тип объекта
filtered_df['OBJTYPE_NAME_new'] = filtered_df['OBJTYPE_NAME'].replace('ВРЕМЯНКА', 'д.')\
    .replace('ДАЧА', 'д.').replace('ДОМ БЛОКИРОВАННОЙ ЗАСТРОЙКИ', 'д.')\
    .replace('М/ДОМ', 'д.').replace('МНОГОКВАРТИРНОЕ СТРОЕНИЕ', 'д.')\
    .replace('ОБЩЕЖИТИЕ', 'д.').replace('ЧАСТНЫЙ ДОМ', 'д.').replace('ЧАСТНЫЙ ДОМ - КОТТЕДЖ', 'д.')
        

# Тип улицы преобразуем
filtered_df['TYPE_UL_new'] = filtered_df['TYPE_UL'].replace ('микр', 'мкр')     
        
#Замены после преобразований
filtered_df['KORP_new'] = filtered_df['KORP_new'].replace('n', '')

#Фильтрация данных на 2 части: дома и участки
filtered_df_land = filtered_df [filtered_df['OBJTYPE_NAME_new'] == 'УЧАСТОК']
filtered_df_house = filtered_df [filtered_df['OBJTYPE_NAME_new'] != 'УЧАСТОК']

filtered_df_land.to_excel('Участки.xlsx', index=False)
filtered_df_house.to_excel('Дома.xlsx', index=False)


# # Поиск данных

# ## Вариант 1. Для домов

# In[ ]:


for i, row in filtered_df_house.iloc [0:10000, :].iterrows():
    headers = {
                'accept': 'application/json',
                'master-token': '', #указать мастер-токен
                'Content-Type': 'application/json',
               }

    json_data = {
            'region': { 'name': 'Челябинская область'},
            'district': {'name': row ['REGION_NAME']},
            'city': { 'name': row['GOROD']},
            'city_settlement': { 'name': row['NASPUNKT']},
            'street': { 'name': row['UL'], 'type_name': row['TYPE_UL_new']},
            'house': { 'number': str(row['house_new']), 'type_name': row['OBJTYPE_NAME_new']},
            'building': { 'number': str(row['KORP_new'])},
            'flat': { 'number': str(row['KV_new'])},
            'room': {'number': str(row['LITERA_KV'])}
             }

    response = requests.post('https://fias-public-service.nalog.ru/api/spas/v2.0/SearchByParts', headers=headers, json=json_data)
    json = response.json()
    
    if json['error'] is not None:
        filtered_df_house.loc [i,'FIAS'] = 'Не найден ГАР' 
        filtered_df_house.loc [i,'error'] = json['error'] # текст ошибки
        filtered_df_house.loc [i,'description_inf'] = json['description'] #описание   
    else: 
        filtered_df_house.loc [i,'FIAS'] = json['address_item']['object_guid'] # код ГАР 
        filtered_df_house.loc [i,'full_name_address'] = json['address_item']['full_name'] # полная строка адреса (муниципальное деление)
        if json['address_item']['address_details'] is not None:
            filtered_df_house.loc [i,'cadastral_number'] = json['address_item']['address_details']['cadastral_number'] #кадастровый номер
        else: filtered_df_house.loc [i,'cadastral_number'] = 'отсутствует'
     
    time.sleep(0.02)   


# In[ ]:


# фильтр по ошибкам
filtered_df_house_error = filtered_df_house [filtered_df_house ['error'] !='nan']
# filtered_df_house_error = filtered_df_house [~filtered_df_house ['error'].str.contains('nan')]
filtered_df_house_error.to_excel('Дом_ошибки после обработки.xlsx', index=False)

# фильтр по обработанным строкам
filtered_df_house_done = filtered_df_house [
                                ~filtered_df_house ['FIAS'].str.contains('^$', regex=True) & 
                                ~filtered_df_house ['FIAS'].str.contains('Не найден ГАР') 
                                ]
filtered_df_house_done.to_excel('Дом_обработано.xlsx', index=False)

# фильтр по НЕ обработанным строкам
filtered_df_house_not_done = filtered_df_house [
                                filtered_df_house ['FIAS'].str.contains('^$', regex=True)]
filtered_df_house_not_done.to_excel('Дом_НЕ_обработано.xlsx', index=False)


# ## Вариант 2. Поиск по адресной строке

# In[ ]:


# Загрузка из таблицы
df = pd.read_excel ('', dtype = str) # вставить адрес расположения файла
df = df.fillna('')

df


# In[ ]:


# Составление адресной строки
for i, row in df.iterrows():
    if row['REGION_NAME'] != '':
        df.loc [i,'Address'] = row['REGION_NAME'] + ', ' + row['NASPUNKT'] + ', ' + row['TYPE_UL_new']\
            + ' ' + row['UL'] + ', ' + row['OBJTYPE_NAME_new'] + ', '+ row['house_new'] + ', ' + \
            row['KORP_new'] + ', '  + row['KV_new']
    else: df.loc [i,'Address'] = row['GOROD'] + ', ' + row['NASPUNKT'] + ', ' + row['TYPE_UL_new']\
            + ' ' + row['UL'] + ', ' + row['OBJTYPE_NAME_new'] + ', '+ row['house_new'] + ', '  \
            + row['KORP_new'] + ', '+ row['KV_new']
df


# In[ ]:


# Получаем из налоговой информацию по адресам. Может быть два адреса похожих по написанию. Из ответа налоговой получаем запись о двух адресах.
for i, row in df.iloc [0:10000, :].iterrows():
   
    headers = {
                'accept': 'application/json',
                 'master-token': '', # указать токен
                }
    params = {
            'search_string': row ['Address'],
            'address_type': '2', #Вид представления адреса: 1 - административное деление, 2 - муниципальное деление
             }
    response = requests.get('https://fias-public-service.nalog.ru/api/spas/v2.0/SearchAddressItems', 
                            params=params, headers=headers)
    json = response.json()    
    
    # Записываем первый найденный адрес   
    df.loc [i,'FIAS'] = json['addresses'][0]['object_guid'] # код ГАР 
    df.loc [i,'caddastral_number'] = json['addresses'][0]['address_details']['cadastral_number'] #кадастровый номер
    df.loc [i,'full_name_address'] = json['addresses'][0]['full_name'] # полная строка адреса (муниципальное деление)
    
    # Записываем второй найденный адрес
    df.loc [i,'FIAS_1'] = json['addresses'][1]['object_guid'] # код ГАР 
    df.loc [i,'caddastral_number_1'] = json['addresses'][1]['address_details']['cadastral_number'] #кадастровый номер
    df.loc [i,'full_name_address_1'] = json['addresses'][1]['full_name'] # полная строка адреса (муниципальное деление)
 
    time.sleep(0.02)
df


# In[ ]:


# фильтрация для записи в файл
filtered_df_not_done = df[df['FIAS'].str.contains('^$', regex=True)]
filtered_df_not_done.to_excel('НЕ обработан файл.xlsx', index=False)

filtered_df_done= df[~df['FIAS'].str.contains('^$', regex=True)]
filtered_df_done.to_excel('обработан файл.xlsx', index=False)

