# -*- coding: utf-8 -*-
"""
Created on Sat Oct  1 14:43:12 2022

@author: Vikesh Jain
"""

#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import pandas as pd
import numpy as np
import json
from oauth2client.service_account import ServiceAccountCredentials
from pyhive import presto
import psycopg2
from sqlalchemy import create_engine
import pygsheets as pg
from functools import reduce
import sys
import gspread
from datetime import datetime
from datetime import timedelta
import time
from dataplatform_azure import read_excel_sheet, write_excel_sheet, clear_excel_sheet
import pytz
from pytz import timezone

import requests
def createPrestoConnection():
    session=requests.Session()
    session.headers.update({'X-Trino-Password': 'Oyorooms@456'})
    try:
        conn = presto.Connection(requests_session = session,host="az-presto.oyorooms.io", port=8889, username='analytics_p0@weddingz.in')
        return conn
    except Exception as error:
        print("Unable to connect to Database")
        print(error)
        sys.exit(0)

presto_con = createPrestoConnection()
print("Connected")
excel_link = 'https://oyoenterprise-my.sharepoint.com/:x:/g/personal/shubham_kumar17_oyorooms_com/EXqQlZV2QHhAlOZz-DaoseABs0R7ek1BB7BWaa58jPjH_Q?email=vikesh.jain%40oyorooms.com&e=4%3A73uwcY&at=9&CID=867045a6-335a-c856-bf90-b9fe728e8d84'


today = datetime.utcnow()
formatted_date = today.strftime('%Y-%m-%d')


Raw = pd.DataFrame()
Raw = pd.read_sql_query(f"""
SELECT date(A.date1) as date,b.id as booking_id,b.invoice_no,b.check_in,b.check_out,date(created_at) as created_date,hs.oyo_id,hs.city_name,'India' as country_name,coalesce(c.prepaid_flag,0) as prepaid_flag,b.status,
case when b.source in (22,23,38) and b.micro_market_id is null then 'oyo'
                            when b.source in (17,21) and b.micro_market_id is null then 'oyo'
                            when b.source in (0,7)  and (b.micro_market_id is null OR b.micro_market_id = 99) then 'oyo'
                            when b.source = 4  and b.micro_market_id is null then 'oyo'
                            when b.source in (1,10,64)  and b.micro_market_id is null then 'OTA'
                            when  b.micro_market_id is not null and b.micro_market_id > 0 and b.micro_market_id != 99 then 'OBA'
                            else 'Walkin' end as booking_source, 
                            b.oyo_rooms as brns,case when b.status in (1,2)  then b.oyo_rooms else 0 end as urns,
                            gmv/(case when date_diff('day',b.check_in,b.check_out) = 0 then 1 else date_diff('day',b.check_in,b.check_out) end) as daily_gmv,
                            b.coupon_code,b.rc,created_at +interval '5' hour + interval '30' minute as lca,cancellation_time +interval '5' hour + interval '30' minute as lct,guest_id,Guest_Name,channel from 
                            
(select date_add('day',checkin,date'1970-01-01') as check_in,date_add('day',checkout,date'1970-01-01') as check_out,invoice_no,source,micro_market_id,id,after_booking_status,cancellation_reason,hotel_id,notes,status,oyo_rooms,FROM_UNIXTIME(cancellation_time/1000000) as "cancellation_time",FROM_UNIXTIME(updated_at/1000000) as "Update_Date",FROM_UNIXTIME(checkin_time/1000000) as "checkin_time",FROM_UNIXTIME(checkout_time/1000000) as "checkout_time",coupon_code,FROM_UNIXTIME(created_at/1000000) as "created_at",
--case when date(checkin) = date(checkout) then date(checkout) + interval '1' day else date(checkout) end as checkout1,
(coalesce(selling_amount,amount)-coalesce(discount,0)) as gmv,guest_id,ota_id
,case
when ota_id in (values 2,2002) then 'Agoda'
when ota_id in (values 2007) then 'EaseMyTrip'
when ota_id in (values 5,113,2005) then 'Booking.com'
when ota_id in (values 63) then 'GOMMT'
when ota_id in (values 3) then 'expedia'
when ota_id in (values 42,2048) then 'HotelBeds'
when ota_id in (values 272,30) then 'Cleartrip'
when ota_id in (values 4) then 'Yatra'
when micro_market_id is not null and micro_market_id > 0 and micro_market_id not in (306,126,256,99) then 'MM'
else 'REST'
end rc,0 as Guest_Name,split_part(external_reference_id, '_', 1) as channel
--(case when date(checkout) = date(checkin) then date(cast(checkout as date)+interval '1' day) else date(checkout) end) as checkout_v2,
--date_diff('day',cast(checkin as date),cast(checkout as date)) as r
FROM ingestiondb_service.bookings
WHERE status in (values 0,1,2,3,4,12) and inserted_at >= '202301' --and id= 264992750
) b 
INNER JOIN
(SELECT DISTINCT date(cal.date) as date1 FROM ingestiondb.calendar as cal
WHERE date(cal.date) BETWEEN date('2023-05-01') AND date '{formatted_date}'
)A
ON A.date1 >= b.check_in and A.date1 < b.check_out
inner join 
(select hotel_id,oyo_id,city_name,state_name from aggregatedb.hotels_summary
where country_id in (values 1)
and oyo_id in ('AHM527','ZKR082','JAI890','VAD217','VAD281','MRB018'))hs
on hs.hotel_id = b.hotel_id
left join
(select distinct(p.booking_id) as booking_id, 1 as prepaid_flag
from (select id,merchant,status,is_visible,booking_id,payment_type,amount from ingestiondb_service.payment_transacs p where inserted_at >= '202301') p
where p.merchant not in (values 0,1,2,17,35,34,37,45,47,48,49,50,67,73,44,6,46,56,42,16,65,31,57) and p.status= 1 and p.is_visible = TRUE)
C ON b.id=C.booking_id
""", presto_con)
print("Successfully executed")

clear_excel_sheet(url = excel_link , sheet_name = 'mountania 2021', range_address = 'A135723:Z300000')
print ("successfully  Data clear");
write_excel_sheet( url = excel_link, sheet_name = 'mountania 2021', df = Raw, location = 'A135723', chunk_size=2000)
print ("successfully Data updated");
