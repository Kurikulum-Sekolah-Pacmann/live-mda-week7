from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from datetime import timedelta
from pipeline_warehouse.task.extract import Extract

import pandas as pd
import pytz
import requests
import logging

class Transform:
    """
    A class used to transform data.
    """

    @staticmethod
    def _dim_product(**kwargs) -> pd.DataFrame:

        try:
            products = Extract._kafka(topic='source.production.products', **kwargs)
            brand = Extract._kafka(topic='source.production.brands', **kwargs)
            categories = Extract._kafka(topic='source.production.categories', **kwargs)
            
           
        except Exception as e:
            raise AirflowException(f"Error: {str(e)}")

        if not products:
            raise AirflowSkipException("No data found. Skipping...")
        else:
             # get "payload"
            df_products = pd.json_normalize([msg['payload'] for msg in products])
            df_brands = pd.json_normalize([msg['payload'] for msg in brand])
            df_categories = pd.json_normalize([msg['payload'] for msg in categories])

            # Join dengan brand
            df = df_products.merge(df_brands[['brand_id', 'brand_name']], on='brand_id', how='left')

            # Join dengan category
            df = df.merge(df_categories[['category_id', 'category_name']], on='category_id', how='left')

            # Rename kolom sesuai warehouse
            df.rename(columns={
                'product_id': 'product_nk',
            }, inplace=True)

            df['updated_at'] = pd.Timestamp.now()

            # Pilih kolom sesuai dengan skema warehouse
            df_warehouse = df[[
                'product_nk', 'product_name', 'brand_name', 'category_name',
                'model_year', 'list_price', 'updated_at'
            ]].drop_duplicates(subset=['product_nk'])

            logging.info(f"Starting to consume from topic: {df_warehouse}")

            return df_warehouse
    
    @staticmethod
    def _dim_customer(**kwargs) -> pd.DataFrame:
        """
        Transform customer data.
        """
        try:
            customer = Extract._kafka(topic='source.sales.customers', **kwargs)
        except Exception as e:
            raise AirflowException(f"Error: {str(e)}")

        if not customer:
            raise AirflowSkipException("No data found. Skipping...")
        else:
            #msg to dataframe
            df_customers = pd.json_normalize([msg['payload'] for msg in customer])
            # Rename kolom sesuai warehouse
            df = df_customers.rename(columns={
                    'customer_id': 'customer_nk'
                })
            df['updated_at'] = pd.Timestamp.now()   
            df_dim_customer = df[[
                'customer_nk', 'first_name', 'last_name', 'phone',
                'email', 'street', 'city', 'state', 'zip_code',
                 'updated_at'
            ]].drop_duplicates(subset=['customer_nk'])

            return df         
        
    @staticmethod
    def _dim_store(**kwargs) -> pd.DataFrame:
        """
        Transform store data.
        """
        try:
            stores = Extract._kafka(topic='source.sales.stores', **kwargs)
           
        except Exception as e:
            raise AirflowException(f"Error: {str(e)}")

        if not stores:
            raise AirflowSkipException("No data found. Skipping...")
        else:
            df_stores = pd.json_normalize([msg['payload'] for msg in stores])
            # Rename kolom sesuai warehouse
            df = df_stores.rename(columns={
                    'store_id': 'store_nk'
                })
            df['updated_at'] = pd.Timestamp.now()   
            df_dim_store = df[[
                    'store_nk', 'store_name', 'phone', 'email',
                    'street', 'city', 'state', 'zip_code',
                    'updated_at'
                ]].drop_duplicates(subset=['store_nk'])
            return df_dim_store

    @staticmethod
    def _dim_staff(**kwargs) -> pd.DataFrame:
        """
        Transform staff data.
        """
        try:
            staffs = Extract._kafka(topic='source.sales.staffs', **kwargs)
        except Exception as e:
            raise AirflowException(f"Error: {str(e)}")

        if not staffs:
            raise AirflowSkipException("No data found. Skipping...")
        else:
            df_staffs = pd.json_normalize([msg['payload'] for msg in staffs])
            # Rename kolom sesuai warehouse
            df_staffs = df_staffs.rename(columns={
                    'staff_id': 'staff_nk'
                })
            df_staffs['updated_at'] = pd.Timestamp.now()   
            df_dim_staff = df_staffs[[
                'staff_nk', 'first_name', 'last_name', 'email', 'phone',
                'active', 'manager_id', 'created_at', 'updated_at'
            ]].drop_duplicates(subset=['staff_nk'])

            return df_dim_staff
    
    @staticmethod
    def _fact_order(**kwargs) -> pd.DataFrame:
        """
        Transform order and order_items into fact_order schema.
        """
        try:
            orders = Extract._kafka(topic='source.sales.orders', **kwargs)
            order_items = Extract._kafka(topic='source.sales.order_items', **kwargs)
        except Exception as e:
            raise AirflowException(f"Error during extraction: {str(e)}")

        if not orders or not order_items:
            raise AirflowSkipException("No data found. Skipping...")
        else:
            try:
                df_orders = pd.json_normalize([msg['payload'] for msg in orders])
                df_order_items = pd.json_normalize([msg['payload'] for msg in order_items])

                # Lookup tables
                dim_date = Extract._dwh(schema='public', table_name='dim_date', **kwargs)
                dim_product = Extract._dwh(schema='public', table_name='dim_product', **kwargs)
                dim_store = Extract._dwh(schema='public', table_name='dim_store', **kwargs)
                dim_staff = Extract._dwh(schema='public', table_name='dim_staff', **kwargs)
                dim_customer = Extract._dwh(schema='public', table_name='dim_customer', **kwargs)

                # Merge order_items + orders
                df = pd.merge(df_order_items, df_orders, on='order_id', how='left')

                # Map FK fields using natural keys
                df['product_id'] = df['product_id'].map(dict(zip(dim_product['product_nk'], dim_product['product_id'])))
                df['store_id'] = df['store_id'].map(dict(zip(dim_store['store_nk'], dim_store['store_id'])))
                df['staff_id'] = df['staff_id'].map(dict(zip(dim_staff['staff_nk'], dim_staff['staff_id'])))
                df['customer_id'] = df['customer_id'].map(dict(zip(dim_customer['customer_nk'], dim_customer['customer_id'])))

                # Convert numeric fields
                df['list_price'] = pd.to_numeric(df['list_price'], errors='coerce')
                df['discount'] = pd.to_numeric(df['discount'], errors='coerce').fillna(0)

                # Convert integer date to datetime
                for col in ['order_date', 'required_date', 'shipped_date']:
                    df[col + '_dt'] = pd.to_datetime('1970-01-01') + pd.to_timedelta(df[col], unit='D')
                    df[col + '_str'] = df[col + '_dt'].dt.strftime('%Y-%m-%d')

                # Map date_id from dim_date
                date_map = dict(zip(dim_date['date_actual'].astype(str), dim_date['date_id']))
                for col in ['order_date', 'required_date', 'shipped_date']:
                    df[col + '_id'] = df[col + '_str'].map(date_map)

                # Add timestamps
                now = pd.Timestamp.now()
                df['created_at'] = now
                df['updated_at'] = now

                # Final projection & renaming
                df_fact = df[[
                    'order_id',
                    'item_id',
                    'customer_id',
                    'store_id',
                    'staff_id',
                    'product_id',
                    'order_status',
                    'order_date_id',
                    'required_date_id',
                    'shipped_date_id',
                    'quantity',
                    'list_price',
                    'discount',
                    'created_at',
                    'updated_at'
                ]].rename(columns={
                    'order_id': 'order_nk',
                    'item_id': 'item_nk',
                    'order_date_id': 'order_date',
                    'required_date_id': 'required_date',
                    'shipped_date_id': 'shipped_date'
                })

                return df_fact

            except Exception as e:
                raise AirflowException(f"Transformation error: {str(e)}")