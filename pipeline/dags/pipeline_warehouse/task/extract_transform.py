from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from datetime import timedelta
from pipeline_warehouse.task.extract import Extract

import pandas as pd
import pytz
import requests

class Transform:
    """
    A class used to transform data.
    """

    @staticmethod
    def _dim_product(**kwargs) -> pd.DataFrame:

        try:
            df_products = Extract._kafka(topic='source.production.products', **kwargs)
            df_brands = Extract._kafka(topic='source.production.brands', **kwargs)
            df_categories = Extract._kafka(topic='source.production.categories', **kwargs)
            
           
        except Exception as e:
            raise AirflowException(f"Error: {str(e)}")

        if df_products.empty:
            raise AirflowSkipException(f"Dataframe for 'products' is empty. Skipped...")
        else:
             # get "payload"
            df_products = pd.json_normalize(df_products['payload'])
            df_brands = pd.json_normalize(df_brands['payload'])
            df_categories = pd.json_normalize(df_categories['payload'])

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
            raise AirflowSkipException(f"Dataframe for 'customers' is empty. Skipped...")
        else:
            #msg to dataframe
            df_customers = pd.json_normalize(customer, record_path=None, meta=None)[['payload']]
            df_customers = pd.json_normalize(df_customers['payload'])
            # Rename kolom sesuai warehouse
            df = df_customers.rename(columns={
                    'customer_id': 'customer_nk'
                })
            df['updated_at'] = pd.Timestamp.now()   
            # df_dim_customer = df[[
            #     'customer_nk', 'first_name', 'last_name', 'phone',
            #     'email', 'street', 'city', 'state', 'zip_code',
            #      'updated_at'
            # ]].drop_duplicates(subset=['customer_nk'])

            return df         
        
    @staticmethod
    def _dim_store(**kwargs) -> pd.DataFrame:
        """
        Transform store data.
        """
        try:
            df_stores = Extract._kafka(topic='source.sales.stores', **kwargs)
           
        except Exception as e:
            raise AirflowException(f"Error: {str(e)}")

        if df_stores.empty:
            raise AirflowSkipException(f"Dataframe for 'stores' is empty. Skipped...")
        else:
            df_stores = pd.json_normalize(df_stores['payload'])
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
            df_staffs = Extract._kafka(topic='source.sales.staffs', **kwargs)
        except Exception as e:
            raise AirflowException(f"Error: {str(e)}")

        if df_staffs.empty:
            raise AirflowSkipException(f"Dataframe for 'staffs' is empty. Skipped...")
        else:
            df_staffs = pd.json_normalize(df_staffs['payload'])
            # Rename kolom sesuai warehouse
            df = df_staffs.rename(columns={
                    'staff_id': 'staff_nk'
                })
            df['updated_at'] = pd.Timestamp.now()   
            df_dim_staff = df[[
                    'staff_nk', 'first_name', 'last_name', 'phone',
                    'email', 'street', 'city', 'state', 'zip_code',
                    'updated_at'
                ]].drop_duplicates(subset=['staff_nk'])
            return df_dim_staff
    
    @staticmethod
    def _fact_order(**kwargs) -> pd.DataFrame:
        """
        Transform order data to match fact_order schema.
        """
        try:
            df_orders = Extract._kafka(topic='source.sales.orders', **kwargs)
            df_order_items = Extract._kafka(topic='source.sales.order_items', **kwargs)

        except Exception as e:
            raise AirflowException(f"Error: {str(e)}")

        if df_orders.empty:
            raise AirflowSkipException(f"Dataframe for 'orders' is empty. Skipped...")
        else:
            df_orders = pd.json_normalize(df_orders['payload'])
            df_order_items = pd.json_normalize(df_order_items['payload'])
            # Lookup tables
            
            dim_date = Extract._dwh(schema='public', table_name='dim_date', **kwargs)
            dim_product = Extract._dwh(schema='public', table_name='dim_product', **kwargs)
            dim_store = Extract._dwh(schema='public', table_name='dim_store', **kwargs)
            dim_staff = Extract._dwh(schema='public', table_name='dim_staff', **kwargs)
            dim_customer = Extract._dwh(schema='public', table_name='dim_customer', **kwargs)
            try:
                # Merge order_items + orders
                df = pd.merge(df_order_items, df_orders, on='order_id', how='left')

                # Map product_id
                product_map = dict(zip(dim_product['product_nk'], dim_product['product_id']))
                df['product_id'] = df['product_id'].map(product_map)

                # Map store_id
                store_map = dict(zip(dim_store['store_nk'], dim_store['store_id']))
                df['store_id'] = df['store_id'].map(store_map)

                # Map staff_id
                staff_map = dict(zip(dim_staff['staff_nk'], dim_staff['staff_id']))
                df['staff_id'] = df['staff_id'].map(staff_map)

                # Map customer_id
                customer_map = dict(zip(dim_customer['customer_nk'], dim_customer['customer_id']))
                df['customer_id'] = df['customer_id'].map(customer_map)

                # Convert and map order_date, required_date, shipped_date
                for col in ['order_date', 'required_date', 'shipped_date']:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df[col + '_str'] = df[col].dt.strftime('%Y-%m-%d')
                    date_map = dict(zip(dim_date['date_actual'].astype(str), dim_date['date_id']))
                    df[col + '_id'] = df[col + '_str'].map(date_map)

                # Timestamp
                now = pd.Timestamp.now()
                df['created_at'] = now
                df['updated_at'] = now

                # Final select
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
                raise AirflowException(f"Error: {str(e)}")


