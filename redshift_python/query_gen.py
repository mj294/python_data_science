import psycopg2
import os
import pandas as pd
import numpy as np

class query_gen(object):
    def __init__(self, retailer_id, month_range=3):
        ''' Base Query defines the subset of customers we analyzer '''
        self.con=psycopg2.connect(dbname='ibotta', host=os.environ['REDSHIFT_ENDPOINT'],
        port='5439', user=os.environ['LOOKER_USER'], password=os.environ['LOOKER_PASS'])
        self.retailer_id = retailer_id
        self.month_range = month_range

        self.base_query = '''
            with a as (SELECT customer_id
                      FROM receipts as a
                      INNER JOIN customers as b on a.customer_id = b.id
                      AND dateadd(month,{1},a.updated_at) >= CURRENT_DATE
                      AND b.roles_str = 'default'
                      AND a.total < 300
                      AND a.total IS NOT NULL
                      GROUP BY customer_id
                      HAVING (SUM(CASE WHEN RETAILER_ID = {0} THEN 1 ELSE 0 END) / {1}::float BETWEEN 0.5 AND 1.5) AND
                      SUM(CASE WHEN RETAILER_ID = {0} THEN 1 ELSE 0 END) < SUM(1)
                      )
        '''.format(retailer_id, month_range)

    def run_retailer_id_query(self):
        ''' Create lookup table for retailer ids'''

        query = ''' SELECT DISTINCT id, name FROM retailers ORDER BY 1, 2; '''

        return pd.read_sql_query(query, self.con)

    def run_competitor_query(self):
        ''' Look at basket size etc for other retailers they shop at '''

        query = self.base_query + '''
        SELECT c.name,
        COUNT(DISTINCT a.customer_id) as n_customers,
        SUM(b.n_receipts) AS n_receipts,
        AVG(b.n_receipts) / {0} AS avg_trips_month,
        AVG(b.tot_receipts) / {0} AS avg_total_month,
        AVG(b.tot_items) / {0} AS avg_items_month,
        AVG(b.tot_per_trip) AS avg_tot_per_trip,
        AVG(b.items_per_trip) AS avg_items_per_trip
        FROM a
        INNER JOIN (
                SELECT customer_id, retailer_id, SUM(1)::float AS n_receipts, SUM(total) AS tot_receipts,
                SUM(n_items) AS tot_items, AVG(total) AS tot_per_trip, AVG(n_items) AS items_per_trip
                FROM receipts AS a
                INNER JOIN
                (SELECT receipt_id, SUM(1) AS n_items
                FROM fact_customer_receipt_item_product_details
                GROUP BY receipt_id)
                AS b ON a.id = b.receipt_id
                WHERE dateadd(month,{0},updated_at) >= CURRENT_DATE AND total < 300 AND total IS NOT NULL
                GROUP BY customer_id, retailer_id
                ) AS b on a.customer_id = b.customer_id
        INNER JOIN retailers AS c ON b.retailer_id = c.id
        GROUP BY name
        ORDER BY n_customers DESC
        ;
        '''.format(self.month_range)

        return pd.read_sql_query(query, self.con)

    def run_categories_query(self):
        ''' Look at most common categories purchased from for other retailers'''

        query = self.base_query + '''
        SELECT d.name as retailer_name, e.name as cat_name, SUM(1) AS n_items
        FROM a
        INNER JOIN (
                SELECT customer_id, id
                FROM receipts
                WHERE dateadd(month,{0},updated_at) >= CURRENT_DATE AND total < 300 AND total IS NOT NULL
                GROUP BY customer_id, id
              ) AS b ON a.customer_id = b.customer_id
        INNER JOIN fact_customer_receipt_item_product_details AS c ON b.id = c.receipt_id
        INNER JOIN retailers AS d ON c.retailer_id = d.id
        INNER JOIN product_categories AS e ON c.primary_category_id = e.id
        GROUP BY d.name, e.name
        ORDER BY retailer_name, n_items desc
        ;
        '''.format(self.month_range)

        return pd.read_sql_query(query, self.con)

    def run_index_analysis_query(self):
        ''' Find most and least favored categories for retailer of interest '''

        query = self.base_query + '''
        SELECT e.name as cat_name,
        SUM(1) AS n_receipt_items,
        CASE WHEN d.id = {0} THEN 1 ELSE 0 END AS interest_flag
        FROM a
        INNER JOIN (
                SELECT customer_id, id
                FROM receipts
                WHERE dateadd(month,{1},updated_at) >= CURRENT_DATE AND total < 300 AND total IS NOT NULL
                AND (validation_type = 'tlog' OR retailer_id = {0})
                GROUP BY customer_id, id
              ) AS b ON a.customer_id = b.customer_id
        INNER JOIN fact_customer_receipt_item_product_details AS c ON b.id = c.receipt_id
        INNER JOIN retailers AS d ON c.retailer_id = d.id
        INNER JOIN product_categories AS e ON c.primary_category_id = e.id
        GROUP BY cat_name, interest_flag
        ORDER BY interest_flag, n_receipt_items
        ;
        '''.format(self.retailer_id, self.month_range)

        return pd.read_sql_query(query, self.con)

if __name__ == '__main__':
    pass
