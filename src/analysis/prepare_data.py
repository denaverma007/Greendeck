import pandas as pd
from flask import jsonify

from src.exception.exception import BaseError


class prepare_dataset:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None
        self.simi_list = []

    def load_file(self):
        self.df = pd.read_json(self.file_path, lines=True, orient='columns')

    def make_attr(self):
        self.df['_id'] = self.df['_id'].apply(lambda x: x['$oid'])
        self.df['brand.name'] = self.df['brand'].apply(lambda x: x['name'])
        self.df['rank'] = self.df['positioning'].apply(lambda x: None if isinstance(x, float) else x['rank'])
        self.df['stock_availability'] = self.df['stock'].apply(lambda x: x['available']).astype(int)
        self.df['offer_price'] = self.df['price'].apply(lambda x: x['offer_price']['value'])
        self.df['regular_price'] = self.df['price'].apply(lambda x: x['regular_price']['value'])
        self.df['basket_price'] = self.df['price'].apply(lambda x: x['basket_price']['value'])
        self.df['discount'] = (abs(self.df['regular_price'] - self.df['offer_price']) / self.df['regular_price']) * 100
        self.df.drop(['created_at', 'description_text', 'lv_url', 'media', 'meta', 'positioning',
                      'price', 'price_changes', 'price_positioning', 'sizes', 'sku', 'spider', 'stock',
                      "url", "website_id", 'updated_at', 'classification', 'name', 'brand', 'price_positioning_text'],
                     axis=1, inplace=True)

    def extract_similar_products(self, data):
        if isinstance(data, float) or data is None:
            return None
        data = data['website_results']
        res = {}
        for key in data.keys():
            try:
                if "_source" in data[key].keys():
                    res[key] = data[key]['_source']
                elif "knn_items" in data[key].keys() and "_source" in data[key]['knn_items'][0].keys():
                    res[key] = data[key]['knn_items'][0]['_source']
            except:
                continue
        return res

    def extract_detail(self, data):
        if data[1] is None or isinstance(data[1], float):
            return
        ids, data = data[0], data[1]
        for key in data.keys():
            self.simi_list.append({'_id': ids, "competitor": key, "cp_brand": data[key]['brand']['name'],
                                   'cp_offer_price': data[key]['price']['offer_price']['value'],
                                   'cp_regular_price': data[key]['price']['regular_price']['value'],
                                   'cp_basket_price': data[key]['price']['basket_price']['value']})

    def preprocess(self):
        self.load_file()
        self.make_attr()
        temp = self.df[['_id', 'similar_products']].copy()
        temp['similar_prod_dict'] = temp['similar_products'].apply(self.extract_similar_products)
        temp.loc[temp['similar_prod_dict'] == {}] = None
        temp[["_id", "similar_prod_dict"]].apply(self.extract_detail, axis=1)
        simi_df = pd.DataFrame(self.simi_list)
        simi_df['cp_discount'] = (abs(simi_df['cp_offer_price'] - simi_df['cp_regular_price'])
                                  / simi_df['cp_regular_price']) * 100
        self.df = self.df.merge(simi_df, how='left', on='_id')
        self.df.drop("similar_products", axis=1, inplace=True)
        self.df['discount_diff'] = (abs(self.df['basket_price'] - self.df['cp_basket_price']) / self.df[
            'basket_price']) * 100

        return self.df.copy()


def utilWrapper(input_function):
    """functionality to handle exception and response"""
    def common_function(*args, **kwargs):
        try:
            return jsonify({'error': False,
                            'result': input_function(*args, **kwargs)}), 200
        except BaseError as e:
            return jsonify({'error': True,
                            'message': "Failed to process request, try again.",
                            'error_message': e._message}), e._code
        except Exception as e:
            return jsonify({'error': True,
                            'message': "Failed to process request, try again.",
                            'error_message': str(e)}), 500

    return common_function
