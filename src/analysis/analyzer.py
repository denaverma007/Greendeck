
import os
import gdown as gdown
from src.analysis.prepare_data import prepare_dataset
from src.exception.exception import BaseError


class BaseAnalyzer:
    global dataframe

    def init_files(self, dump_path='resources/netaporter.json'):
        url = 'https://drive.google.com/a/greendeck.co/uc?id=19r_vn0vuvHpE-rJpFHvXHlMvxa8UOeom&export=download'
        if dump_path.split('/')[0] not in os.listdir():
            os.mkdir(dump_path.split('/')[0])
        if os.path.exists(dump_path):
            pass
        else:
            gdown.download(url=url, output=dump_path, quiet=False)
    def prepare_data(self):
        global dataframe
        obj = prepare_dataset('resources/netaporter.json')
        df = obj.preprocess()
        dataframe = df

    def process_request(self, query):
        """ process request"""
        global dataframe
        validate_query(query)
        query['query_type'] = query['query_type'].split("|")[0]
        res = globals()[query['query_type']](query.get('filters', []), dataframe)
        return res


def inp_operation(inp, df):
    """filtering according to operand"""
    if inp['operator'] == ">":
        res = df.loc[df[inp['operand1']] > inp['operand2']].copy()
    elif inp['operator'] == "<":
        res = df.loc[df[inp['operand1']] < inp['operand2']].copy()
    else:
        res = df.loc[df[inp['operand1']] == inp['operand2']].copy()
    res = res.drop_duplicates(subset=['_id'])
    return res


def validate(res):
    if res.shape[0] == 0:
        return True
    return False


def discounted_products_list(inp, df):
    inp = inp[0]
    res = inp_operation(inp, df)
    if validate(res):
        return {"discounted_products_list": "No Matching products found with this operation!"}

    return {"discounted_products_list": res['_id'].to_list()}


def discounted_products_count(inp, df):
    inp = inp[0]
    res = inp_operation(inp, df)
    if validate(res):
        return {"discounted_products_count|discount_avg": "No Matching products found with this operation!"}
    return {"products_count": str(res['_id'].count()), "discount_avg": str(res['discount'].mean())}


def expensive_list(inp, df):
    if len(inp) == 0:
        res = list(df.loc[df['basket_price'] > df['cp_basket_price']]['_id'].unique())
    else:
        inp = inp[0]
        res = inp_operation(inp, df)
        if validate(res):
            return {"expensive_list": "No Matching products found with this operation!"}
        res = res.loc[res['basket_price'] > res['cp_basket_price']]['_id'].to_list()
    return {"expensive_list": res}


def competition_discount_diff_list(inp, df):
    f1, f2 = inp[0], inp[1]
    if f1["operand1"] != 'discount_diff':
        f1, f2 = f2, f1
    res = inp_operation(f1, df)
    res = res.loc[res['cp_id'] == f2['operand2']]["_id"].to_list()
    if len(res) == 0:
        raise {"competition_discount_diff_list": "No Matching products found with this operation!"}
    return {"competition_discount_diff_list": res}


def validate_query(query):
    """validate request body"""
    if 'query_type' not in query:
        raise BaseError(message='query_type is requied property', code=400)
    val_ls = ['discounted_products_list', 'discounted_products_count|avg_discount', 'expensive_list',
              'competition_discount_diff_list']
    if query['query_type'] not in val_ls or query['query_type'] is None:
        raise BaseError(message="Enter a Valid query_type", code=400)
    elif query['filters'] is None or (len(query['filters']) == 0 and query['query_type'] != "expensive_list"):
        raise BaseError(message="Enter Valid Filters", code=400)
