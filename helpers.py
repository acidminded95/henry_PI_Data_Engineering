import pandas as pd

def extraccion_tabla_hecho(dataset, path, dict):
    if dataset[1] in ['xlsx', 'xls']:
            xl_dict = pd.read_excel(path, sheet_name=None, date_parser=None, engine="xlrd")
            for sheet in xl_dict:
                name = f'{sheet[-8:-4]}-{sheet[-4:-2]}-{sheet[-2:]}'
                dict[name] = pd.DataFrame(xl_dict[sheet])
    else:
        name = f'{dataset[0][-8:-4]}-{dataset[0][-4:-2]}-{dataset[0][-2:]}'
        if dataset[1] == 'csv':
            try:
                dict[name] = pd.read_csv(path, encoding='UTF-16 LE')
            except UnicodeDecodeError:
                pass
        elif dataset[1] == 'json':
            dict[name] = pd.read_json(path)
        elif dataset[1] == 'txt':
            dict[name] = pd.read_csv(path, delimiter='|')