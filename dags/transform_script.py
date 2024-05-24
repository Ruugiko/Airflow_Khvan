import pandas as pd


def transform(profit_table, date, product):
    """ Собирает таблицу флагов активности по продуктам
        на основании прибыли и количеству совершёных транзакций
        
        :param profit_table: таблица с суммой и кол-вом транзакций
        :param date: дата в формате 'YYYY-MM-DD'
        :param product: название продукта (буква от 'a' до 'j')
        :return df_tmp: pandas-датафрейм флагов за указанную дату
    """
    start_date = pd.to_datetime(date) - pd.DateOffset(months=2)
    end_date = pd.to_datetime(date) + pd.DateOffset(months=1)
    date_list = pd.date_range(start=start_date, end=end_date, freq='M').strftime('%Y-%m-01')

    df_tmp = profit_table[profit_table['date'].isin(date_list)].drop('date', axis=1).groupby('id').sum()

    df_tmp[f'flag_{product}'] = df_tmp.apply(
        lambda x: x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0, axis=1).astype(int)

    return df_tmp.filter(regex=f'flag_{product}').reset_index()

if __name__ == "__main__":
    profit_data = pd.read_csv('profit_table.csv')
    product = 'a'  # пример для одного продукта
    flags_activity = transform(profit_data, '2024-03-01', product)
    flags_activity.to_csv(f'flags_activity_{product}.csv', index=False)