# coding=utf-8

from configs import truckerpath_eld_config as config
import pandas as pd
import psycopg2
import datetime
import copy
import os


now_end = datetime.datetime.now()
now_end_str = datetime.datetime.strftime(now_end, '%Y-%m-%d')

now = now_end + datetime.timedelta(days=-1)
now_str = datetime.datetime.strftime(now, '%Y-%m-%d')

now_before_7days = now + datetime.timedelta(days=-7)
now_before_7days_str = datetime.datetime.strftime(now_before_7days, '%Y-%m-%d')

cur = None
conn = None


def connect_to_database():
    global conn,cur
    success = False
    while success == False:
        if conn == None or cur == None:
            conn_success = False
            while conn_success == False:
                try:
                    conn = psycopg2.connect(host=config.database_config['host'],
                                           user=config.database_config['username'],
                                           password=config.database_config['password'],
                                           database=config.database_config['db']
                                           )
                except Exception:
                    conn_success = False
                else:
                    conn_success = True
        try:
            cur = conn.cursor()
            cur.execute('select * from vt_user limit 1')
        except Exception as e:
            conn = None
            cur = None
            success = False
        else:
            success = True



def get_user(csv_path):
    user_cols = ['id', 'first_name', 'last_name', 'email']
    sql = '''
        select {} from vt_user;
    '''.format(reduce(lambda x, y: x + ',' + y, user_cols))

    cur.execute(sql)
    sql_return = cur.fetchall()
    user_df = pd.DataFrame(sql_return, columns=user_cols)

    user_df.to_csv(csv_path)

    return user_df

def get_history(csv_path):
    history_cols = ['user_id', 'event_type', 'create_date', 'physical_odometer_km', 'odometer_km']
    sql = '''
        select {} from driver_history where TO_CHAR(create_date, 'yyyy-MM-dd') >= '{}' and TO_CHAR(create_date, 'yyyy-MM-dd') <= '{}';
    '''.format(reduce(lambda x, y: x + ',' + y, history_cols), now_before_7days_str, now_end_str)
    cur.execute(sql)
    sql_return = cur.fetchall()
    history_df = pd.DataFrame(sql_return, columns=history_cols)

    history_df.to_csv(csv_path)

    return history_df


class GenerateExceptionUserCsv(object):

    def _fun_if_unusual(self, x):
        according = x.split('||')
        physical_odometer_km = according[0]
        odometer_km = according[1]
        if physical_odometer_km == '':
            return 1 if float(odometer_km) <= 0 else 0
        return 1 if float(physical_odometer_km) <= 0 else 0


    def _cal_proportion(self, x):
        logs_number = x.split('||')[0]
        unusual_number = x.split('||')[1]

        return str(round(float(unusual_number) / float(logs_number), 2) * 100) + '%' if float(logs_number) != 0 else '-'


    def _process_add_unusual_according(self):
        self.history_df['if_unusual_according'] = self.history_df.physical_odometer_km.apply(
            lambda x: str(x) if x == x else '') + self.history_df.odometer_km.apply(lambda x: '||' + str(x))


    def _process_add_unusual_flag(self):
        self.history_df['if_unusual'] = self.history_df.if_unusual_according.apply(lambda x: self._fun_if_unusual(x))
        self.history_cp_df = copy.deepcopy(self.history_df)


    def _process_add_logs_unusual_number(self):
        self.history_df = self.history_df.groupby(['user_id'], as_index=True).apply(
            lambda x: pd.Series({
                'logs_number': int(len(x.user_id)),
                'unusual_number': int(sum(x.if_unusual))
            })
        ).reset_index()


    def _combine_history_user_df(self):
        self.combine_df = self.history_df.merge(self.user_df, how='left', left_on='user_id', right_on='id')


    def _process_combine_df_add_proportion_according(self):
        self.combine_df['proportion_according'] = self.combine_df.logs_number.apply(
            lambda x: str(x)) + self.combine_df.unusual_number.apply(lambda x: '||' + str(x))


    def _process_combine_df_add_proportion(self):
        self.combine_df['proportion'] = self.combine_df.proportion_according.apply(lambda x: self._cal_proportion(x))


    def _unusual_df_generate(self):
        self.unusual_df = self.history_cp_df[self.history_cp_df.if_unusual.apply(lambda x: x == 1)]


    def _process_combine_df_merge_usual_df(self):
        self.combine_f_df = self.combine_df.merge(self.unusual_df, how='left', on=['user_id'])
        self.combine_f_df = self.combine_f_df.sort_values(by='user_id', ascending=[1])
        self.combine_f_df['event_type'] = self.combine_f_df.event_type.apply(lambda x: x if x == x else '-')

        self.combine_f_df = self.combine_f_df.groupby(['user_id', 'event_type'], as_index=False).apply(lambda x: pd.Series({
            'email': max(x.email),
            'first_name': max(x.first_name),
            'last_name': max(x.last_name),
            'logs_number': int(max(x.logs_number)),
            'unusual_number': int(max(x.unusual_number)),
            'unusual_proportion': max(x.proportion),

            'event_number': len(x.event_type) if max(x.event_type) != '-' else '-',
            'event_proportion': '-' if max(x.event_type) == '-' else str(
                round(len(x.event_type) / float(max(x.logs_number)), 2) * 100) + '%'
        })).reset_index()


    def _sorted_columns_of_combine_f_df(self):
        self.combine_f_df = self.combine_f_df[
            ['email', 'first_name', 'last_name', 'logs_number', 'unusual_number', 'unusual_proportion', 'event_type',
             'event_number', 'event_proportion']]


    def _rename_columns_of_combine_f_df(self):
        self.combine_f_df = self.combine_f_df.rename(columns={
            'email': 'Email',
            'first_name': 'First name',
            'last_name': 'Last name',
            'logs_number': 'Logs',
            'unusual_number': 'Exceptions',
            'unusual_proportion': 'Exceptions/Logs',
            'event_type': 'Exceptions type',
            'event_number': 'Exceptions type num',
            'event_proportion': 'Exceptions Type/Logs'
        })


    def __init__(self, csv_path, user_df, history_df):
        self.csv_path = csv_path
        self.user_df = user_df
        self.history_df = history_df
        self.columns = ['Email', 'First name', 'Last name', 'Logs', 'Exceptions', 'Exceptions/Logs', 'Exceptions type', 'Exceptions type num', 'Exceptions Type/Logs']
        self.history_cp_df = None
        self.combine_df = None
        self.unusual_df = None
        self.combine_f_df = None


    def pre_process_history_df(self):
        if not self.history_df.shape[0]:
            self.combine_f_df = pd.DataFrame([['-', '-', '-', 0, 0, '-', '-', 0, '-']], columns=self.columns)
            return

        self._process_add_unusual_according()
        self._process_add_unusual_flag()
        self._process_add_logs_unusual_number()

        self._combine_history_user_df()
        self._process_combine_df_add_proportion_according()
        self._process_combine_df_add_proportion()

        self._unusual_df_generate()

        self._process_combine_df_merge_usual_df()
        self._sorted_columns_of_combine_f_df()
        self._rename_columns_of_combine_f_df()

    def generate_csv(self):
        self.combine_f_df.to_csv(self.csv_path)


    def __call__(self, *args, **kwargs):
        self.pre_process_history_df()
        self.generate_csv()


if __name__ == '__main__':


    user_csv_path = '../data/csv/vt_user.csv'
    if os.path.exists(user_csv_path):
        os.remove(user_csv_path)

    history_csv_path = '../data/csv/history.csv'
    if os.path.exists(history_csv_path):
        os.remove(history_csv_path)


    connect_to_database()

    while True:
        try:
            user_df = get_user(user_csv_path)
        except:
            connect_to_database()
        else:
            break

    while True:
        try:
            history_last_7_days_df = get_history(history_csv_path)
        except Exception as e:
            connect_to_database()
        else:
            break

    history_last_1_days_df = history_last_7_days_df[history_last_7_days_df.create_date.apply(lambda x: str(x)[:10] == now_str)]

    csv_last_7_path = '../data/csv/{} Last 7 days activity users.csv'.format(now_end_str)
    csv_last_1_path = '../data/csv/{} activity users.csv'.format(now_end_str)

    GenerateExceptionUserCsv(csv_path=csv_last_7_path, user_df=user_df, history_df=history_last_7_days_df)()
    GenerateExceptionUserCsv(csv_path=csv_last_1_path, user_df=user_df, history_df=history_last_1_days_df)()
