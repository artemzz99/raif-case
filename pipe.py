def pipe(df_test):
    df_test['dlk_cob_date'] = pd.to_datetime(df_test['dlk_cob_date'], yearfirst=True)
    df_test.sort_values(['cif_id', 'dlk_cob_date'], inplace=True)
    df_test = df_test[df_test['dlk_cob_date'] < '2018-07-01']
    df = df_test.drop(['cu_education_level', 'cu_empl_area', 'cu_empl_level',
                  'cu_empl_cur_dur_m', 'rank'], axis=1)
    df.rename(columns={'gi_smooth_3m': 'gi_smooth_3m_x'}, inplace=True)
    del df_test
    df = df.fillna(0)
    df['all_credits_count'] = df['cur_quantity_pl'] + df['cur_quantity_mort']
    df['all_cards_count'] = df['cur_quantity_dc'] + df['cur_quantity_cc']
    df['all_accounts_count'] = df['cur_quantity_accounts'] + df['cur_quantity_deposits'] + df['cur_quantity_saccounts'] + df['cur_quantity_mf']
    df['pl_share'] = df['cur_quantity_pl'] / df['all_credits_count']
    df['mort_share'] = df['cur_quantity_mort'] / df['all_credits_count']
    df['dc_share'] = df['cur_quantity_dc'] / df['all_cards_count']
    df['cc_share'] = df['cur_quantity_cc'] / df['all_cards_count']
    df['acc_share'] = df['cur_quantity_accounts'] / df['all_accounts_count']
    df['dep_share'] = df['cur_quantity_deposits'] / df['all_accounts_count']
    df['sacc_share'] = df['cur_quantity_saccounts'] / df['all_accounts_count']
    df['mf_share'] = df['cur_quantity_mf'] / df['all_accounts_count']
    df['all_credits_sum'] = df['cc_balance'] + df['cl_balance'] + df['ml_balance'] + df['pl_balance']
    df['cc_sumshare'] = df['cc_balance'] / df['all_credits_sum']
    df['cl_sumshare'] = df['cl_balance'] / df['all_credits_sum']
    df['ml_sumshare'] = df['ml_balance'] / df['all_credits_sum']
    df['pl_sumshare'] = df['pl_balance'] / df['all_credits_sum']
    df['all_accounts_sum'] = df['td_volume'] + df['ca_volume'] + df['sa_volume'] + df['mf_volume']
    df['td_sumshare'] = df['td_volume'] / df['all_accounts_sum']
    df['ca_sumshare'] = df['ca_volume'] / df['all_accounts_sum']
    df['sa_sumshare'] = df['sa_volume'] / df['all_accounts_sum']
    df['mf_sumshare'] = df['mf_volume'] / df['all_accounts_sum']
    df['avg_cash_spend_cc'] = df['cc_cash_spend_v'] / df['cc_cash_spend_c']
    df['avg_cash_spend_dc'] = df['dc_cash_spend_v'] / df['dc_cash_spend_c']
    df['sum_spend_cash'] = df['cc_cash_spend_v'] + df['dc_cash_spend_v']
    df['count_spend_cash'] = df['cc_cash_spend_c'] + df['dc_cash_spend_c']
    df['dc_cash_share'] = df['dc_cash_spend_v'] / df['sum_spend_cash']
    df['cc_cash_share'] = df['cc_cash_spend_v'] / df['sum_spend_cash']
    df['pos_sum'] = df['dc_pos_spend_v'] + df['cc_pos_spend_v']
    df['pos_count'] = df['dc_pos_spend_c'] + df['cc_pos_spend_c']
    df['dc_pos_share_sum'] = df['dc_pos_spend_v'] / df['pos_sum']
    df['cc_pos_share_sum'] = df['cc_pos_spend_v'] / df['pos_sum']
    df['dc_pos_share_count'] = df['dc_pos_spend_c'] / df['pos_count']
    df['cc_pos_share_count'] = df['cc_pos_spend_c'] / df['pos_count']
    df.dlk_cob_date = df.dlk_cob_date.apply(lambda x: x.month)
    df = df.set_index(['cif_id', 'dlk_cob_date']).unstack().fillna(0)
    lst_diff_sum = ['gi_smooth_3m_x', 'salary', 
                     'all_accounts_sum', 'all_credits_sum',
                     'sum_spend_cash', 'dc_cash_spend_v', 'dc_cash_spend_c', 
                    'cc_cash_spend_v','cc_cash_spend_c', 'dc_pos_spend_v', 
                    'dc_pos_spend_c', 'cc_pos_spend_v',
           'cc_pos_spend_c','cc_balance', 'cl_balance',
           'ml_balance', 'pl_balance', 'td_volume', 'ca_volume', 'sa_volume',
           'mf_volume', 'cur_quantity_pl', 'cur_quantity_mort', 'cur_quantity_cc',
           'cur_quantity_deposits', 'cur_quantity_dc', 'cur_quantity_accounts',
           'cur_quantity_saccounts', 'cur_quantity_mf', 'standalone_dc_f', 'standalone_payroll_dc_f',
           'standalone_nonpayroll_dc_f', 'all_credits_count', 'all_cards_count', 'all_accounts_count',
           'pl_share', 'mort_share', 'dc_share', 'cc_share', 'acc_share',
           'dep_share', 'sacc_share', 'mf_share', 'cc_sumshare',
           'cl_sumshare', 'ml_sumshare', 'pl_sumshare',
           'td_sumshare', 'ca_sumshare', 'sa_sumshare', 'mf_sumshare',
           'avg_cash_spend_cc', 'avg_cash_spend_dc',
           'count_spend_cash', 'dc_cash_share', 'cc_cash_share', 'pos_sum',
           'pos_count', 'dc_pos_share_sum', 'cc_pos_share_sum',
           'dc_pos_share_count', 'cc_pos_share_count']
    for col in lst_diff_sum:
          df['diff_sum_' + col] = df[col].diff(axis=1).drop(1,axis=1).sum(axis=1)
    for col in lst_diff_sum:
          df['max_diff_' + col] = df[col].diff(axis=1).drop(1,axis=1).max(axis=1)
    for col in lst_diff_sum:
          df['min_diff_' + col] = df[col].diff(axis=1).drop(1,axis=1).min(axis=1)
    lst_diff_mean = ['salary', 'all_accounts_sum', 'all_credits_sum',
                     'gi_smooth_3m_x','cc_balance', 
                     'cl_balance','ml_balance', 'pl_balance', 'td_volume', 
                     'ca_volume', 'sa_volume','mf_volume', 'sum_spend_cash']
    for col in lst_diff_mean:
          df['diff_mean_' + col] = df['diff_sum_'+col]/5
    last_col_lst = ['cur_quantity_pl', 'cur_quantity_mort', 'cur_quantity_cc',
           'cur_quantity_deposits', 'cur_quantity_dc', 'cur_quantity_accounts',
           'cur_quantity_saccounts', 'cur_quantity_mf', 'ca_f', 'cu_age', 'cu_mob', 
           'is_married', 'cu_eduaction_level', 'big_city', 'cu_gender', 'standalone_dc_f', 'standalone_payroll_dc_f',
           'standalone_nonpayroll_dc_f', 'active', 'payroll_f', 'gi_smooth_3m_x']
    for col in last_col_lst:
        df[col+'_m6'] = df[col][6]
        if col != 'gi_smooth_3m_x':
            df.drop(col, axis=1, inplace=True)
    summary_col_lst = ['gi_smooth_3m_x', 'salary', 
                     'all_accounts_sum', 'all_credits_sum',
                     'sum_spend_cash', 'dc_cash_spend_v', 'dc_cash_spend_c', 
                    'cc_cash_spend_v','cc_cash_spend_c', 'dc_pos_spend_v', 
                    'dc_pos_spend_c', 'cc_pos_spend_v',
           'cc_pos_spend_c','cc_balance', 'cl_balance',
           'ml_balance', 'pl_balance', 'td_volume', 'ca_volume', 'sa_volume',
           'mf_volume','all_credits_count', 'all_cards_count', 'all_accounts_count',
           'pl_share', 'mort_share', 'dc_share', 'cc_share', 'acc_share',
           'dep_share', 'sacc_share', 'mf_share', 'cc_sumshare',
           'cl_sumshare', 'ml_sumshare', 'pl_sumshare',
           'td_sumshare', 'ca_sumshare', 'sa_sumshare', 'mf_sumshare',
           'avg_cash_spend_cc', 'avg_cash_spend_dc',
           'count_spend_cash', 'dc_cash_share', 'cc_cash_share', 'pos_sum',
           'pos_count', 'dc_pos_share_sum', 'cc_pos_share_sum',
           'dc_pos_share_count', 'cc_pos_share_count', 'cur_qnt_sms','rc_session_qnt_cur_mon']
    for col in summary_col_lst:
        df['sum_' + col] = df[col].sum(axis=1)
        df['min_' + col] = df[col].min(axis=1)
        df['max_' + col] = df[col].max(axis=1)
        df['mean_' + col] = df[col].sum(axis=1)/6
        df.drop(col, axis=1, inplace=True)
    df.columns = [ ''.join([str(c) for c in c_list]) for c_list in df.columns.values ]
    city = pd.get_dummies(df['big_city_m6'])
    df = pd.concat([df, city], axis=1)
    df.drop(['big_city_m6'], axis=1, inplace=True)
    print(df.shape)
    return df
