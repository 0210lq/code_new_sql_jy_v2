from MktData_update.Mktdata_update import indexComponent_update,stockData_update
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt

def MktData_update_main(start_date,end_date,is_sql):
    ICU = indexComponent_update(start_date,end_date, is_sql)
    SDU = stockData_update(start_date,end_date, is_sql)
    SDU.stock_data_update_main()
    ICU.index_component_update_main2()

def MktData_update_main2(start_date,end_date,is_sql):
    working_days_list=gt.working_days_list(start_date,end_date)
    for available_date in working_days_list:
        print(available_date)
        # ICU = indexComponent_update(start_date, end_date, is_sql)
        SDU = stockData_update(start_date, end_date, is_sql)
        SDU.stock_data_update_main()
        # ICU.index_component_update_main2()

if __name__ == '__main__':
    #CBData_update_main('2015-01-05', '2025-07-14',True)
    MktData_update_main2('2025-08-04', '2025-08-04',True)
    # MktData_update_main('2022-07-19','2025-07-14',True)

