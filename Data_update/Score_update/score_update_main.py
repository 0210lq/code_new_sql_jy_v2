from Score_update.rrScore_update import rrScore_update
from Score_update.scoreCombination_update import combineScore_update
from Score_update.scorePortfolio_update import scorePortfolio_update
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
def score_update_main(score_type,start_date,end_date,is_sql): #这里面的date是target_date
    rr=rrScore_update(start_date,end_date,is_sql)
    su=scorePortfolio_update(start_date,end_date,is_sql)
    cu=combineScore_update(start_date,end_date,is_sql)
    if score_type=='fm':
        rr.rr_update_main()
        su.scorePortfolio_update_main()
        cu.score_combination_main()
    else:
        print('非生产时间段')
if __name__ == '__main__':
    score_update_main('fm', '2025-08-22','2025-08-22',True)