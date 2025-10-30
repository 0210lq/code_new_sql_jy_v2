from Optimizer_python.optimizer_history_main import history_optimizer_main
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import pandas as pd
import os
def backtest_main_part1():
    n = 2
    chunks_outputpath_list = gt.chunks(history_optimizer_main(), n)
    j = 1
    for i in chunks_outputpath_list:
        with open(f'output_part1{j}.txt', 'w', encoding='utf-8') as f:
            for k in i:
                f.write(f"{k}\n")
        j += 1
    sys.exit(n)

if __name__ == '__main__':
    backtest_main_part1()
