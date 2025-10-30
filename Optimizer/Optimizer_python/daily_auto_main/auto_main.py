from Optimizer_python.optimizer_update_main import update_optimizer_main,update_optimizer_main2
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
def main_part1():#交易日版
    n = 2
    chunks_outputpath_list = gt.chunks(update_optimizer_main(), n)
    j = 1
    for i in chunks_outputpath_list:
        with open(f'output_part1{j}.txt', 'w', encoding='utf-8') as f:
            for k in i:
                f.write(f"{k}\n")
        j += 1
    sys.exit(n)
def main_part1_2():#周末版本
    n = 2
    chunks_outputpath_list = gt.chunks(update_optimizer_main2(), n)
    j = 1
    for i in chunks_outputpath_list:
        with open(f'output_part1{j}.txt', 'w', encoding='utf-8') as f:
            for k in i:
                f.write(f"{k}\n")
        j += 1
    sys.exit(n)

if __name__ == '__main__':
    main_part1()