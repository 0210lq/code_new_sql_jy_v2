# MySQL数据库配置文件

# 数据库连接配置字典，可包含多个数据库连接信息
DATABASE_CONFIGS = {
    # 默认数据库配置
    'default': {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'default_db'
    },
    # 源数据库配置
    'source_db': {
        'host': 'rm-bp1o6we7s3o1h76x1to.mysql.rds.aliyuncs.com',
        'port': 3306,
        'user': 'back',
        'password': 'Abcd1234#',
        'database': 'data_prepared_new'
    },
    # 目标数据库配置
    'target_db': {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'data_prepared_new'
    }
}

# 获取指定名称的数据库配置
def get_db_config(config_name='default'):
    """
    获取指定名称的数据库配置
    
    参数:
        config_name: 配置名称，默认为'default'
    
    返回:
        数据库配置字典
    """
    if config_name in DATABASE_CONFIGS:
        return DATABASE_CONFIGS[config_name]
    else:
        raise KeyError(f"数据库配置 '{config_name}' 不存在")